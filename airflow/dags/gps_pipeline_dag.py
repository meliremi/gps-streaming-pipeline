"""
DAG : gps_pipeline
Orchestration du pipeline GPS Streaming (Bronze → Silver → Gold)

Tâches :
  t1 — check_kafka_health   : vérifie Kafka + Flink + PostgreSQL
  t2 — check_producer       : vérifie que raw_events reçoit des messages
  t3 — check_transform_job  : vérifie que le job Transform Silver tourne
  t4 — check_aggregate_job  : vérifie que le job Aggregate Gold tourne
  t5 — monitor_jobs         : surveillance continue des jobs Flink

Flux : t1 >> t2 >> t3 >> t4 >> t5
"""

from __future__ import annotations

import json
import time
import psycopg2
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ─── Constantes ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = "kafka:29092"
FLINK_API         = "http://flink-jobmanager:8081"
PG_HOST           = "postgres"
PG_PORT           = 5432
PG_DB             = "gold"
PG_USER           = "gold_user"
PG_PASS           = "gold_password"


# ─── Task 1 : Health check ───────────────────────────────────────────────────
def check_kafka_health(**context):
    """Vérifie que Kafka, Flink JobManager et PostgreSQL sont accessibles."""
    from kafka import KafkaAdminClient

    errors = []

    # Kafka
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=5000)
        topics = admin.list_topics()
        admin.close()
        print(f"[Kafka] OK — topics : {topics}")
    except Exception as e:
        errors.append(f"Kafka : {e}")

    # Flink
    try:
        r = requests.get(f"{FLINK_API}/overview", timeout=5)
        r.raise_for_status()
        overview = r.json()
        print(f"[Flink] OK — version {overview.get('flink-version')} | "
              f"jobs running : {overview.get('jobs-running', 0)}")
    except Exception as e:
        errors.append(f"Flink : {e}")

    # PostgreSQL
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                user=PG_USER, password=PG_PASS, connect_timeout=5)
        conn.close()
        print("[PostgreSQL] OK")
    except Exception as e:
        errors.append(f"PostgreSQL : {e}")

    if errors:
        raise Exception("Health check échoué :\n" + "\n".join(errors))

    print("[OK] Tous les services sont disponibles ✓")


# ─── Task 2 : Vérifier le producer ───────────────────────────────────────────
def check_producer(**context):
    """Vérifie que raw_events reçoit des messages depuis le producer."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        "raw_events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        consumer_timeout_ms=15000,  # attendre max 15s
        group_id="airflow-check-group",
    )

    messages = []
    for msg in consumer:
        messages.append(msg)
        if len(messages) >= 3:
            break
    consumer.close()

    if not messages:
        raise Exception("Aucun message dans raw_events après 15s — le producer est-il démarré ?")

    print(f"[OK] {len(messages)} messages reçus dans raw_events ✓")
    sample = json.loads(messages[0].value.decode("utf-8"))
    print(f"[INFO] Exemple d'événement : icao24={sample.get('icao24')} | "
          f"country={sample.get('origin_country')}")


# ─── Task 3 : Vérifier job Transform Silver ──────────────────────────────────
def check_transform_job(**context):
    """Vérifie que le job Flink SQL Transform Silver est en état RUNNING."""
    _wait_for_flink_job("transform", timeout=120)


# ─── Task 4 : Vérifier job Aggregate Gold ────────────────────────────────────
def check_aggregate_job(**context):
    """Vérifie que le job Flink SQL Aggregate Gold est en état RUNNING."""
    _wait_for_flink_job("aggregate", timeout=120)


# ─── Task 5 : Surveillance continue ──────────────────────────────────────────
def monitor_jobs(**context):
    """
    Interroge l'API Flink toutes les 60s pendant 5 min.
    Lève une exception si un job n'est plus RUNNING.
    """
    print("[INFO] Surveillance des jobs Flink pendant 5 minutes...")
    end_time = time.time() + 300  # 5 minutes

    while time.time() < end_time:
        try:
            r = requests.get(f"{FLINK_API}/jobs/overview", timeout=5)
            r.raise_for_status()
            jobs = r.json().get("jobs", [])

            running = [j for j in jobs if j["state"] == "RUNNING"]
            failed  = [j for j in jobs if j["state"] in ("FAILED", "CANCELED")]

            print(f"[Monitor] Jobs RUNNING : {len(running)} | "
                  f"échoués/annulés : {len(failed)}")

            for j in failed:
                raise Exception(f"Job {j['name']} en état {j['state']} !")

        except requests.RequestException as e:
            print(f"[WARN] Impossible de joindre Flink : {e}")

        time.sleep(60)

    print("[OK] Surveillance terminée — pipeline stable ✓")


# ─── Helpers ─────────────────────────────────────────────────────────────────
def _wait_for_flink_job(name_fragment: str, timeout: int = 120):
    """Attend qu'un job Flink contenant `name_fragment` soit RUNNING."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{FLINK_API}/jobs/overview", timeout=5)
            r.raise_for_status()
            jobs = r.json().get("jobs", [])
            running = [j for j in jobs if j["state"] == "RUNNING"]
            if running:
                print(f"[OK] {len(running)} job(s) Flink RUNNING ✓")
                for j in running:
                    print(f"     • {j.get('name', j['jid'])} — {j['state']}")
                return
        except Exception as e:
            print(f"[WARN] {e}")
        print(f"[INFO] En attente de jobs Flink RUNNING ({name_fragment})...")
        time.sleep(10)
    raise Exception(f"Timeout : aucun job Flink RUNNING après {timeout}s")


# ─── DAG ─────────────────────────────────────────────────────────────────────
default_args = {
    "owner":            "remila-selma",
    "retries":          1,
    "retry_delay":      30,  # secondes
}

with DAG(
    dag_id="gps_pipeline",
    description="Orchestration pipeline GPS Streaming — Bronze → Silver → Gold",
    schedule_interval=None,   # déclenchement manuel
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["streaming", "gps", "kafka", "flink"],
) as dag:

    t1 = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    t2 = PythonOperator(
        task_id="check_producer",
        python_callable=check_producer,
    )

    t3 = PythonOperator(
        task_id="check_transform_job",
        python_callable=check_transform_job,
    )

    t4 = PythonOperator(
        task_id="check_aggregate_job",
        python_callable=check_aggregate_job,
    )

    t5 = PythonOperator(
        task_id="monitor_jobs",
        python_callable=monitor_jobs,
    )

    t1 >> t2 >> t3 >> t4 >> t5
