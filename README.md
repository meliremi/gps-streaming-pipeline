# GPS Streaming Pipeline

Pipeline de traitement temps réel de positions ADS-B d'aéronefs.  
Données source : [OpenSky Network](https://opensky-network.org/) (ou mode faker intégré).

**Auteurs :**  Remila Mélissa · Benzouaoua Selma — M2 Data Analytics

---

## Stack technique

| Composant | Rôle | Port |
|-----------|------|------|
| Apache Kafka 3.5 | Bus de messages Bronze/Silver/Gold | 9092 (host) / 29092 (inter-container) |
| Apache Flink 1.18.1 | Jobs SQL streaming (transform + agrégation) | 8081 |
| Apache Airflow 2.8.0 | Orchestration et monitoring du pipeline | 8080 |
| PostgreSQL 15 | Stockage Gold (gold.aggregates) | 5432 |
| Tableau | Dashboard analytique | — |
| Docker Compose | Orchestration des services | — |

---

## Architecture

```
OpenSky API / Faker
        │
        ▼
   [Producer]
        │  raw_events (Bronze)
        ▼
  [Flink Job 2]  ──── late_events (side output > 30s)
        │  silver_events (Silver)
        ▼
  [Flink Job 3]
     ┌──┴──┐
     ▼     ▼
gold_output  gold.aggregates (PostgreSQL)
(Kafka)      └── Tableau Dashboard
```

**Topics Kafka :**

| Topic | Partitions | Rétention | Description |
|-------|-----------|-----------|-------------|
| `raw_events` | 3 | 7 jours | Données brutes ADS-B |
| `silver_events` | 3 | 24h | Données filtrées et normalisées |
| `gold_output` | 3 | 3 jours | Agrégats fenêtrés |
| `late_events` | 1 | 24h | Événements arrivés > 30s en retard |

---

## Prérequis

- Docker Desktop (4 Go RAM minimum alloués)
- PowerShell 5+ (téléchargement des JARs)
- Tableau Desktop (optionnel, pour le dashboard)

---

## Installation et démarrage (< 5 minutes)

### Étape 1 — Cloner le repo

```powershell
git clone https://github.com/meliremi/gps-streaming-pipeline.git
cd gps-streaming-pipeline
```

### Étape 2 — Télécharger les JARs Flink (une seule fois)

Les connecteurs Kafka et JDBC sont trop volumineux pour Git. Télécharge-les via PowerShell :

```powershell
New-Item -ItemType Directory -Force -Path flink-jobs\lib
$base = "https://repo1.maven.org/maven2"

Invoke-WebRequest "$base/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" `
  -OutFile "flink-jobs\lib\flink-sql-connector-kafka-3.1.0-1.18.jar"

Invoke-WebRequest "$base/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" `
  -OutFile "flink-jobs\lib\flink-connector-jdbc-3.1.2-1.18.jar"

Invoke-WebRequest "$base/org/postgresql/postgresql/42.7.0/postgresql-42.7.0.jar" `
  -OutFile "flink-jobs\lib\postgresql-42.7.0.jar"
```

### Étape 3 — Configurer l'environnement

```powershell
Copy-Item .env.example .env   # si disponible, sinon créer le fichier
```

Contenu minimal du `.env` pour le mode faker (aucune clé API requise) :

```env
FAKER_MODE=true
OPENSKY_CLIENT_ID=
OPENSKY_CLIENT_SECRET=
```

### Étape 4 — Démarrer le pipeline

```powershell
docker compose up -d --build
```

### Étape 5 — Vérifier que tout tourne

```powershell
docker compose ps
```

Attendre ~60s que Kafka et Flink soient prêts. Le service `flink-sql-runner` soumet automatiquement les jobs SQL.

### Étape 6 — Contrôler les interfaces

| Interface | URL | Description |
|-----------|-----|-------------|
| Kafka UI | http://localhost:8090 | Messages par topic |
| Flink UI | http://localhost:8081 | Jobs streaming (6 RUNNING attendus) |
| Airflow | http://localhost:8080 | DAG `gps_pipeline` |
| PostgreSQL | localhost:5432 | Base `gold`, table `gold.aggregates` |

### Étape 7 — Lancer le DAG Airflow

1. Aller sur http://localhost:8080
2. Récupérer le mot de passe : `docker logs airflow 2>&1 | Select-String "password"`
3. Activer et déclencher le DAG `gps_pipeline`
4. Vérifier que les 5 tâches passent au vert

---

## Jobs Flink SQL

### Job 2 — Transform Silver (`02_transform_silver.sql`)

- Source : `raw_events` (Kafka)
- Transformations : m → ft, m/s → km/h, cap → cardinal (N/S/E/W)
- Filtre : latitude/longitude non nulles, altitude ≥ 0, vitesse ≤ 330 m/s
- Sink 1 : `silver_events` (Kafka)
- Sink 2 : `late_events` — side output pour événements arrivés > 30s après leur `event_time`

### Job 3 — Aggregate Gold (`03_aggregate_gold.sql`)

- Source : `silver_events` (Kafka) avec watermark 10s
- Fenêtres :
  - **Tumbling 1 min** → `gold_output` (Kafka) + `gold.aggregates` (PostgreSQL)
  - **Sliding 5 min / 1 min** → `gold_output` (Kafka) + `gold.aggregates` (PostgreSQL)
- Métriques : aircraft_count, avg_speed_kmh, avg_altitude_ft, max_speed_kmh

---

## Connexion DataGrip / Tableau

**Paramètres de connexion PostgreSQL :**

| Paramètre | Valeur |
|-----------|--------|
| Host | `localhost` |
| Port | `5432` |
| Database | `gold` |
| User | `gold_user` |
| Password | `gold_password` |
| Schéma | `gold` |
| Table | `gold.aggregates` |

**Requête de vérification :**

```sql
SELECT window_type, origin_country,
       SUM(aircraft_count)      AS total_avions,
       AVG(avg_speed_kmh)       AS vitesse_moy_kmh,
       AVG(avg_altitude_ft)     AS altitude_moy_ft,
       MAX(max_speed_kmh)       AS vitesse_max_kmh
FROM gold.aggregates
GROUP BY window_type, origin_country
ORDER BY total_avions DESC
LIMIT 20;
```

---

## Structure du projet

```
gps-streaming-pipeline/
├── docker-compose.yml          # Orchestration de tous les services
├── .env                        # Variables d'environnement (non commité)
├── .gitignore
├── producer/
│   ├── Dockerfile
│   ├── producer.py             # Producer Kafka (OpenSky API + Faker)
│   └── requirements.txt
├── flink-jobs/
│   ├── Dockerfile              # Image Flink + JARs connecteurs
│   ├── lib/                    # JARs (non commités — voir Étape 2)
│   └── sql/
│       ├── 01_ddl_sources.sql  # DDL de référence
│       ├── 02_transform_silver.sql  # Job Bronze → Silver + late events
│       └── 03_aggregate_gold.sql    # Job Silver → Gold (TUMBLE + HOP)
├── postgres/
│   └── init.sql                # Schéma gold.aggregates + index
└── airflow/
    └── dags/
        └── gps_pipeline_dag.py # DAG monitoring (5 tâches)
```

---

## Arrêt du pipeline

```powershell
docker compose down          # Arrêt des containers (données conservées)
docker compose down -v       # Arrêt + suppression des volumes (reset complet)
```
