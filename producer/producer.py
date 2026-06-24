import requests
import json
import time
import uuid
import random
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_TOPIC    = "raw_events"
KAFKA_SERVER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

CLIENT_ID      = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET  = os.getenv("OPENSKY_CLIENT_SECRET")
TOKEN_URL      = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_URL    = "https://opensky-network.org/api/states/all"

FETCH_INTERVAL = 10
MAX_PLANES     = 50
FAKER_MODE     = os.getenv("FAKER_MODE", "false").lower() == "true"

# ─── Kafka Producer avec retry ────────────────────────────────────────────────
def create_producer():
    """Tente de se connecter à Kafka, retry toutes les 5s jusqu'à succès."""
    while True:
        try:
            print(f"[INFO] Connexion à Kafka sur {KAFKA_SERVER}...")
            p = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks=1,
                linger_ms=10,
                retries=3,
                api_version=(3, 5, 1),  # Confluent 7.5 = Kafka 3.5.1
            )
            print("[INFO] Connecté à Kafka ✓")
            return p
        except NoBrokersAvailable:
            print("[WARN] Kafka pas encore prêt, retry dans 5s...")
            time.sleep(5)

producer = create_producer()

# ─── OAuth2 Token ─────────────────────────────────────────────────────────────
_token        = None
_token_expiry = 0

def get_token():
    global _token, _token_expiry
    if _token and time.time() < _token_expiry - 30:
        return _token
    print("[AUTH] Récupération du token OAuth2...")
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }, timeout=10)
    if resp.status_code != 200:
        raise Exception(f"Erreur OAuth2 : {resp.status_code} — {resp.text}")
    data          = resp.json()
    _token        = data["access_token"]
    _token_expiry = time.time() + data.get("expires_in", 300)
    print("[AUTH] Token obtenu ✓")
    return _token


# ─── OpenSky Fetch ────────────────────────────────────────────────────────────
def fetch_opensky():
    try:
        token    = get_token()
        headers  = {"Authorization": f"Bearer {token}"}
        response = requests.get(OPENSKY_URL, headers=headers, timeout=10)

        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 60))
            print(f"[429] Rate limit → attente {wait}s...")
            time.sleep(wait)
            return []

        if response.status_code != 200:
            print(f"[ERROR] API OpenSky : HTTP {response.status_code}")
            return []

        data = response.json()
        if not data or "states" not in data or not data["states"]:
            print("[WARN] Réponse vide depuis OpenSky")
            return []

        events = []
        for s in data["states"][:MAX_PLANES]:
            if not s:
                continue
            longitude = s[5]
            latitude  = s[6]
            if latitude is None or longitude is None:
                continue

            event = {
                "event_id":       str(uuid.uuid4()),
                "event_time":     int(data.get("time", time.time())),
                "source":         "opensky",
                "icao24":         s[0] or "",
                "callsign":       (s[1] or "").strip(),
                "origin_country": s[2] or "Unknown",
                "longitude":      float(longitude),
                "latitude":       float(latitude),
                "baro_altitude":  float(s[7])  if s[7]  is not None else 0.0,
                "velocity":       float(s[9])  if s[9]  is not None else 0.0,
                "true_track":     float(s[10]) if s[10] is not None else 0.0,
                "on_ground":      bool(s[8]),
            }
            events.append((s[0] or "unknown", event))

        return events

    except requests.exceptions.ConnectionError:
        print("[ERROR] Impossible de joindre OpenSky (réseau ?)")
        return []
    except Exception as e:
        print(f"[ERROR] fetch_opensky : {e}")
        return []


# ─── Faker Fallback ───────────────────────────────────────────────────────────
COUNTRIES = ["Germany", "France", "Spain", "Italy", "Netherlands",
             "United Kingdom", "Belgium", "Turkey", "Switzerland", "Portugal"]
CALLSIGNS = ["DLH", "AFR", "IBE", "BAW", "KLM", "EZY", "VLG", "THY", "SWR", "TAP"]

def fetch_fake():
    """
    Génère 10 événements GPS simulés.
    ~10% des événements ont un event_time 60-120s dans le passé
    pour simuler des late events (DAT section 7.2).
    """
    events = []
    now = int(time.time())
    for i in range(10):
        icao24 = uuid.uuid4().hex[:6]
        # 1 événement sur 10 est un late event (delay 60-120s)
        is_late = (i == 9)
        event_time = now - random.randint(60, 120) if is_late else now
        event = {
            "event_id":       str(uuid.uuid4()),
            "event_time":     event_time,
            "source":         "faker_late" if is_late else "faker",
            "icao24":         icao24,
            "callsign":       f"{random.choice(CALLSIGNS)}{random.randint(100, 999)}",
            "origin_country": random.choice(COUNTRIES),
            "longitude":      round(random.uniform(-10, 30), 4),
            "latitude":       round(random.uniform(35, 60), 4),
            "baro_altitude":  round(random.uniform(3000, 12500), 1),
            "velocity":       round(random.uniform(150, 300), 1),
            "true_track":     round(random.uniform(0, 360), 1),
            "on_ground":      False,
        }
        events.append((icao24, event))
    return events


# ─── Main Loop ────────────────────────────────────────────────────────────────
def send_to_kafka():
    mode = "FAKER" if FAKER_MODE else "OPENSKY (OAuth2)"
    print(f"[INFO] Producer démarré — mode={mode} | topic={KAFKA_TOPIC} | broker={KAFKA_SERVER}")

    while True:
        try:
            events = fetch_fake() if FAKER_MODE else fetch_opensky()

            if events:
                futures = []
                for icao24, event in events:
                    f = producer.send(KAFKA_TOPIC, key=icao24, value=event)
                    futures.append(f)
                producer.flush()
                errors = 0
                for f in futures:
                    try:
                        f.get(timeout=10)
                    except Exception as e:
                        print(f"[ERROR] Delivery failed: {e}")
                        errors += 1
                ok = len(events) - errors
                if errors:
                    print(f"[WARN] {ok}/{len(events)} envoyés, {errors} erreurs")
                else:
                    print(f"[OK] {len(events)} événements envoyés → {KAFKA_TOPIC}")
            else:
                print("[INFO] Aucun événement à envoyer")

        except Exception as e:
            print(f"[ERROR] send_to_kafka : {e}")

        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    send_to_kafka()