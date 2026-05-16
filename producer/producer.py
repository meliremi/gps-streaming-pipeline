import requests
import json
import time
import uuid
from kafka import KafkaProducer

KAFKA_TOPIC = "raw_events"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

OPEN_SKY_URL = "https://opensky-network.org/api/states/all"

def fetch_data():
    try:
        response = requests.get(OPEN_SKY_URL, timeout=10)
        data = response.json()

        states = data.get("states", [])
        events = []

        for s in states[:50]:  # limiter pour test
            if s is None:
                continue

            event = {
                "event_id": str(uuid.uuid4()),
                "event_time": int(time.time()),
                "payload": {
                    "icao24": s[0],
                    "callsign": s[1],
                    "origin_country": s[2],
                    "longitude": s[5],
                    "latitude": s[6],
                    "baro_altitude": s[7],
                    "velocity": s[9],
                    "true_track": s[10],
                    "on_ground": s[8]
                }
            }

            events.append(event)

        return events

    except Exception as e:
        print("API error:", e)
        return []

def send_to_kafka():
    while True:
        events = fetch_data()

        for e in events:
            producer.send(KAFKA_TOPIC, value=e)

        producer.flush()
        print(f"{len(events)} events sent")

        time.sleep(5)

if __name__ == "__main__":
    send_to_kafka()