#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex06 — Producer statique (ville + pays)
Envoie 10 messages JSON sur un topic Kafka (par défaut: weather_stream).
Arguments:
  --city-name "Paris" --country "FR"
Compat: si non fournis, on met des valeurs par défaut.
"""

import os, json, time, argparse, logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def build_producer(servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=servers.split(","),
        acks="all",
        linger_ms=10,
        retries=3,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="weather_stream")
    parser.add_argument("--city-name", default=os.getenv("CITY_NAME", "Paris"))
    parser.add_argument("--country", default=os.getenv("COUNTRY", "FR"))
    args = parser.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        producer = build_producer(servers)
    except NoBrokersAvailable:
        logging.error("Kafka non joignable sur %s. Lance d’abord: docker compose up -d", servers)
        raise SystemExit(1)

    logging.info("Envoi de 10 messages sur topic '%s' -> %s (city=%s, country=%s)",
                 args.topic, servers, args.city_name, args.country)

    for i in range(10):
        payload = {
            "msg": "Hello Kafka",
            "seq": i + 1,
            "ts": datetime.now(timezone.utc).isoformat(),
            # Ajouts Ex06
            "city_name": args.city_name,
            "country": None,              # nom complet inconnu ici
            "country_code": args.country,
            "version": 2
        }
        producer.send(args.topic, payload).get(timeout=10)
        logging.info("Envoyé: %s", payload)
        time.sleep(0.1)

    producer.flush()
    producer.close()
    logging.info("Terminé.")

if __name__ == "__main__":
    main()
