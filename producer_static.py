#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex01 — Producer statique
Envoie 10 messages JSON sur un topic Kafka (par défaut: weather_stream).
Usage:
    python producer_static.py --topic weather_stream
Vars env supportées:
    KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092)
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
    args = parser.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        producer = build_producer(servers)
    except NoBrokersAvailable:
        logging.error("Kafka non joignable sur %s. Lance d’abord: docker compose up -d", servers)
        raise SystemExit(1)

    logging.info("Envoi de 10 messages sur topic '%s' -> %s", args.topic, servers)
    for i in range(10):
        payload = {
            "msg": "Hello Kafka",
            "seq": i + 1,
            "ts": datetime.now(timezone.utc).isoformat()
        }
        future = producer.send(args.topic, payload)
        # forcer l'exception ici si pb d’acks
        future.get(timeout=10)
        logging.info("Envoyé: %s", payload)
        time.sleep(0.1)

    producer.flush()
    producer.close()
    logging.info("Terminé.")

if __name__ == "__main__":
    main()
