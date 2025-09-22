#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex02 — Consumer basique
- Consomme des messages JSON d'un topic Kafka.
- Affiche chaque record + un petit récap.
- Options: TOPIC, GROUP_ID, AUTO_OFFSET_RESET, MAX_MESSAGES.
"""
import os
import json
import argparse
import logging
from typing import Optional
from kafka import KafkaConsumer

def build_consumer(
    servers: str,
    topic: str,
    group_id: Optional[str],
    auto_offset_reset: str,
    timeout_ms: int,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=servers.split(","),
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=auto_offset_reset,  # 'earliest' ou 'latest'
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,       # stop si pas de nouveaux messages
        max_poll_records=100,
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=os.getenv("TOPIC", "weather_stream"))
    parser.add_argument("--group-id", default=os.getenv("GROUP_ID", "ex02-consumer"))
    parser.add_argument("--auto-offset-reset", default=os.getenv("AUTO_OFFSET_RESET", "earliest"),
                        choices=["earliest", "latest"])
    parser.add_argument("--timeout-ms", type=int, default=int(os.getenv("TIMEOUT_MS", "5000")))
    parser.add_argument("--max-messages", type=int, default=int(os.getenv("MAX_MESSAGES", "0")),
                        help="0 = illimité")
    args = parser.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Consumer -> topic=%s, group=%s, reset=%s, servers=%s",
                 args.topic, args.group_id, args.auto_offset_reset, servers)

    consumer = build_consumer(servers, args.topic, args.group_id, args.auto_offset_reset, args.timeout_ms)

    seen = 0
    try:
        for msg in consumer:
            seen += 1
            logging.info("Record@%s-%d offset=%d key=%s value=%s",
                         msg.topic, msg.partition, msg.offset, msg.key, msg.value)
            if args.max_messages and seen >= args.max_messages:
                break
    except KeyboardInterrupt:
        logging.warning("Interruption utilisateur (Ctrl+C).")
    finally:
        consumer.close()
        logging.info("Terminé. Messages lus: %d", seen)

if __name__ == "__main__":
    main()
