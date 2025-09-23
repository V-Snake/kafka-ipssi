#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ex07 — Consommateur Kafka -> stockage HDFS (ou dossier local).

Le script peut écrire les alertes météo soit :

* dans un dossier local (fallback historique),
* soit dans un véritable cluster HDFS exposant WebHDFS/HttpFS.

Dans les deux cas, la structure respecte l’énoncé :
``/base/{country}/{city}/alerts.json`` avec une ligne JSON par message.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional

from kafka import KafkaConsumer
from hdfs import InsecureClient
from hdfs.util import HdfsError

DEFAULT_BASE_DIR = os.getenv("HDFS_BASE_DIR", "/hdfs-data")
DEFAULT_HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/hdfs-data")
_SANITIZE_RE = re.compile(r"[^0-9A-Za-z._-]+")
ALERT_FILENAME = "alerts.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("Ex07 - Kafka consumer vers HDFS/local")
    parser.add_argument(
        "--topic",
        default=os.getenv("TOPIC", "weather_transformed"),
        help="Topic Kafka à consommer.",
    )
    parser.add_argument(
        "--group-id",
        default=os.getenv("GROUP_ID", "ex07-hdfs-writer"),
        help="Group.id Kafka utilisé pour la consommation.",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Bootstrap servers (ex: localhost:9092).",
    )
    parser.add_argument(
        "--auto-offset-reset",
        default=os.getenv("AUTO_OFFSET_RESET", "earliest"),
        choices=["earliest", "latest"],
        help="Position initiale si pas de commit.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=int(os.getenv("TIMEOUT_MS", "5000")),
        help="Timeout Kafka (ms) pour arrêter la boucle si aucun message.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=int(os.getenv("MAX_MESSAGES", "0")),
        help="Stop après N messages (>0) pour des tests.",
    )
    parser.add_argument(
        "--base-dir",
        default=DEFAULT_BASE_DIR,
        help="Répertoire local racine pour écrire les alertes (fallback).",
    )
    parser.add_argument(
        "--hdfs-url",
        default=os.getenv("HDFS_URL"),
        help="URL WebHDFS/HttpFS (ex: http://localhost:9870).",
    )
    parser.add_argument(
        "--hdfs-user",
        default=os.getenv("HDFS_USER", "root"),
        help="Utilisateur utilisé pour les appels WebHDFS.",
    )
    parser.add_argument(
        "--hdfs-base-path",
        default=DEFAULT_HDFS_BASE_PATH,
        help="Chemin racine HDFS pour stocker les alertes.",
    )
    return parser.parse_args()


def build_consumer(args: argparse.Namespace) -> KafkaConsumer:
    return KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group_id,
        enable_auto_commit=True,
        auto_offset_reset=args.auto_offset_reset,
        value_deserializer=lambda v: v.decode("utf-8"),
        consumer_timeout_ms=args.timeout_ms,
        max_poll_records=200,
    )


def _sanitize_segment(value: Optional[str], default: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return default
    cleaned = _SANITIZE_RE.sub("_", raw)
    cleaned = cleaned.strip("._-")
    return cleaned or default


def pick_country(data: Dict[str, object]) -> str:
    """Choisit le meilleur identifiant de pays disponible."""
    country_code = data.get("country_code")
    if isinstance(country_code, str) and country_code.strip():
        return _sanitize_segment(country_code.upper(), "unknown")

    country = data.get("country")
    if isinstance(country, str) and country.strip():
        return _sanitize_segment(country.lower(), "unknown")

    return "unknown"


def pick_city(data: Dict[str, object]) -> str:
    """Renvoie un slug de la ville (city > city_name)."""
    for field in ("city", "city_name"):
        value = data.get(field)
        if isinstance(value, str) and value.strip():
            return _sanitize_segment(value.lower(), "unknown-city")
    return "unknown-city"


def _ensure_newline(payload: str) -> str:
    return payload if payload.endswith("\n") else f"{payload}\n"


class BaseAlertWriter:
    """Interface minimale pour écrire une alerte."""

    def write(self, country: str, city: str, record_json: str) -> str:
        raise NotImplementedError

    @property
    def description(self) -> str:
        raise NotImplementedError


class LocalAlertWriter(BaseAlertWriter):
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write(self, country: str, city: str, record_json: str) -> str:
        target_dir = self.base_dir / country / city
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / ALERT_FILENAME
        payload = _ensure_newline(record_json)
        with target_file.open("a", encoding="utf-8") as handle:
            handle.write(payload)
        return str(target_file)

    @property
    def description(self) -> str:
        return f"local:{self.base_dir}"


class HDFSAlertWriter(BaseAlertWriter):
    def __init__(self, url: str, user: str, base_path: str) -> None:
        cleaned_url = url.strip()
        self.webhdfs_url = cleaned_url.rstrip("/") if cleaned_url else cleaned_url
        self.client = InsecureClient(self.webhdfs_url, user=user)
        cleaned = (base_path or "").strip()
        if not cleaned or cleaned == "/":
            self.base_path = ""
        else:
            if not cleaned.startswith("/"):
                cleaned = "/" + cleaned
            self.base_path = cleaned.rstrip("/")

    def _target_dir(self, country: str, city: str) -> str:
        if self.base_path:
            return f"{self.base_path}/{country}/{city}"
        return f"/{country}/{city}"

    def write(self, country: str, city: str, record_json: str) -> str:
        target_dir = self._target_dir(country, city)
        self.client.makedirs(target_dir)
        target_path = f"{target_dir}/{ALERT_FILENAME}"
        payload = _ensure_newline(record_json)
        exists = self.client.status(target_path, strict=False) is not None
        if exists:
            self.client.write(target_path, data=payload, encoding="utf-8", append=True)
        else:
            self.client.write(target_path, data=payload, encoding="utf-8", overwrite=True)
        return target_path

    @property
    def description(self) -> str:
        base = self.base_path or "/"
        return f"hdfs:{self.webhdfs_url}{base}"


def make_writer(args: argparse.Namespace) -> BaseAlertWriter:
    if args.hdfs_url:
        return HDFSAlertWriter(args.hdfs_url, args.hdfs_user, args.hdfs_base_path)
    return LocalAlertWriter(Path(args.base_dir))


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    writer = make_writer(args)
    logging.info(
        "Consumer HDFS -> topic=%s bootstrap=%s target=%s",
        args.topic,
        args.bootstrap,
        writer.description,
    )

    consumer = build_consumer(args)

    total = 0
    per_target: Dict[str, int] = defaultdict(int)

    try:
        for msg in consumer:
            total += 1
            raw_value = msg.value

            try:
                data = json.loads(raw_value)
            except json.JSONDecodeError:
                logging.warning("Message non JSON ignoré (offset=%s): %r", msg.offset, raw_value)
                continue

            country = pick_country(data)
            city = pick_city(data)
            record_json = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

            try:
                target = writer.write(country, city, record_json)
            except (OSError, HdfsError):
                logging.exception(
                    "Erreur d'écriture pour country=%s city=%s (offset=%d)",
                    country,
                    city,
                    msg.offset,
                )
                continue

            per_target[str(target)] += 1

            logging.info(
                "Ecrit #%d -> %s (country=%s city=%s offset=%d)",
                total,
                target,
                data.get("country_code") or data.get("country"),
                data.get("city") or data.get("city_name"),
                msg.offset,
            )

            if args.max_messages and total >= args.max_messages:
                break

    except KeyboardInterrupt:
        logging.warning("Interruption utilisateur (Ctrl+C).")
    finally:
        consumer.close()

        if per_target:
            summary = ", ".join(
                f"{path}: {count}" for path, count in sorted(per_target.items())
            )
            logging.info("Messages écrits par fichier: %s", summary)
        logging.info("Terminé. Messages traités: %d", total)


if __name__ == "__main__":
    main()