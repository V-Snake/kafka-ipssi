#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex03 — Producer météo "live" (Open-Meteo, pas de clé API)
- Géocode une ville -> (lat, lon)
- Récupère current_weather
- Envoie N messages JSON dans Kafka (topic par défaut: weather_stream)

Exemples:
  python producer_weather.py --city "Paris,FR" --loops 5 --interval 2
  python producer_weather.py --city "London,UK" --topic weather_stream
"""

import os, time, json, argparse, logging
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
WX_URL  = "https://api.open-meteo.com/v1/forecast"

def geocode_city(session: requests.Session, city: str) -> Tuple[float, float, str]:
    """Retourne (lat, lon, name_display). Lève ValueError si introuvable."""
    r = session.get(GEO_URL, params={"name": city, "count": 1, "language": "en", "format": "json"}, timeout=10)
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    if not results:
        raise ValueError(f"Ville introuvable: {city}")
    res = results[0]
    return float(res["latitude"]), float(res["longitude"]), f'{res.get("name")}, {res.get("country_code")}'

def fetch_current_weather(session: requests.Session, lat: float, lon: float) -> Dict[str, Any]:
    r = session.get(WX_URL, params={"latitude": lat, "longitude": lon, "current_weather": "true"}, timeout=10)
    r.raise_for_status()
    data = r.json()
    cw = data.get("current_weather")
    if not cw:
        raise ValueError("Réponse sans current_weather")
    return {
        "temperature": cw.get("temperature"),
        "windspeed": cw.get("windspeed"),
        "winddirection": cw.get("winddirection"),
        "weathercode": cw.get("weathercode"),
        "time": cw.get("time"),
        "elevation": data.get("elevation"),
        "timezone": data.get("timezone"),
        "units": data.get("current_weather_units"),
        "source": "open-meteo"
    }

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
    parser.add_argument("--city", default=os.getenv("CITY", "Paris,FR"))
    parser.add_argument("--topic", default=os.getenv("TOPIC", "weather_stream"))
    parser.add_argument("--loops", type=int, default=int(os.getenv("LOOPS", "5")))
    parser.add_argument("--interval", type=float, default=float(os.getenv("INTERVAL", "2")))
    args = parser.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("Producer météo -> city=%s topic=%s servers=%s", args.city, args.topic, servers)

    session = requests.Session()
    try:
        lat, lon, display = geocode_city(session, args.city)
        logging.info("Géocodage: %s -> lat=%.4f lon=%.4f", display, lat, lon)
    except Exception as e:
        logging.error("Échec géocodage (%s): %s", args.city, e)
        raise SystemExit(1)

    try:
        producer = build_producer(servers)
    except NoBrokersAvailable:
        logging.error("Kafka non joignable sur %s. Lance d’abord: docker compose up -d", servers)
        raise SystemExit(1)

    sent = 0
    for i in range(args.loops):
        try:
            wx = fetch_current_weather(session, lat, lon)
            payload = {
                "city": display,
                "lat": lat,
                "lon": lon,
                "current": wx,
                "ts_sent": datetime.now(timezone.utc).isoformat()
            }
            producer.send(args.topic, payload).get(timeout=10)
            sent += 1
            logging.info("Envoyé %d/%d: %s°C, wind %.1f", sent, args.loops, wx.get("temperature"), wx.get("windspeed"))
        except Exception as e:
            logging.warning("Erreur fetch/envoi (iteration %d): %s", i + 1, e)
        time.sleep(args.interval)

    producer.flush(); producer.close()
    logging.info("Terminé. Messages envoyés: %d", sent)

if __name__ == "__main__":
    main()
