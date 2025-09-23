#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex06 — Producer météo "live" (ville + pays en arguments)
- Accepte:
    * --city-name "Paris" et --country "FR"
    * ou --city "Paris,FR" (compat)
    * ou coordonnées "48.8566,2.3522" (compat)
- Géocode via Open-Meteo Geocoding
- Récupère current_weather
- Envoie des messages JSON dans Kafka (topic par défaut: weather_stream)
- Chaque message inclut: city_name, country_code, country (+ meta admin1/admin2)

Exemples:
  python producer_weather.py --city-name "Paris" --country "FR" --loops 5 --interval 1
  python producer_weather.py --city "Paris,FR" --loops 3
  python producer_weather.py --city "48.8566,2.3522" --loops 3
"""

import os, time, json, argparse, logging, re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
WX_URL  = "https://api.open-meteo.com/v1/forecast"

_COORDS_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$")

def parse_coords(s: str) -> Optional[Tuple[float, float]]:
    m = _COORDS_RE.match(s or "")
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))

def om_geocode(session: requests.Session, name: str, count: int = 5) -> List[Dict[str, Any]]:
    r = session.get(GEO_URL, params={"name": name, "count": count, "language": "en", "format": "json"}, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("results") or []

def geocode_city(
    session: requests.Session,
    query: str,
    prefer_country: Optional[str] = None
) -> Tuple[float, float, Dict[str, Any]]:
    """
    Renvoie (lat, lon, meta) avec meta = {
        name, country, country_code, admin1, admin2, display
    }

    Stratégie:
      - si query = "lat,lon" -> retourne direct
      - sinon geocode; si prefer_country fourni (ex "FR"), sélectionne le meilleur match avec ce code
      - sinon prend le premier résultat
    """
    coords = parse_coords(query)
    if coords:
        lat, lon = coords
        meta = {
            "name": f"coords({lat:.4f},{lon:.4f})",
            "country": None,
            "country_code": None,
            "admin1": None,
            "admin2": None,
        }
        meta["display"] = meta["name"]
        return lat, lon, meta

    results = om_geocode(session, query, count=8)
    if not results:
        # Si "Ville,CC" -> retry sur "Ville" avec préférence CC
        parts = [p.strip() for p in (query or "").split(",") if p.strip()]
        if len(parts) >= 2:
            base, cc = parts[0], parts[-1]
            results = om_geocode(session, base, count=8)
            prefer_country = prefer_country or (cc.upper() if len(cc) in (2, 3) else None)

    if not results:
        raise ValueError(f"Ville introuvable: {query}")

    pick = None
    if prefer_country:
        pc = prefer_country.upper()
        for r in results:
            if (r.get("country_code") or "").upper() == pc:
                pick = r
                break
    if pick is None:
        pick = results[0]

    lat = float(pick["latitude"])
    lon = float(pick["longitude"])
    meta = {
        "name": pick.get("name"),
        "country": pick.get("country"),
        "country_code": (pick.get("country_code") or "").upper() or None,
        "admin1": pick.get("admin1"),
        "admin2": pick.get("admin2"),
    }
    meta["display"] = f'{meta["name"]}, {meta["country_code"]}' if meta["country_code"] else meta["name"]
    return lat, lon, meta

def build_producer(servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=servers.split(","),
        acks="all",
        linger_ms=10,
        retries=3,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )

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

def main():
    parser = argparse.ArgumentParser()
    # Nouveaux arguments séparés
    parser.add_argument("--city-name", help='Nom de la ville (ex: "Paris")')
    parser.add_argument("--country", help='Code pays (ex: "FR")')
    # Compat: ancien --city "Paris,FR" ou "lat,lon"
    parser.add_argument("--city", help='Compat: "Paris,FR" ou "48.8566,2.3522"')
    parser.add_argument("--topic", default=os.getenv("TOPIC", "weather_stream"))
    parser.add_argument("--loops", type=int, default=int(os.getenv("LOOPS", "5")))
    parser.add_argument("--interval", type=float, default=float(os.getenv("INTERVAL", "1")))
    args = parser.parse_args()

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Construire la requête de géocodage à partir des nouveaux args
    if args.city_name:
        query = args.city_name if not args.country else f"{args.city_name},{args.country}"
        prefer_country = args.country
    elif args.city:
        query = args.city
        # Si --city a la forme "Ville,CC", on peut déduire une préférence
        parts = [p.strip() for p in args.city.split(",") if p.strip()]
        prefer_country = parts[-1] if len(parts) >= 2 and len(parts[-1]) in (2, 3) else None
    else:
        # Valeur par défaut identique à avant
        query = os.getenv("CITY", "Paris,FR")
        prefer_country = None

    logging.info("Producer météo -> query=%s topic=%s servers=%s", query, args.topic, servers)

    session = requests.Session()
    try:
        lat, lon, meta = geocode_city(session, query, prefer_country=prefer_country)
        display = meta["display"]
        logging.info("Géocodage OK: %s -> lat=%.4f lon=%.4f (country=%s)",
                     display, lat, lon, meta.get("country_code"))
    except Exception as e:
        logging.error("Échec géocodage (%s): %s", query, e)
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
                # Champs *séparés* pour partitionnements et agrégats région
                "city_name": meta.get("name"),
                "country": meta.get("country"),                 # ex: "France"
                "country_code": meta.get("country_code"),       # ex: "FR"
                "admin1": meta.get("admin1"),                   # région (si dispo)
                "admin2": meta.get("admin2"),                   # département (si dispo)

                # Compat / lisible
                "city": display,
                "lat": lat,
                "lon": lon,

                # Mesure
                "current": wx,

                # Métadonnées
                "ts_sent": datetime.now(timezone.utc).isoformat(),
                "version": 2
            }
            producer.send(args.topic, payload).get(timeout=10)
            sent += 1
            logging.info("Envoyé %d/%d: %s°C, wind %.1f (%s, %s)",
                         sent, args.loops, wx.get("temperature"), wx.get("windspeed"),
                         payload["city_name"], payload["country_code"])
        except Exception as e:
            logging.warning("Erreur fetch/envoi (it=%d): %s", i + 1, e)
        time.sleep(args.interval)

    producer.flush(); producer.close()
    logging.info("Terminé. Messages envoyés: %d", sent)

if __name__ == "__main__":
    main()
