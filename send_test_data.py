#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script temporaire pour envoyer des données transformées vers weather_transformed"""

import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

def build_producer(servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=servers.split(","),
        acks="all",
        linger_ms=10,
        retries=3,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )

def main():
    servers = "localhost:9092"
    topic = "weather_transformed"
    
    producer = build_producer(servers)
    
    # Données de test simulant les alertes transformées par Spark
    test_data = [
        {
            "city": "Paris, FR",
            "city_name": "Paris",
            "country_code": "FR", 
            "admin1": "Île-de-France",
            "lat": 48.8534,
            "lon": 2.3488,
            "event_time": "2025-09-23 14:47:05",
            "temperature": 14.9,
            "windspeed_ms": 4.8,  # 17.3 km/h converti
            "winddirection": 220.0,
            "weathercode": 3,
            "wind_alert_level": None,
            "heat_alert_level": None,
            "ts_sent": datetime.now(timezone.utc).isoformat(),
            "severity": "OK"
        },
        {
            "city": "Marseille, FR",
            "city_name": "Marseille", 
            "country_code": "FR",
            "admin1": "Provence-Alpes-Côte d'Azur",
            "lat": 43.2965,
            "lon": 5.3698,
            "event_time": "2025-09-23 14:47:10",
            "temperature": 32.5,
            "windspeed_ms": 6.2,
            "winddirection": 310.0,
            "weathercode": 1,
            "wind_alert_level": None,
            "heat_alert_level": "level_1",  # Chaleur modérée
            "ts_sent": datetime.now(timezone.utc).isoformat(),
            "severity": "OK"
        },
        {
            "city": "Lyon, FR",
            "city_name": "Lyon",
            "country_code": "FR", 
            "admin1": "Auvergne-Rhône-Alpes",
            "lat": 45.7640,
            "lon": 4.8357,
            "event_time": "2025-09-23 14:47:15",
            "temperature": 37.2,
            "windspeed_ms": 18.5,  # Vent fort
            "winddirection": 180.0,
            "weathercode": 95,  # Orage
            "wind_alert_level": "level_2",  # Vent fort
            "heat_alert_level": "level_2",  # Canicule
            "ts_sent": datetime.now(timezone.utc).isoformat(),
            "severity": "CRITICAL"
        }
    ]
    
    print(f"Envoi de {len(test_data)} messages de test vers {topic}...")
    
    for i, data in enumerate(test_data):
        producer.send(topic, data).get(timeout=10)
        print(f"Envoyé {i+1}/{len(test_data)}: {data['city']} - {data['temperature']}°C, wind {data['windspeed_ms']} m/s - {data['severity']}")
        time.sleep(0.5)
    
    producer.flush()
    producer.close()
    print("Terminé!")

if __name__ == "__main__":
    main()