#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur de données météo de test pour l'Exercice 8

Crée des données météo synthétiques pour plusieurs villes/pays afin de 
tester les visualisations.
"""

import json
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

# Données de test pour différentes villes
CITIES_DATA = [
    {"city": "Paris", "country": "FR", "country_code": "FR", "lat": 48.8566, "lon": 2.3522},
    {"city": "London", "country": "GB", "country_code": "GB", "lat": 51.5074, "lon": -0.1278},
    {"city": "Madrid", "country": "ES", "country_code": "ES", "lat": 40.4168, "lon": -3.7038},
    {"city": "Berlin", "country": "DE", "country_code": "DE", "lat": 52.5200, "lon": 13.4050},
    {"city": "Rome", "country": "IT", "country_code": "IT", "lat": 41.9028, "lon": 12.4964},
    {"city": "Amsterdam", "country": "NL", "country_code": "NL", "lat": 52.3676, "lon": 4.9041},
    {"city": "Stockholm", "country": "SE", "country_code": "SE", "lat": 59.3293, "lon": 18.0686},
    {"city": "Prague", "country": "CZ", "country_code": "CZ", "lat": 50.0755, "lon": 14.4378},
]

# Codes météo et leurs probabilités
WEATHER_CODES = [
    (0, 0.15),   # Clear sky
    (1, 0.20),   # Mainly clear
    (2, 0.25),   # Partly cloudy
    (3, 0.20),   # Overcast
    (45, 0.05),  # Fog
    (51, 0.05),  # Light drizzle
    (61, 0.10),  # Slight rain
]

def generate_weather_record(city_data: dict, base_time: datetime, temp_base: float) -> dict:
    """Génère un enregistrement météo aléatoire"""
    
    # Variations temporelles réalistes
    temp_variation = random.uniform(-3, 3)
    wind_variation = random.uniform(0, 15)
    
    # Sélection du code météo
    weather_code = random.choices(
        [code for code, _ in WEATHER_CODES],
        weights=[weight for _, weight in WEATHER_CODES]
    )[0]
    
    # Calcul des niveaux d'alerte
    wind_speed = max(0, 8 + wind_variation)
    temperature = temp_base + temp_variation
    
    # Alertes vent (seuils: 10, 15, 20 m/s)
    if wind_speed < 10:
        wind_alert = "level_0"
    elif wind_speed < 15:
        wind_alert = "level_1"
    else:
        wind_alert = "level_2"
    
    # Alertes chaleur (seuils: 25, 30°C)
    if temperature < 25:
        heat_alert = "level_0"
    elif temperature < 30:
        heat_alert = "level_1"
    else:
        heat_alert = "level_2"
    
    return {
        "event_time": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "ts_sent": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "city": city_data["city"],
        "country": city_data["country"],
        "country_code": city_data["country_code"],
        "lat": city_data["lat"] + random.uniform(-0.1, 0.1),
        "lon": city_data["lon"] + random.uniform(-0.1, 0.1),
        "temperature": round(temperature, 1),
        "windspeed_ms": round(wind_speed, 1),
        "winddirection": random.randint(0, 359),
        "weathercode": weather_code,
        "wind_alert_level": wind_alert,
        "heat_alert_level": heat_alert
    }

def create_test_data(output_dir: str = "./test-weather-data", records_per_city: int = 15):
    """Crée des données de test structurées"""
    
    base_path = Path(output_dir)
    base_path.mkdir(exist_ok=True)
    
    # Températures moyennes par ville (réalistes pour septembre)
    temp_ranges = {
        "Paris": 18, "London": 16, "Madrid": 24, "Berlin": 17,
        "Rome": 22, "Amsterdam": 15, "Stockholm": 12, "Prague": 16
    }
    
    total_records = 0
    
    for city_data in CITIES_DATA:
        city_name = city_data["city"].lower().replace(" ", "_") + "_" + city_data["country_code"].lower()
        country_dir = base_path / city_data["country_code"].lower()
        alerts_dir = country_dir / city_name
        alerts_dir.mkdir(parents=True, exist_ok=True)
        
        # Fichier de destination
        alerts_file = alerts_dir / "alerts.json"
        
        # Génère les enregistrements pour cette ville
        base_time = datetime.now() - timedelta(hours=records_per_city)
        temp_base = temp_ranges.get(city_data["city"], 18)
        
        with open(alerts_file, 'w', encoding='utf-8') as f:
            for i in range(records_per_city):
                record_time = base_time + timedelta(hours=i)
                record = generate_weather_record(city_data, record_time, temp_base)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                total_records += 1
        
        print(f"[OK] Cree {records_per_city} records pour {city_data['city']} -> {alerts_file}")
    
    print(f"\n[SUCCESS] Donnees de test creees !")
    print(f"   Repertoire: {base_path}")
    print(f"   Total records: {total_records}")
    print(f"   Villes: {len(CITIES_DATA)}")
    print(f"   Pays: {len(set(c['country_code'] for c in CITIES_DATA))}")
    
    return str(base_path)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Générateur de données météo de test")
    parser.add_argument("--output-dir", default="./test-weather-data", help="Répertoire de sortie")
    parser.add_argument("--records-per-city", type=int, default=15, help="Nombre d'enregistrements par ville")
    
    args = parser.parse_args()
    
    create_test_data(args.output_dir, args.records_per_city)