#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exercice 8 - Version finale compatible Windows

Visualisation simplifiée des données météo sans problèmes d'encodage.
"""

import argparse
import json
import os
import sys
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Import conditionnel de matplotlib
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Codes météo WMO
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail"
}

def load_weather_data(data_path: str, limit: Optional[int] = None) -> List[Dict]:
    """Charge les données météo depuis le répertoire local"""
    records = []
    data_path = Path(data_path)
    
    if not data_path.exists():
        raise FileNotFoundError(f"Le repertoire {data_path} n'existe pas")
    
    print(f"Recherche de fichiers dans {data_path}")
    
    for alerts_file in data_path.rglob("alerts.json"):
        print(f"Traitement de {alerts_file}")
        
        try:
            with open(alerts_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if limit and len(records) >= limit:
                        return records
                        
                    line = line.strip()
                    if not line:
                        continue
                        
                    try:
                        record = json.loads(line)
                        record['_file_path'] = str(alerts_file)
                        record['_line_number'] = line_num
                        records.append(record)
                    except json.JSONDecodeError:
                        print(f"Ligne JSON invalide dans {alerts_file}:{line_num}")
                        
        except Exception as e:
            print(f"Erreur lors de la lecture de {alerts_file} : {e}")
    
    print(f"Total de {len(records)} records charges")
    return records

def analyze_data_basic(records: List[Dict]) -> Dict:
    """Analyse basique des données"""
    if not records:
        return {}
    
    analysis = {
        'total_records': len(records),
        'countries': Counter(),
        'cities': Counter(),
        'temperatures': [],
        'wind_speeds': [],
        'weather_codes': Counter(),
        'wind_alerts': Counter(),
        'heat_alerts': Counter()
    }
    
    for record in records:
        # Pays
        country = record.get('country_code') or record.get('country')
        if country:
            analysis['countries'][country] += 1
        
        # Villes
        city = record.get('city')
        if city:
            analysis['cities'][city] += 1
        
        # Température
        temp = record.get('temperature')
        if temp is not None:
            try:
                analysis['temperatures'].append(float(temp))
            except (ValueError, TypeError):
                pass
        
        # Vitesse du vent
        wind = record.get('windspeed_ms')
        if wind is not None:
            try:
                analysis['wind_speeds'].append(float(wind))
            except (ValueError, TypeError):
                pass
        
        # Codes météo
        weather_code = record.get('weathercode')
        if weather_code is not None:
            try:
                analysis['weather_codes'][int(weather_code)] += 1
            except (ValueError, TypeError):
                pass
        
        # Alertes
        wind_alert = record.get('wind_alert_level')
        if wind_alert:
            analysis['wind_alerts'][wind_alert] += 1
            
        heat_alert = record.get('heat_alert_level')
        if heat_alert:
            analysis['heat_alerts'][heat_alert] += 1
    
    return analysis

def print_analysis_report(analysis: Dict):
    """Affiche un rapport d'analyse en mode texte"""
    print("\n" + "="*60)
    print("RAPPORT D'ANALYSE - DONNEES METEOROLOGIQUES")
    print("="*60)
    print(f"Genere le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Nombre total d'enregistrements : {analysis['total_records']}")
    print()
    
    if analysis['countries']:
        print("REPARTITION PAR PAYS :")
        for country, count in analysis['countries'].most_common(10):
            print(f"   {country}: {count} enregistrements")
        print()
    
    if analysis['cities']:
        print("REPARTITION PAR VILLE :")
        for city, count in analysis['cities'].most_common(5):
            print(f"   {city}: {count} enregistrements")
        print()
    
    if analysis['temperatures']:
        temps = analysis['temperatures']
        print("STATISTIQUES DE TEMPERATURE (°C) :")
        print(f"   Minimum: {min(temps):.1f}")
        print(f"   Maximum: {max(temps):.1f}")
        print(f"   Moyenne: {sum(temps)/len(temps):.1f}")
        print()
    
    if analysis['wind_speeds']:
        winds = analysis['wind_speeds']
        print("STATISTIQUES DE VENT (m/s) :")
        print(f"   Minimum: {min(winds):.1f}")
        print(f"   Maximum: {max(winds):.1f}")
        print(f"   Moyenne: {sum(winds)/len(winds):.1f}")
        print()
    
    if analysis['wind_alerts']:
        print("ALERTES VENT :")
        for level, count in analysis['wind_alerts'].items():
            print(f"   {level}: {count}")
        print()
    
    if analysis['heat_alerts']:
        print("ALERTES CHALEUR :")
        for level, count in analysis['heat_alerts'].items():
            print(f"   {level}: {count}")
        print()
    
    if analysis['weather_codes']:
        print("CODES METEO LES PLUS FREQUENTS :")
        for code, count in analysis['weather_codes'].most_common(5):
            desc = WMO_CODES.get(code, f"Code {code}")
            print(f"   {desc} ({code}): {count}")
        print()
    
    print("="*60)

def create_simple_plots(records: List[Dict], analysis: Dict, output_dir: str = "./visualizations"):
    """Crée des graphiques simples avec matplotlib"""
    if not HAS_MATPLOTLIB:
        print("matplotlib non disponible pour creer les graphiques")
        return
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    plt.style.use('default')
    
    # 1. Distribution des températures
    if analysis['temperatures']:
        plt.figure(figsize=(10, 6))
        plt.hist(analysis['temperatures'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
        plt.title("Distribution des temperatures", fontsize=14, fontweight='bold')
        plt.xlabel("Temperature (°C)")
        plt.ylabel("Frequence")
        plt.grid(True, alpha=0.3)
        
        temps = analysis['temperatures']
        plt.axvline(sum(temps)/len(temps), color='red', linestyle='--', 
                   label=f'Moyenne: {sum(temps)/len(temps):.1f}°C')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(output_path / "temperature_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Graphique sauvegarde : {output_path}/temperature_distribution.png")
    
    # 2. Distribution des vitesses de vent
    if analysis['wind_speeds']:
        plt.figure(figsize=(10, 6))
        plt.hist(analysis['wind_speeds'], bins=20, alpha=0.7, color='lightgreen', edgecolor='black')
        plt.title("Distribution des vitesses de vent", fontsize=14, fontweight='bold')
        plt.xlabel("Vitesse du vent (m/s)")
        plt.ylabel("Frequence")
        plt.grid(True, alpha=0.3)
        
        winds = analysis['wind_speeds']
        plt.axvline(sum(winds)/len(winds), color='red', linestyle='--',
                   label=f'Moyenne: {sum(winds)/len(winds):.1f} m/s')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(output_path / "wind_speed_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Graphique sauvegarde : {output_path}/wind_speed_distribution.png")
    
    # 3. Distribution des alertes
    alert_data = []
    alert_labels = []
    
    if analysis['wind_alerts']:
        for level, count in analysis['wind_alerts'].items():
            alert_data.append(count)
            alert_labels.append(f"Vent {level}")
    
    if analysis['heat_alerts']:
        for level, count in analysis['heat_alerts'].items():
            alert_data.append(count)
            alert_labels.append(f"Chaleur {level}")
    
    if alert_data:
        plt.figure(figsize=(10, 6))
        colors = ['green', 'orange', 'red'] * (len(alert_data) // 3 + 1)
        bars = plt.bar(alert_labels, alert_data, color=colors[:len(alert_data)], alpha=0.7)
        
        for bar, value in zip(bars, alert_data):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    str(value), ha='center', va='bottom')
        
        plt.title("Distribution des alertes meteo", fontsize=14, fontweight='bold')
        plt.xlabel("Type et niveau d'alerte")
        plt.ylabel("Nombre d'occurrences")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(output_path / "alerts_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Graphique sauvegarde : {output_path}/alerts_distribution.png")
    
    # 4. Distribution par pays
    if analysis['countries'] and analysis['weather_codes']:
        top_weather_code = analysis['weather_codes'].most_common(1)[0][0]
        top_weather_desc = WMO_CODES.get(top_weather_code, f"Code {top_weather_code}")
        
        plt.figure(figsize=(10, 6))
        countries = list(analysis['countries'].keys())[:10]
        counts = [analysis['countries'][country] for country in countries]
        
        plt.bar(countries, counts, alpha=0.7, color='lightcoral')
        plt.title(f"Enregistrements par pays\n(Code meteo global le plus frequent: {top_weather_desc})", 
                 fontsize=14, fontweight='bold')
        plt.xlabel("Pays")
        plt.ylabel("Nombre d'enregistrements")
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3, axis='y')
        
        for country, count in zip(countries, counts):
            plt.text(country, count + 0.1, str(count), ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig(output_path / "countries_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Graphique sauvegarde : {output_path}/countries_distribution.png")

def main():
    parser = argparse.ArgumentParser(description="Visualisation finale des logs meteo")
    parser.add_argument("data_path", help="Repertoire contenant les logs")
    parser.add_argument("--limit", type=int, help="Limiter le nombre de records")
    parser.add_argument("--output-dir", default="./visualizations", help="Repertoire de sortie")
    parser.add_argument("--no-plots", action="store_true", help="Ne pas creer de graphiques")
    
    args = parser.parse_args()
    
    try:
        print("Demarrage de l'analyse des donnees meteo")
        records = load_weather_data(args.data_path, limit=args.limit)
        
        if not records:
            print("Aucune donnee trouvee")
            return 1
        
        print("Analyse des donnees...")
        analysis = analyze_data_basic(records)
        
        print_analysis_report(analysis)
        
        if not args.no_plots:
            print("Creation des graphiques...")
            create_simple_plots(records, analysis, args.output_dir)
        
        print("Analyse terminee avec succes !")
        return 0
        
    except Exception as e:
        print(f"Erreur : {e}")
        return 1

if __name__ == "__main__":
    exit(main())