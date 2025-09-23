#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exercice 8 - Visualisation et agrégation des logs météo

Ce script lit les logs stockés dans HDFS (ou localement) et génère des visualisations :
1. Évolution de la température au fil du temps
2. Évolution de la vitesse du vent  
3. Nombre d'alertes vent et chaleur par niveau
4. Code météo le plus fréquent par pays

Usage:
    python visualize_weather_logs.py --source ./hdfs-final-test
    python visualize_weather_logs.py --hdfs-url http://localhost:9870 --hdfs-path /hdfs-data
"""

import argparse
import json
import logging
import os
import re
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import seaborn as sns
from hdfs import InsecureClient
from hdfs.util import HdfsError

# Configuration des styles
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Codes météo WMO (simplifié)
WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail"
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Visualisation des logs météo")
    
    # Source des données
    parser.add_argument("--source", help="Répertoire local contenant les logs (ex: ./hdfs-final-test)")
    parser.add_argument("--hdfs-url", help="URL HDFS WebHDFS (ex: http://localhost:9870)")
    parser.add_argument("--hdfs-path", default="/hdfs-data", help="Chemin HDFS racine")
    parser.add_argument("--hdfs-user", default="root", help="Utilisateur HDFS")
    
    # Options de sortie
    parser.add_argument("--output-dir", default="./visualizations", help="Répertoire de sortie pour les graphiques")
    parser.add_argument("--format", default="png", choices=["png", "jpg", "pdf", "svg"], help="Format des images")
    parser.add_argument("--dpi", type=int, default=300, help="Résolution des images")
    parser.add_argument("--show", action="store_true", help="Afficher les graphiques à l'écran")
    
    # Filtres
    parser.add_argument("--country", help="Filtrer par pays (ex: FR)")
    parser.add_argument("--city", help="Filtrer par ville")
    parser.add_argument("--limit", type=int, help="Limiter le nombre de records à analyser")
    
    return parser.parse_args()

class WeatherLogReader:
    """Lecteur de logs météo depuis HDFS ou système de fichiers local"""
    
    def __init__(self, hdfs_url: Optional[str] = None, hdfs_user: str = "root"):
        self.hdfs_client = None
        if hdfs_url:
            try:
                self.hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
                logging.info(f"Connexion HDFS établie : {hdfs_url}")
            except Exception as e:
                logging.warning(f"Impossible de se connecter à HDFS : {e}")
    
    def read_from_local(self, base_path: str, limit: Optional[int] = None) -> List[Dict]:
        """Lit les logs depuis le système de fichiers local"""
        records = []
        base_path = Path(base_path)
        
        if not base_path.exists():
            raise FileNotFoundError(f"Le répertoire {base_path} n'existe pas")
        
        # Parcourt récursivement tous les fichiers alerts.json
        for alerts_file in base_path.rglob("alerts.json"):
            logging.info(f"Traitement de {alerts_file}")
            
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
                            # Ajoute des métadonnées sur le chemin
                            record['_file_path'] = str(alerts_file)
                            record['_line_number'] = line_num
                            records.append(record)
                        except json.JSONDecodeError as e:
                            logging.warning(f"Ligne JSON invalide dans {alerts_file}:{line_num} : {e}")
                            
            except Exception as e:
                logging.error(f"Erreur lors de la lecture de {alerts_file} : {e}")
        
        logging.info(f"Total de {len(records)} records lus depuis {base_path}")
        return records
    
    def read_from_hdfs(self, hdfs_path: str, limit: Optional[int] = None) -> List[Dict]:
        """Lit les logs depuis HDFS"""
        if not self.hdfs_client:
            raise ValueError("Client HDFS non initialisé")
        
        records = []
        
        try:
            # Liste récursivement tous les fichiers alerts.json dans HDFS
            for root, dirs, files in self.hdfs_client.walk(hdfs_path):
                for file_name in files:
                    if file_name == "alerts.json":
                        file_path = f"{root}/{file_name}"
                        logging.info(f"Traitement HDFS : {file_path}")
                        
                        try:
                            with self.hdfs_client.read(file_path, encoding='utf-8') as f:
                                for line_num, line in enumerate(f, 1):
                                    if limit and len(records) >= limit:
                                        return records
                                    
                                    line = line.strip()
                                    if not line:
                                        continue
                                    
                                    try:
                                        record = json.loads(line)
                                        record['_file_path'] = file_path
                                        record['_line_number'] = line_num
                                        records.append(record)
                                    except json.JSONDecodeError as e:
                                        logging.warning(f"Ligne JSON invalide dans {file_path}:{line_num} : {e}")
                                        
                        except Exception as e:
                            logging.error(f"Erreur lors de la lecture HDFS de {file_path} : {e}")
        
        except HdfsError as e:
            logging.error(f"Erreur HDFS : {e}")
            raise
        
        logging.info(f"Total de {len(records)} records lus depuis HDFS {hdfs_path}")
        return records

class WeatherAnalyzer:
    """Analyseur et générateur de visualisations pour les données météo"""
    
    def __init__(self, records: List[Dict], output_dir: str = "./visualizations"):
        self.records = records
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Convertit en DataFrame pour faciliter l'analyse
        self.df = self._records_to_dataframe()
        
    def _records_to_dataframe(self) -> pd.DataFrame:
        """Convertit les records en DataFrame pandas avec nettoyage des données"""
        if not self.records:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.records)
        
        # Nettoyage et conversion des types
        if 'event_time' in df.columns:
            df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
        if 'ts_sent' in df.columns:
            df['ts_sent'] = pd.to_datetime(df['ts_sent'], errors='coerce')
        
        # Convertit les colonnes numériques
        numeric_cols = ['temperature', 'windspeed_ms', 'winddirection', 'weathercode', 'lat', 'lon']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Extraction du pays depuis le chemin de fichier si pas disponible
        if 'country_code' not in df.columns or df['country_code'].isna().all():
            df['country_code'] = df['_file_path'].str.extract(r'/([A-Z]{2})/')
        
        # Ajoute les libellés des codes météo
        if 'weathercode' in df.columns:
            df['weather_description'] = df['weathercode'].map(WMO_CODES).fillna('Unknown')
        
        logging.info(f"DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
        return df
    
    def plot_temperature_evolution(self, save: bool = True, show: bool = False, format: str = "png", dpi: int = 300):
        """Graphique 1 : Évolution de la température au fil du temps"""
        if self.df.empty or 'temperature' not in self.df.columns:
            logging.warning("Pas de données de température disponibles")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Groupe par ville/pays pour différentes courbes
        if 'city' in self.df.columns and 'country_code' in self.df.columns:
            for (city, country), group in self.df.groupby(['city', 'country_code']):
                if len(group) > 1:  # Au moins 2 points pour tracer une ligne
                    group_sorted = group.sort_values('event_time')
                    plt.plot(group_sorted['event_time'], group_sorted['temperature'], 
                            marker='o', label=f"{city} ({country})", alpha=0.7)
        else:
            # Fallback : tous les points ensemble
            df_sorted = self.df.sort_values('event_time')
            plt.plot(df_sorted['event_time'], df_sorted['temperature'], 
                    marker='o', alpha=0.7)
        
        plt.title("Évolution de la température au fil du temps", fontsize=14, fontweight='bold')
        plt.xlabel("Temps")
        plt.ylabel("Température (°C)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Format des dates sur l'axe X
        if 'event_time' in self.df.columns and not self.df['event_time'].isna().all():
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        if save:
            file_path = self.output_dir / f"temperature_evolution.{format}"
            plt.savefig(file_path, dpi=dpi, bbox_inches='tight')
            logging.info(f"Graphique sauvegardé : {file_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def plot_wind_speed_evolution(self, save: bool = True, show: bool = False, format: str = "png", dpi: int = 300):
        """Graphique 2 : Évolution de la vitesse du vent"""
        if self.df.empty or 'windspeed_ms' not in self.df.columns:
            logging.warning("Pas de données de vitesse du vent disponibles")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Groupe par ville/pays
        if 'city' in self.df.columns and 'country_code' in self.df.columns:
            for (city, country), group in self.df.groupby(['city', 'country_code']):
                if len(group) > 1:
                    group_sorted = group.sort_values('event_time')
                    plt.plot(group_sorted['event_time'], group_sorted['windspeed_ms'], 
                            marker='s', label=f"{city} ({country})", alpha=0.7)
        else:
            df_sorted = self.df.sort_values('event_time')
            plt.plot(df_sorted['event_time'], df_sorted['windspeed_ms'], 
                    marker='s', alpha=0.7)
        
        plt.title("Évolution de la vitesse du vent au fil du temps", fontsize=14, fontweight='bold')
        plt.xlabel("Temps")
        plt.ylabel("Vitesse du vent (m/s)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        if 'event_time' in self.df.columns and not self.df['event_time'].isna().all():
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        if save:
            file_path = self.output_dir / f"wind_speed_evolution.{format}"
            plt.savefig(file_path, dpi=dpi, bbox_inches='tight')
            logging.info(f"Graphique sauvegardé : {file_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def plot_alert_levels(self, save: bool = True, show: bool = False, format: str = "png", dpi: int = 300):
        """Graphique 3 : Nombre d'alertes vent et chaleur par niveau"""
        alert_cols = ['wind_alert_level', 'heat_alert_level']
        available_cols = [col for col in alert_cols if col in self.df.columns]
        
        if not available_cols:
            logging.warning("Pas de données d'alertes disponibles")
            return
        
        fig, axes = plt.subplots(1, len(available_cols), figsize=(6*len(available_cols), 6))
        if len(available_cols) == 1:
            axes = [axes]
        
        for i, col in enumerate(available_cols):
            # Compte les alertes par niveau (excluant les None/NaN)
            alert_counts = self.df[col].value_counts().dropna()
            
            if alert_counts.empty:
                axes[i].text(0.5, 0.5, 'Pas de données\nd\'alertes', 
                           ha='center', va='center', transform=axes[i].transAxes)
                axes[i].set_title(f"Alertes {col.replace('_', ' ').title()}")
                continue
            
            # Graphique en barres
            bars = axes[i].bar(alert_counts.index, alert_counts.values, 
                              color=['green', 'orange', 'red'][:len(alert_counts)])
            
            # Ajoute les valeurs sur les barres
            for bar, value in zip(bars, alert_counts.values):
                axes[i].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                           str(value), ha='center', va='bottom')
            
            axes[i].set_title(f"Alertes {col.replace('_', ' ').title()}")
            axes[i].set_xlabel("Niveau d'alerte")
            axes[i].set_ylabel("Nombre d'occurrences")
            axes[i].grid(True, alpha=0.3, axis='y')
        
        plt.suptitle("Distribution des alertes météo par niveau", fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        if save:
            file_path = self.output_dir / f"alert_levels.{format}"
            plt.savefig(file_path, dpi=dpi, bbox_inches='tight')
            logging.info(f"Graphique sauvegardé : {file_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def plot_weather_codes_by_country(self, save: bool = True, show: bool = False, format: str = "png", dpi: int = 300):
        """Graphique 4 : Code météo le plus fréquent par pays"""
        if self.df.empty or 'weathercode' not in self.df.columns:
            logging.warning("Pas de données de codes météo disponibles")
            return
        
        # Groupe par pays et compte les codes météo
        country_col = 'country_code' if 'country_code' in self.df.columns else 'country'
        if country_col not in self.df.columns:
            logging.warning("Pas de données de pays disponibles")
            return
        
        country_weather = self.df.groupby([country_col, 'weathercode']).size().reset_index(name='count')
        
        # Pour chaque pays, trouve le code météo le plus fréquent
        most_frequent = country_weather.loc[country_weather.groupby(country_col)['count'].idxmax()]
        
        if most_frequent.empty:
            logging.warning("Pas assez de données pour analyser par pays")
            return
        
        plt.figure(figsize=(10, 6))
        
        # Graphique en barres horizontales pour une meilleure lisibilité
        countries = most_frequent[country_col]
        codes = most_frequent['weathercode'] 
        counts = most_frequent['count']
        
        bars = plt.barh(range(len(countries)), counts, alpha=0.7)
        
        # Personnalise les étiquettes
        labels = []
        for country, code in zip(countries, codes):
            weather_desc = WMO_CODES.get(code, f"Code {code}")
            labels.append(f"{country}\n({weather_desc})")
        
        plt.yticks(range(len(countries)), labels)
        plt.xlabel("Nombre d'occurrences")
        plt.title("Code météo le plus fréquent par pays", fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3, axis='x')
        
        # Ajoute les valeurs à droite des barres
        for i, (bar, count) in enumerate(zip(bars, counts)):
            plt.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                    str(count), ha='left', va='center')
        
        plt.tight_layout()
        
        if save:
            file_path = self.output_dir / f"weather_codes_by_country.{format}"
            plt.savefig(file_path, dpi=dpi, bbox_inches='tight')
            logging.info(f"Graphique sauvegardé : {file_path}")
        
        if show:
            plt.show()
        else:
            plt.close()
    
    def generate_summary_report(self, save: bool = True) -> str:
        """Génère un rapport de synthèse textuel"""
        if self.df.empty:
            return "Aucune donnée disponible pour générer le rapport."
        
        report = []
        report.append("=" * 60)
        report.append("RAPPORT DE SYNTHÈSE - DONNÉES MÉTÉOROLOGIQUES")
        report.append("=" * 60)
        report.append(f"Période d'analyse : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Nombre total d'enregistrements : {len(self.df)}")
        report.append("")
        
        # Statistiques par pays/ville
        if 'country_code' in self.df.columns:
            countries = self.df['country_code'].value_counts()
            report.append("RÉPARTITION PAR PAYS :")
            for country, count in countries.head(10).items():
                report.append(f"  {country}: {count} enregistrements")
            report.append("")
        
        # Statistiques de température
        if 'temperature' in self.df.columns:
            temp_stats = self.df['temperature'].describe()
            report.append("STATISTIQUES DE TEMPÉRATURE (°C) :")
            for stat, value in temp_stats.items():
                report.append(f"  {stat.capitalize()}: {value:.2f}")
            report.append("")
        
        # Statistiques de vent
        if 'windspeed_ms' in self.df.columns:
            wind_stats = self.df['windspeed_ms'].describe()
            report.append("STATISTIQUES DE VENT (m/s) :")
            for stat, value in wind_stats.items():
                report.append(f"  {stat.capitalize()}: {value:.2f}")
            report.append("")
        
        # Alertes
        alert_cols = ['wind_alert_level', 'heat_alert_level']
        for col in alert_cols:
            if col in self.df.columns:
                alerts = self.df[col].value_counts().dropna()
                if not alerts.empty:
                    report.append(f"ALERTES {col.replace('_', ' ').upper()} :")
                    for level, count in alerts.items():
                        report.append(f"  {level}: {count}")
                    report.append("")
        
        # Codes météo les plus fréquents
        if 'weathercode' in self.df.columns:
            weather_codes = self.df['weathercode'].value_counts().head(5)
            report.append("CODES MÉTÉO LES PLUS FRÉQUENTS :")
            for code, count in weather_codes.items():
                desc = WMO_CODES.get(code, f"Code {code}")
                report.append(f"  {desc} ({code}): {count}")
            report.append("")
        
        report.append("=" * 60)
        
        report_text = "\n".join(report)
        
        if save:
            report_file = self.output_dir / "weather_summary_report.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            logging.info(f"Rapport sauvegardé : {report_file}")
        
        return report_text
    
    def generate_all_visualizations(self, **kwargs):
        """Génère toutes les visualisations"""
        logging.info("Génération de toutes les visualisations...")
        
        self.plot_temperature_evolution(**kwargs)
        self.plot_wind_speed_evolution(**kwargs)
        self.plot_alert_levels(**kwargs)
        self.plot_weather_codes_by_country(**kwargs)
        
        # Génère le rapport de synthèse
        report = self.generate_summary_report()
        
        logging.info(f"Toutes les visualisations ont été générées dans {self.output_dir}")
        return report

def main():
    args = parse_args()
    
    # Configuration du logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    
    # Initialise le lecteur
    reader = WeatherLogReader(hdfs_url=args.hdfs_url, hdfs_user=args.hdfs_user)
    
    # Lit les données
    if args.hdfs_url:
        records = reader.read_from_hdfs(args.hdfs_path, limit=args.limit)
    elif args.source:
        records = reader.read_from_local(args.source, limit=args.limit)
    else:
        logging.error("Vous devez spécifier soit --source soit --hdfs-url")
        return 1
    
    if not records:
        logging.error("Aucune donnée trouvée à analyser")
        return 1
    
    # Filtre les données si demandé
    if args.country:
        records = [r for r in records if r.get('country_code') == args.country or r.get('country') == args.country]
        logging.info(f"Filtré par pays {args.country}: {len(records)} records")
    
    if args.city:
        records = [r for r in records if args.city.lower() in str(r.get('city', '')).lower()]
        logging.info(f"Filtré par ville {args.city}: {len(records)} records")
    
    # Crée l'analyseur et génère les visualisations
    analyzer = WeatherAnalyzer(records, output_dir=args.output_dir)
    
    viz_params = {
        'save': True,
        'show': args.show,
        'format': args.format,
        'dpi': args.dpi
    }
    
    report = analyzer.generate_all_visualizations(**viz_params)
    
    # Affiche le rapport
    print("\n" + report)
    
    logging.info("Analyse terminée avec succès !")
    return 0

if __name__ == "__main__":
    exit(main())