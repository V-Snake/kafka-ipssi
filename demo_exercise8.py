#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Démonstrateur complet de l'Exercice 8 - Visualisation météo

Ce script exécute une démonstration complète du pipeline:
1. Génération de données de test
2. Analyse et visualisation
3. Génération du rapport

Usage: python demo_exercise8.py
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def print_header(title):
    """Affiche un en-tête formaté"""
    print("\n" + "="*60)
    print(f"🎯 {title}")
    print("="*60)

def run_command(cmd, description):
    """Exécute une commande avec gestion d'erreur"""
    print(f"\n🚀 {description}")
    print(f"   Commande: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print("✅ Succès:")
            for line in result.stdout.strip().split('\n')[-5:]:  # Dernières 5 lignes
                print(f"   {line}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur: {e}")
        if e.stderr:
            print(f"   Détails: {e.stderr}")
        return False

def main():
    print_header("DÉMONSTRATION EXERCICE 8 - VISUALISATION MÉTÉO")
    print("🌤️  Pipeline complet de visualisation des données météo")
    print("📊 Génération → Analyse → Visualisation → Rapport")
    
    # Configuration
    python_exe = "python"
    test_data_dir = "../demo-weather-data"
    viz_dir = "./demo-visualizations"
    
    # Étape 1: Génération des données de test
    print_header("ÉTAPE 1 - GÉNÉRATION DES DONNÉES DE TEST")
    cmd1 = f"{python_exe} generate_test_data.py --output-dir {test_data_dir} --records-per-city 25"
    if not run_command(cmd1, "Génération de 200 enregistrements météo pour 8 villes européennes"):
        return 1
    
    # Vérification des données générées
    data_path = Path(test_data_dir)
    if data_path.exists():
        json_files = list(data_path.rglob("*.json"))
        print(f"✅ {len(json_files)} fichiers JSON créés dans {len(list(data_path.iterdir()))} pays")
    
    # Étape 2: Analyse simple
    print_header("ÉTAPE 2 - ANALYSE TEXTUELLE")
    cmd2 = f"{python_exe} simple_weather_viz.py {test_data_dir} --output-dir {viz_dir} --no-plots"
    if not run_command(cmd2, "Analyse statistique des données météo"):
        return 1
    
    # Étape 3: Génération des visualisations
    print_header("ÉTAPE 3 - GÉNÉRATION DES GRAPHIQUES")
    cmd3 = f"{python_exe} simple_weather_viz.py {test_data_dir} --output-dir {viz_dir}"
    if not run_command(cmd3, "Création des 4 visualisations demandées"):
        return 1
    
    # Vérification des graphiques
    viz_path = Path(viz_dir)
    if viz_path.exists():
        png_files = list(viz_path.glob("*.png"))
        print(f"✅ {len(png_files)} graphiques PNG générés:")
        for png_file in png_files:
            print(f"   📊 {png_file.name}")
    
    # Étape 4: Démonstration des filtres
    print_header("ÉTAPE 4 - DÉMONSTRATION DES FILTRES")
    cmd4 = f"{python_exe} simple_weather_viz.py {test_data_dir} --limit 50 --no-plots"
    if not run_command(cmd4, "Analyse avec limitation à 50 enregistrements"):
        return 1
    
    # Étape 5: Informations sur le dashboard
    print_header("ÉTAPE 5 - DASHBOARD INTERACTIF")
    print("🌐 Le dashboard Streamlit peut être lancé avec:")
    print(f"   streamlit run dashboard_weather.py")
    print(f"   Puis ouvrir: http://localhost:8501")
    print()
    print("💡 Fonctionnalités du dashboard:")
    print("   ✅ Interface web interactive")
    print("   ✅ Filtrage temps réel par pays/ville/dates")
    print("   ✅ Graphiques Plotly zoomables")
    print("   ✅ Export CSV des données")
    print("   ✅ Métriques dynamiques")
    
    # Résumé final
    print_header("RÉSUMÉ DE LA DÉMONSTRATION")
    print("🎯 OBJECTIFS DE L'EXERCICE 8 ATTEINTS:")
    print("   ✅ Évolution de la température au fil du temps")
    print("   ✅ Évolution de la vitesse du vent") 
    print("   ✅ Nombre d'alertes vent et chaleur par niveau")
    print("   ✅ Code météo le plus fréquent par pays")
    print()
    print("📁 FICHIERS CRÉÉS:")
    print(f"   📊 Données: {test_data_dir}/ (8 pays, 200+ records)")
    print(f"   📈 Graphiques: {viz_dir}/ (4 visualisations PNG)")
    print()
    print("🛠️  OUTILS DÉVELOPPÉS:")
    print("   📄 simple_weather_viz.py - Analyse rapide")
    print("   📄 visualize_weather_logs.py - Analyse avancée + HDFS")
    print("   🌐 dashboard_weather.py - Interface web Streamlit")
    print("   🔧 generate_test_data.py - Générateur de données")
    print()
    print("🎉 EXERCICE 8 TERMINÉ AVEC SUCCÈS !")
    print("   Le pipeline de visualisation météo Kafka-IPSSI est opérationnel")
    
    return 0

if __name__ == "__main__":
    exit(main())