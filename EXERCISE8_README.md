# 📊 Exercice 8 - Visualisation et agrégation des logs météo

## 🎯 Objectif

Consommer les logs HDFS et implémenter les visualisations suivantes :
1. **Évolution de la température au fil du temps**
2. **Évolution de la vitesse du vent**
3. **Nombre d'alertes vent et chaleur par niveau**
4. **Code météo le plus fréquent par pays**

## 🚀 Solutions implémentées

### 1. Script de visualisation simple (`simple_weather_viz.py`)

**Analyse en mode texte + graphiques matplotlib**

```bash
# Analyse des données de test
python simple_weather_viz.py ../test-weather-data

# Analyse des données HDFS
python simple_weather_viz.py ../hdfs-exercise8

# Options avancées
python simple_weather_viz.py ../test-weather-data --limit 50 --no-plots
```

**Fonctionnalités :**
- ✅ Lecture récursive des fichiers `alerts.json`
- ✅ Rapport d'analyse textuel détaillé
- ✅ Graphiques matplotlib (distribution, alertes, pays)
- ✅ Statistiques par ville/pays
- ✅ Compatible sans pandas/matplotlib (mode dégradé)

### 2. Script de visualisation avancé (`visualize_weather_logs.py`)

**Analyse complète avec support HDFS**

```bash
# Analyse locale
python visualize_weather_logs.py --source ../test-weather-data

# Analyse HDFS
python visualize_weather_logs.py --hdfs-url http://localhost:9870 --hdfs-path /hdfs-data

# Options de filtrage
python visualize_weather_logs.py --source ../test-weather-data --country FR --city Paris
```

**Fonctionnalités :**
- ✅ Support HDFS natif via WebHDFS
- ✅ Graphiques temporels sophistiqués
- ✅ Filtrage par pays/ville/dates
- ✅ Export multi-formats (PNG, PDF, SVG)
- ✅ Rapport de synthèse automatique

### 3. Dashboard interactif Streamlit (`dashboard_weather.py`)

**Interface web interactive**

```bash
# Lancement automatique
run-dashboard.bat

# Lancement manuel
streamlit run dashboard_weather.py
```

**Fonctionnalités :**
- ✅ Interface web moderne sur http://localhost:8501
- ✅ Graphiques interactifs Plotly
- ✅ Filtrage en temps réel
- ✅ Export des données CSV
- ✅ Statistiques dynamiques

## 📁 Structure des fichiers

```
kafka-ipssi/
├── visualize_weather_logs.py    # Script complet (HDFS + local)
├── simple_weather_viz.py        # Script simple (local uniquement)
├── dashboard_weather.py         # Dashboard Streamlit
├── generate_test_data.py        # Générateur de données de test
├── run-dashboard.bat            # Lanceur Windows
└── visualizations/              # Graphiques générés
    ├── temperature_distribution.png
    ├── wind_speed_distribution.png
    ├── alerts_distribution.png
    └── countries_distribution.png
```

## 🔧 Installation des dépendances

```bash
# Dépendances de base
pip install matplotlib seaborn pandas

# Dashboard interactif
pip install streamlit plotly

# Support HDFS (optionnel)
pip install hdfs
```

## 📊 Données de test

Pour générer des données de test riches (8 villes européennes) :

```bash
# Génère 160 enregistrements (20 par ville)
python generate_test_data.py --output-dir ../test-weather-data --records-per-city 20
```

**Villes incluses :** Paris, London, Madrid, Berlin, Rome, Amsterdam, Stockholm, Prague

## 🎨 Visualisations produites

### 1. Évolution de la température
- Graphique temporel par ville
- Moyennes et tendances
- Variations climatiques

### 2. Évolution du vent  
- Vitesse par ville dans le temps
- Seuils d'alerte visuels
- Patterns météorologiques

### 3. Alertes par niveau
- Distribution des alertes vent/chaleur
- Codage couleur (vert/orange/rouge)
- Compteurs par niveau

### 4. Codes météo par pays
- Code le plus fréquent par pays
- Descriptions météo WMO
- Répartition géographique

## 📈 Exemple d'analyse

```
📊 RAPPORT D'ANALYSE - DONNÉES MÉTÉOROLOGIQUES
════════════════════════════════════════════════════════════
📅 Généré le : 2025-09-23 17:20:50
📈 Nombre total d'enregistrements : 160

🌍 RÉPARTITION PAR PAYS :
   FR: 20 enregistrements
   GB: 20 enregistrements  
   ES: 20 enregistrements
   DE: 20 enregistrements
   ...

🌡️ STATISTIQUES DE TEMPÉRATURE (°C) :
   Minimum: 9.1
   Maximum: 26.6
   Moyenne: 17.4

🌪️ ALERTES VENT :
   level_0: 33    (🟢 Normal)
   level_1: 59    (🟡 Modéré)  
   level_2: 68    (🔴 Élevé)

☁️ CODES MÉTÉO LES PLUS FRÉQUENTS :
   Partly cloudy (2): 42
   Overcast (3): 32
   Mainly clear (1): 28
```

## 🌐 Dashboard web

Le dashboard Streamlit offre :

- **Filtrage interactif** par pays, ville, dates
- **Graphiques Plotly** zoomables et exportables
- **Métriques en temps réel** avec indicateurs visuels
- **Export CSV** des données filtrées
- **Mise à jour automatique** lors des changements de filtres

## 🔗 Intégration avec les exercices précédents

L'Exercice 8 consomme les données produites par :
- **Exercice 3** : Producer météo (données source)
- **Exercice 7** : Consumer HDFS (stockage structuré)  
- **Exercice 4-5** : Spark alerts/aggregates (enrichissement optionnel)

## 🎯 Points clés réalisés

✅ **Évolution de la température au fil du temps** - Graphiques temporels multi-villes  
✅ **Évolution de la vitesse du vent** - Suivi des patterns météo  
✅ **Nombre d'alertes vent et chaleur par niveau** - Distribution des risques  
✅ **Code météo le plus fréquent par pays** - Analyse géographique  

## 🚀 Utilisation rapide

```bash
# 1. Générer des données
python generate_test_data.py

# 2. Analyse simple
python simple_weather_viz.py ../test-weather-data

# 3. Dashboard interactif
run-dashboard.bat
```

**🎉 L'Exercice 8 est maintenant complet avec des visualisations riches et interactives !**