#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard interactif simple pour la visualisation des données météo

Ce script crée un dashboard web simple avec Streamlit pour explorer
les données météo de manière interactive.

Usage:
    pip install streamlit
    streamlit run dashboard_weather.py
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Météo - Kafka IPSSI",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Codes météo WMO
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail"
}

@st.cache_data
def load_weather_data(data_path: str) -> pd.DataFrame:
    """Charge les données météo depuis le répertoire spécifié"""
    records = []
    data_path = Path(data_path)
    
    if not data_path.exists():
        st.error(f"Le répertoire {data_path} n'existe pas")
        return pd.DataFrame()
    
    # Parcourt récursivement tous les fichiers alerts.json
    for alerts_file in data_path.rglob("alerts.json"):
        try:
            with open(alerts_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            record['_file_path'] = str(alerts_file)
                            records.append(record)
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            st.warning(f"Erreur lors de la lecture de {alerts_file}: {e}")
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    
    # Nettoyage des données
    if 'event_time' in df.columns:
        df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
    if 'ts_sent' in df.columns:
        df['ts_sent'] = pd.to_datetime(df['ts_sent'], errors='coerce')
    
    # Conversion des colonnes numériques
    numeric_cols = ['temperature', 'windspeed_ms', 'winddirection', 'weathercode', 'lat', 'lon']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Extraction du pays depuis le chemin si pas disponible
    if 'country_code' not in df.columns or df['country_code'].isna().all():
        df['country_code'] = df['_file_path'].str.extract(r'/([A-Z]{2})/')
    
    # Ajoute les descriptions météo
    if 'weathercode' in df.columns:
        df['weather_description'] = df['weathercode'].map(WMO_CODES).fillna('Unknown')
    
    return df

def plot_temperature_evolution(df: pd.DataFrame):
    """Graphique d'évolution de la température"""
    if df.empty or 'temperature' not in df.columns:
        st.warning("Pas de données de température disponibles")
        return
    
    fig = px.line(
        df, 
        x='event_time', 
        y='temperature',
        color='city' if 'city' in df.columns else None,
        title="Évolution de la température au fil du temps",
        labels={'temperature': 'Température (°C)', 'event_time': 'Temps'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def plot_wind_speed_evolution(df: pd.DataFrame):
    """Graphique d'évolution de la vitesse du vent"""
    if df.empty or 'windspeed_ms' not in df.columns:
        st.warning("Pas de données de vitesse du vent disponibles")
        return
    
    fig = px.line(
        df,
        x='event_time',
        y='windspeed_ms',
        color='city' if 'city' in df.columns else None,
        title="Évolution de la vitesse du vent au fil du temps",
        labels={'windspeed_ms': 'Vitesse du vent (m/s)', 'event_time': 'Temps'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def plot_alert_levels(df: pd.DataFrame):
    """Graphique des niveaux d'alerte"""
    alert_cols = ['wind_alert_level', 'heat_alert_level']
    available_cols = [col for col in alert_cols if col in df.columns]
    
    if not available_cols:
        st.warning("Pas de données d'alertes disponibles")
        return
    
    col1, col2 = st.columns(len(available_cols))
    cols = [col1, col2] if len(available_cols) == 2 else [col1]
    
    for i, col in enumerate(available_cols):
        alert_counts = df[col].value_counts().dropna()
        
        if not alert_counts.empty:
            fig = px.bar(
                x=alert_counts.index,
                y=alert_counts.values,
                title=f"Alertes {col.replace('_', ' ').title()}",
                labels={'x': 'Niveau d\'alerte', 'y': 'Nombre d\'occurrences'},
                color=alert_counts.index,
                color_discrete_map={
                    'level_0': 'green',
                    'level_1': 'orange', 
                    'level_2': 'red'
                }
            )
            fig.update_layout(height=400)
            cols[i].plotly_chart(fig, use_container_width=True)

def plot_weather_codes_by_country(df: pd.DataFrame):
    """Graphique des codes météo par pays"""
    if df.empty or 'weathercode' not in df.columns:
        st.warning("Pas de données de codes météo disponibles")
        return
    
    country_col = 'country_code' if 'country_code' in df.columns else 'country'
    if country_col not in df.columns:
        st.warning("Pas de données de pays disponibles")
        return
    
    # Pour chaque pays, trouve le code météo le plus fréquent
    country_weather = df.groupby([country_col, 'weathercode', 'weather_description']).size().reset_index(name='count')
    most_frequent = country_weather.loc[country_weather.groupby(country_col)['count'].idxmax()]
    
    if not most_frequent.empty:
        fig = px.bar(
            most_frequent,
            x=country_col,
            y='count',
            color='weather_description',
            title="Code météo le plus fréquent par pays",
            labels={'count': 'Nombre d\'occurrences', country_col: 'Pays'},
            hover_data=['weathercode']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def show_data_summary(df: pd.DataFrame):
    """Affiche un résumé des données"""
    if df.empty:
        st.warning("Aucune donnée à afficher")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", len(df))
    
    with col2:
        if 'country_code' in df.columns:
            st.metric("Pays", df['country_code'].nunique())
    
    with col3:
        if 'city' in df.columns:
            st.metric("Villes", df['city'].nunique())
    
    with col4:
        if 'event_time' in df.columns:
            time_span = df['event_time'].max() - df['event_time'].min()
            st.metric("Période", f"{time_span.days}j {time_span.seconds//3600}h")
    
    # Statistiques détaillées
    with st.expander("Statistiques détaillées"):
        if 'temperature' in df.columns:
            st.subheader("Température (°C)")
            temp_stats = df['temperature'].describe()
            st.dataframe(temp_stats.to_frame().T)
        
        if 'windspeed_ms' in df.columns:
            st.subheader("Vitesse du vent (m/s)")
            wind_stats = df['windspeed_ms'].describe()
            st.dataframe(wind_stats.to_frame().T)

def main():
    """Application principale Streamlit"""
    st.title("🌤️ Dashboard Météo - Kafka IPSSI")
    st.markdown("**Exercice 8 : Visualisation et agrégation des logs météo**")
    
    # Sidebar pour la configuration
    st.sidebar.header("Configuration")
    
    # Sélection du répertoire de données
    data_path = st.sidebar.text_input(
        "Répertoire des données",
        value="./hdfs-final-test",
        help="Chemin vers le répertoire contenant les fichiers alerts.json"
    )
    
    if not data_path:
        st.warning("Veuillez spécifier un répertoire de données")
        return
    
    # Chargement des données
    with st.spinner("Chargement des données..."):
        df = load_weather_data(data_path)
    
    if df.empty:
        st.error("Aucune donnée trouvée dans le répertoire spécifié")
        return
    
    # Filtres dans la sidebar
    st.sidebar.header("Filtres")
    
    # Filtre par pays
    if 'country_code' in df.columns:
        countries = ['Tous'] + list(df['country_code'].dropna().unique())
        selected_country = st.sidebar.selectbox("Pays", countries)
        if selected_country != 'Tous':
            df = df[df['country_code'] == selected_country]
    
    # Filtre par ville
    if 'city' in df.columns:
        cities = ['Toutes'] + list(df['city'].dropna().unique())
        selected_city = st.sidebar.selectbox("Ville", cities)
        if selected_city != 'Toutes':
            df = df[df['city'] == selected_city]
    
    # Filtre par plage de dates
    if 'event_time' in df.columns and not df['event_time'].isna().all():
        date_range = st.sidebar.date_input(
            "Plage de dates",
            value=(df['event_time'].min().date(), df['event_time'].max().date()),
            min_value=df['event_time'].min().date(),
            max_value=df['event_time'].max().date()
        )
        if len(date_range) == 2:
            start_date, end_date = date_range
            df = df[(df['event_time'].dt.date >= start_date) & (df['event_time'].dt.date <= end_date)]
    
    # Résumé des données
    st.header("📊 Résumé des données")
    show_data_summary(df)
    
    # Graphiques
    st.header("📈 Visualisations")
    
    # Tabs pour organiser les graphiques
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌡️ Température", 
        "💨 Vitesse du vent", 
        "⚠️ Alertes", 
        "🌍 Codes météo par pays"
    ])
    
    with tab1:
        plot_temperature_evolution(df)
    
    with tab2:
        plot_wind_speed_evolution(df)
    
    with tab3:
        plot_alert_levels(df)
    
    with tab4:
        plot_weather_codes_by_country(df)
    
    # Données brutes
    with st.expander("🔍 Données brutes"):
        st.dataframe(df, use_container_width=True)
    
    # Bouton de téléchargement
    if not df.empty:
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Télécharger les données CSV",
            data=csv,
            file_name='weather_data.csv',
            mime='text/csv'
        )

if __name__ == "__main__":
    main()