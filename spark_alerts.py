#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex04 — Spark Structured Streaming
- Lit depuis Kafka (topic source: --topic, défaut: weather_stream)
- Parse le JSON du producer météo
- (A) Affiche les alertes en console selon des SEUILS *dynamiques* passés en args:
      --wind-max (km/h) et --temp-min (°C) => colonne 'severity' ∈ {WIND, COLD, CRITICAL}
- (B) Écrit *toutes* les mesures transformées dans un nouveau topic Kafka 'weather_transformed'
      avec:
        - event_time (timestamp)
        - temperature (°C)
        - windspeed_ms (m/s, conversion km/h -> m/s)
        - wind_alert_level ∈ {level_0, level_1, level_2} selon m/s ( <10, 10–20, ≥20 )
        - heat_alert_level ∈ {level_0, level_1, level_2} selon °C  ( <25, 25–35, ≥35 )

Lancement recommandé (Windows PowerShell) :
  .\scripts\run-spark-alerts.ps1 -Fresh `
    -Bootstrap "127.0.0.1:9092" -Topic "weather_stream" `
    -WindMax 10 -TempMin 20

Le wrapper .ps1 ajoute le package Kafka:
  org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, MapType
)

def build_schema():
    return StructType([
        StructField("city", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("current", StructType([
            StructField("temperature", DoubleType()),
            StructField("windspeed", DoubleType()),     # km/h (Open-Meteo)
            StructField("winddirection", DoubleType()),
            StructField("weathercode", IntegerType()),
            StructField("time", StringType()),
            StructField("elevation", DoubleType()),
            StructField("timezone", StringType()),
            StructField("units", MapType(StringType(), StringType()))
        ])),
        StructField("ts_sent", StringType()),
    ])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="weather_stream")
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--wind-max", type=float, default=60.0, help="Alerte si windspeed (km/h) > wind-max")
    ap.add_argument("--temp-min", type=float, default=0.0,  help="Alerte si temperature (°C) < temp-min")
    ap.add_argument("--checkpoint", default="./chk/ex04", help="Dossier checkpoint Structured Streaming")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("ex04-spark-transform-alerts").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1) Source Kafka
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    # 2) Parse JSON
    schema = build_schema()
    df = (
        df_kafka.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json(F.col("json"), schema).alias("data"))
        .select(
            F.col("data.city").alias("city"),
            F.col("data.lat").alias("lat"),
            F.col("data.lon").alias("lon"),
            F.col("data.current.temperature").alias("temperature"),
            F.col("data.current.windspeed").alias("windspeed"),           # km/h
            F.col("data.current.winddirection").alias("winddirection"),
            F.col("data.current.weathercode").alias("weathercode"),
            F.col("data.current.time").alias("obs_time"),
            F.col("data.ts_sent").alias("ts_sent"),
        )
    )

    # 3A) Alertes dynamiques (pour la console) selon --wind-max / --temp-min
    df_alerts = (
        df.withColumn("alert_wind", F.col("windspeed") > F.lit(args.wind_max))
          .withColumn("alert_cold", F.col("temperature") < F.lit(args.temp_min))
          .withColumn(
              "severity",
              F.when(F.col("alert_wind") & F.col("alert_cold"), F.lit("CRITICAL"))
               .when(F.col("alert_wind"), F.lit("WIND"))
               .when(F.col("alert_cold"), F.lit("COLD"))
               .otherwise(F.lit("OK"))
          )
          .filter(F.col("severity") != F.lit("OK"))
          .select("city", "temperature", "windspeed", "winddirection", "weathercode", "obs_time", "severity")
    )

    # 3B) Transformation générique (pour Kafka 'weather_transformed')
    #     - conversion km/h -> m/s
    #     - niveaux d'alerte *indépendants* des args (seuils fixes pour standardiser les downstream jobs)
    df_out = (
        df
        .withColumn("event_time", F.to_timestamp(F.col("obs_time")))
        .withColumn("windspeed_ms", F.col("windspeed") / F.lit(3.6))  # km/h -> m/s
        .withColumn(
            "wind_alert_level",
            F.when(F.col("windspeed_ms") >= F.lit(20), F.lit("level_2"))
             .when(F.col("windspeed_ms") >= F.lit(10), F.lit("level_1"))
             .otherwise(F.lit("level_0"))
        )
        .withColumn(
            "heat_alert_level",
            F.when(F.col("temperature") >= F.lit(35), F.lit("level_2"))
             .when(F.col("temperature") >= F.lit(25), F.lit("level_1"))
             .otherwise(F.lit("level_0"))
        )
        .select(
            "city", "lat", "lon", "event_time",
            F.round(F.col("temperature"), 2).alias("temperature"),
            F.round(F.col("windspeed_ms"), 2).alias("windspeed_ms"),
            "wind_alert_level", "heat_alert_level",
            "weathercode", "ts_sent"
        )
    )

    # 4) Sinks
    # 4.1) Console (alertes dynamiques)
    q_console = (
        df_alerts.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint + "/console")
        .start()
    )

    # 4.2) Kafka (flux transformé normalisé)
    out_json = df_out.select(F.to_json(F.struct(*df_out.columns)).alias("value"))
    q_kafka = (
        out_json.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", "weather_transformed")
        .option("checkpointLocation", args.checkpoint + "/kafka")
        .outputMode("append")
        .start()
    )

    # 5) Attente
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
