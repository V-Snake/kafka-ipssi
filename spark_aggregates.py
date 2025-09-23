#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    p = argparse.ArgumentParser("Weather aggregates (fenêtres)")
    p.add_argument("--in-topic", required=True, help="Topic d'entrée (ex: weather_transformed)")
    p.add_argument("--out-topic", required=True, help="Topic de sortie (ex: weather_aggregates)")
    p.add_argument("--bootstrap-server", default="localhost:9092")
    p.add_argument("--checkpoint", default="./.chk/agg")
    p.add_argument("--window", default="5 minutes", help='Taille de fenêtre (ex: "5 minutes")')
    p.add_argument("--slide", default=None, help='Pas de fenêtre (ex: "5 minutes"). Défaut = même que --window')
    p.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    p.add_argument("--console", action="store_true", help="Afficher aussi les agrégats dans la console")
    return p.parse_args()

def main():
    args = parse_args()
    slide = args.slide or args.window

    # SparkSession
    spark = (
        SparkSession.builder
        .appName("weather_aggregates")
        .config("spark.hadoop.io.native.lib.available", "false")
        # Rendre le parsing tolérant par défaut (évite les exceptions sur dates invalides)
        .config("spark.sql.ansi.enabled", "false")
        # Optionnel: fixe explicitement le fuseau si tu veux un rendu stable
        # .config("spark.sql.session.timeZone", "Europe/Paris")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Lire depuis Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.in_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Schéma JSON des messages de weather_transformed
    schema = T.StructType([
        T.StructField("city", T.StringType()),
        T.StructField("city_name", T.StringType()),
        T.StructField("country_code", T.StringType()),
        T.StructField("admin1", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("temperature", T.DoubleType()),
        T.StructField("windspeed_ms", T.DoubleType()),
        T.StructField("winddirection", T.DoubleType()),
        T.StructField("weathercode", T.IntegerType()),
        T.StructField("wind_alert_level", T.StringType()),
        T.StructField("heat_alert_level", T.StringType()),
        T.StructField("ts_sent", T.StringType()),
        T.StructField("severity", T.StringType()),
    ])

    events = (
        raw.select(F.col("value").cast("string").alias("json"))
           .select(F.from_json("json", schema).alias("d"))
           .select("d.*")
    )

    # 2b) Parsing robuste d'un ISO 8601 sans pattern explicite (évite le piège du 'T')
    events = (
        events
        .withColumn("event_ts", F.to_timestamp("event_time"))  # Laisse Spark parser l'ISO
        .where(F.col("event_ts").isNotNull())                  # écarte les lignes non parsées
        .withWatermark("event_ts", "10 minutes")
    )

    # 3) Fenêtrage + agrégats (inclut les compteurs d'alertes)
    grouped = (
        events.groupBy(
            "country_code",
            "admin1",
            "city",
            F.window(F.col("event_ts"), args.window, slide)
        )
        .agg(
            F.avg("temperature").alias("avg_temp"),
            F.min("temperature").alias("min_temp"),
            F.max("temperature").alias("max_temp"),
            F.avg("windspeed_ms").alias("avg_windspeed_ms"),
            F.max("windspeed_ms").alias("max_windspeed_ms"),
            F.count(F.lit(1)).alias("records"),
            F.sum(F.when(F.col("wind_alert_level") == "level_1", 1).otherwise(0)).alias("wind_l1"),
            F.sum(F.when(F.col("wind_alert_level") == "level_2", 1).otherwise(0)).alias("wind_l2"),
            F.sum(F.when(F.col("heat_alert_level") == "level_1", 1).otherwise(0)).alias("heat_l1"),
            F.sum(F.when(F.col("heat_alert_level") == "level_2", 1).otherwise(0)).alias("heat_l2"),
        )
    )

    # 4) Mise en forme JSON pour Kafka
    # NB: on sort "window_start/end" au format 'yyyy-MM-dd HH:mm:ss' (sans 'T' pour éviter tout souci de pattern)
    out_df = (
        grouped.select(
            F.col("country_code"),
            F.col("admin1"),
            F.col("city"),
            F.date_format(F.col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            F.date_format(F.col("window.end"),   "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            F.round(F.col("avg_temp"), 2).alias("avg_temperature"),
            F.round(F.col("min_temp"), 2).alias("min_temperature"),
            F.round(F.col("max_temp"), 2).alias("max_temperature"),
            F.round(F.col("avg_windspeed_ms"), 2).alias("avg_windspeed_ms"),
            F.round(F.col("max_windspeed_ms"), 2).alias("max_windspeed_ms"),
            F.col("records").cast("long").alias("records"),
            F.col("wind_l1").cast("long").alias("wind_level_1"),
            F.col("wind_l2").cast("long").alias("wind_level_2"),
            F.col("heat_l1").cast("long").alias("heat_level_1"),
            F.col("heat_l2").cast("long").alias("heat_level_2"),
        )
    )

    json_for_kafka = (
        out_df
        .select(F.to_json(F.struct([F.col(c) for c in out_df.columns])).alias("value"))
        .selectExpr("CAST(value AS STRING) AS value")
    )

    # 5) Sink Kafka — IMPORTANT: outputMode append (Kafka ne supporte pas update)
    kafka_query = (
        json_for_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("topic", args.out_topic)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
        .start()
    )

    # (Optionnel) Affichage console pour debug (update est ok pour console)
    if args.console:
        (
            out_df.writeStream
            .format("console")
            .option("truncate", False)
            .outputMode("update")
            .start()
        )

    kafka_query.awaitTermination()

if __name__ == "__main__":
    main()
