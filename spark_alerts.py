#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    p = argparse.ArgumentParser("Ex04/06 - Transform & Alerts (avec région)")
    p.add_argument("--bootstrap", dest="bootstrap", default="127.0.0.1:9092")
    p.add_argument("--topic", dest="topic", default="weather_stream")
    p.add_argument("--starting-offsets", dest="starting_offsets",
                   choices=["earliest", "latest"], default="latest")
    p.add_argument("--checkpoint", dest="checkpoint", default="./chk/ex04")
    p.add_argument("--wind-max", dest="wind_max", type=float, default=60.0)  # km/h
    p.add_argument("--temp-min", dest="temp_min", type=float, default=0.0)   # °C
    return p.parse_args()

def main():
    a = parse_args()

    spark = (
        SparkSession.builder
        .appName("ex04-spark-transform-alerts")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Kafka source
    src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap)
        .option("subscribe", a.topic)
        .option("startingOffsets", a.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Schéma du producer (Exo6)
    schema = T.StructType([
        T.StructField("city_name", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("country_code", T.StringType()),
        T.StructField("admin1", T.StringType()),
        T.StructField("admin2", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("current", T.StructType([
            T.StructField("temperature", T.DoubleType()),
            T.StructField("windspeed", T.DoubleType()),      # km/h
            T.StructField("winddirection", T.DoubleType()),
            T.StructField("weathercode", T.IntegerType()),
            T.StructField("time", T.StringType()),
            T.StructField("elevation", T.DoubleType()),
            T.StructField("timezone", T.StringType()),
            T.StructField("units", T.MapType(T.StringType(), T.StringType())),
            T.StructField("source", T.StringType()),
        ])),
        T.StructField("ts_sent", T.StringType()),
        T.StructField("version", T.IntegerType()),
    ])

    raw = src.select(F.col("key").cast("string").alias("k"),
                     F.col("value").cast("string").alias("json"))

    d = raw.select(F.from_json("json", schema).alias("d")).select("d.*")

    # 3) Flatten + enrichissements (severities / levels)
    ev = (
        d.select(
            "city", "city_name", "country_code", "admin1", "lat", "lon",
            F.col("current.temperature").alias("temperature"),
            F.col("current.windspeed").alias("windspeed_kmh"),
            F.col("current.winddirection").alias("winddirection"),
            F.col("current.weathercode").alias("weathercode"),
            F.col("current.time").alias("event_time"),
            "ts_sent"
        )
        .withColumn("windspeed_ms", F.col("windspeed_kmh")/F.lit(3.6))
    )

    # Règles d’alertes (exemples)
    wind_l2 = (F.col("windspeed_kmh") >= F.lit(a.wind_max))
    wind_l1 = (F.col("windspeed_kmh") >= F.lit(a.wind_max*0.75)) & ~wind_l2
    heat_l2 = (F.col("temperature") >= F.lit(35.0))
    heat_l1 = (F.col("temperature") >= F.lit(30.0)) & ~heat_l2
    cold_crit = (F.col("temperature") <= F.lit(a.temp_min))

    # WMO sévères
    severe_wmo = F.array([F.lit(x) for x in [61,62,63,80,81,82,95,96,99]])
    has_severe_code = F.array_contains(severe_wmo, F.col("weathercode"))

    enriched = (
        ev.withColumn(
            "wind_alert_level",
            F.when(wind_l2, F.lit("level_2"))
             .when(wind_l1, F.lit("level_1"))
             .otherwise(F.lit(None).cast("string"))
        ).withColumn(
            "heat_alert_level",
            F.when(heat_l2, F.lit("level_2"))
             .when(heat_l1, F.lit("level_1"))
             .otherwise(F.lit(None).cast("string"))
        ).withColumn(
            "severity",
            F.when(wind_l2 | heat_l2 | cold_crit | has_severe_code, F.lit("CRITICAL"))
             .otherwise(F.lit("OK"))
        )
    )

    # 4) Colonnes de sortie vers Kafka (topic weather_transformed)
    out = enriched.select(
        "city", "city_name", "country_code", "admin1",
        "lat", "lon",
        "event_time",
        F.col("temperature").alias("temperature"),
        F.col("windspeed_ms").alias("windspeed_ms"),
        "winddirection", "weathercode",
        "wind_alert_level", "heat_alert_level",
        "ts_sent", "severity"
    )

    # Console lisible
    (
        out.select("city", "country_code", "admin1", "temperature", "windspeed_ms",
                   "winddirection", "weathercode", "event_time", "severity")
        .writeStream.format("console").outputMode("append")
        .option("truncate", False).start()
    )

    # JSON -> Kafka
    kafka_df = out.select(
        F.to_json(F.struct([F.col(c) for c in out.columns])).alias("value")
    )

    (
        kafka_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap)
        .option("topic", "weather_transformed")
        .option("checkpointLocation", a.checkpoint)
        .outputMode("append")
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    main()
