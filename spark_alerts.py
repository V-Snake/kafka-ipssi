#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ex04 — Spark Structured Streaming: parse messages météo et remonter des alertes
- Lit depuis Kafka (topic par défaut: weather_stream)
- Parse le JSON produit par producer_weather.py
- Filtre et affiche les alertes (vent > seuil, température < seuil)

Run (Windows PowerShell) :
  .\.venv\Scripts\spark-submit.cmd --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.1 .\spark_alerts.py ^
    --bootstrap localhost:9092 --topic weather_stream --wind-max 50 --temp-min 0

Run (Git Bash) :
  .venv/Scripts/spark-submit.cmd --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.0.1 spark_alerts.py \
    --bootstrap localhost:9092 --topic weather_stream --wind-max 50 --temp-min 0
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
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
            StructField("windspeed", DoubleType()),
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
    ap.add_argument("--wind-max", type=float, default=60.0, help="Alerte si windspeed > wind-max")
    ap.add_argument("--temp-min", type=float, default=0.0, help="Alerte si temperature < temp-min")
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
    df = df_kafka.selectExpr("CAST(value AS STRING) AS json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select(
            col("data.city").alias("city"),
            col("data.lat").alias("lat"),
            col("data.lon").alias("lon"),
            col("data.current.temperature").alias("temperature"),
            col("data.current.windspeed").alias("windspeed"),
            col("data.current.winddirection").alias("winddirection"),
            col("data.current.weathercode").alias("weathercode"),
            col("data.current.time").alias("obs_time"),
            col("data.ts_sent").alias("ts_sent"),
        )

    # 3) Règles d’alerte
    df_alerts = (
        df.withColumn("alert_wind", col("windspeed") > lit(args.wind_max))
          .withColumn("alert_cold", col("temperature") < lit(args.temp_min))
          .withColumn(
              "severity",
              when(col("alert_wind") & col("alert_cold"), lit("CRITICAL"))
              .when(col("alert_wind"), lit("WIND"))
              .when(col("alert_cold"), lit("COLD"))
              .otherwise(lit("OK"))
          )
          .filter(col("severity") != lit("OK"))
          .select("city", "temperature", "windspeed", "winddirection", "weathercode", "obs_time", "severity")
    )

    # 4) Sink console
    q = (df_alerts.writeStream
         .format("console")
         .outputMode("append")
         .option("truncate", "false")
         .option("checkpointLocation", args.checkpoint)
         .start())

    q.awaitTermination()

if __name__ == "__main__":
    main()
