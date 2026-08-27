# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

# COMMAND ----------

ORIGEM = "/Volumes/techsmart/landing/arquivos/clima"
CHECKPT = "/Volumes/techsmart/landing/arquivos/_checkpoints/bronze_clima"
SCHEMA = "/Volumes/techsmart/landing/arquivos/_schemas/bronze_clima"
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(ORIGEM)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPT)
    .trigger(availableNow=True)
    .toTable("techsmart.bronze.clima")
)