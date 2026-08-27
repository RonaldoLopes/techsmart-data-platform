# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

# COMMAND ----------

ORIGEM = "/Volumes/techsmart/landing/arquivos/estoque"
CHECKPT = "/Volumes/techsmart/landing/arquivos/_checkpoints/bronze_estoque"
SCHEMA = "/Volumes/techsmart/landing/arquivos/_schemas/bronze_estoque"

# COMMAND ----------

stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(ORIGEM)
    .withColumn("evento_ts", F.to_timestamp("timestamp"))
    .withColumn("_ingest_ts", F.current_timestamp())
)
(stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPT)
    .trigger(availableNow=True)
    .toTable("techsmart.bronze.estoque_eventos")
)