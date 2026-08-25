# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

ORIGEM = "/Volumes/techsmart/landing/arquivos/vendas"
CHECKPT = "/Volumes/techsmart/landing/arquivos/_checkpoints/bronze_vendas"
SCHEMA = "/Volumes/techsmart/landing/arquivos/_schemas/bronze_vendas"
DESTINO = "techsmart.bronze.vendas"

# COMMAND ----------

df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", SCHEMA)
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
  .option("header", "true")
  .load(ORIGEM)
  .withColumn("_ingest_ts", F.current_timestamp())
  .withColumn("_source_file", F.col("_metadata.file_path"))
  .withColumn("_file_size", F.col("_metadata.file_size"))
)
(df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(DESTINO)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS linhas,
# MAGIC     COUNT(DISTINCT _source_file) AS arquivos,
# MAGIC     MIN(_ingest_ts) AS primeira_ingestao,
# MAGIC     MAX(_ingest_ts) AS ultima_ingestao
# MAGIC     FROM techsmart.bronze.vendas;