# Databricks notebook source
from pyspark.sql import functions as F
import json

ORIGEM = "/Volumes/techsmart/landing/arquivos/estoque"
CHECKPT = "/Volumes/techsmart/landing/arquivos/_checkpoints/bronze_estoque"
DESTINO = "techsmart.bronze.estoque"

# Ler arquivos JSON Lines como stream
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", CHECKPT)
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # ← adicione isso
  .load(ORIGEM)
  .select(
    "*",
    F.col("timestamp").cast("timestamp").alias("_event_ts"),
    F.current_timestamp().alias("_ingest_ts"),
    F.col("_metadata.file_path").alias("_source_file")
  )
)

# Escrever com trigger availableNow
query = (df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", CHECKPT)
  .option("mergeSchema", "true")  # ← adicione isso também
  .trigger(availableNow=True)
  .toTable(DESTINO)
)

query.awaitTermination()
print(f"✓ Ingestão concluída")

# Confira
spark.sql(f"SELECT COUNT(*) FROM {DESTINO}").show()