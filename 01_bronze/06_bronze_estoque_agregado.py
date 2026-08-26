# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Ler stream do bronze
df = (spark.readStream
  .table("techsmart.bronze.estoque")
)

# Watermark: aceita dados até 10 minutos atrasados
agregado = (df
  .withWatermark("_event_ts", "10 minutes")
  .groupBy(
    F.window(F.col("_event_ts"), "5 minutes").alias("janela"),
    F.col("produto_id"),
    F.col("regiao")
  )
  .agg(
    F.sum(F.when(F.col("tipo") == "entrada", F.col("quantidade")).otherwise(0)).alias("entrada"),
    F.sum(F.when(F.col("tipo") == "saida", F.col("quantidade")).otherwise(0)).alias("saida"),
    F.count("*").alias("eventos")
  )
  .select(
    F.col("janela.start").alias("janela_inicio"),
    F.col("janela.end").alias("janela_fim"),
    "produto_id",
    "regiao",
    "entrada",
    "saida",
    "eventos"
  )
)

# Escrever resultado
query = (agregado.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/Volumes/techsmart/landing/_checkpoints/bronze_estoque_agregado")
  .trigger(availableNow=True)
  .toTable("techsmart.bronze.estoque_por_janela")
)

query.awaitTermination()
print("✓ Agregação concluída")

# Confira resultado
spark.sql("""
  SELECT janela_inicio, janela_fim, produto_id, regiao, entrada, saida
  FROM techsmart.bronze.estoque_por_janela
  ORDER BY janela_inicio DESC
  LIMIT 20
""").display()