# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver = spark.read.table("techsmart.silver.vendas_enriquecidas")

# COMMAND ----------

fato = (
    silver.select(
        "pedido_id",
        F.col("data").alias("data"),
        "cliente_id",
        "produto_id",
        "regiao",
        "quantidade",
        "valor",
        "temp_media",
        "chuva_mm"
    )
    .withColumn(
        "valor_unitario",
        F.round(F.col("valor") / F.col("quantidade"), 2)
    )
)

fato.write \
    .mode("overwrite") \
    .clusterBy("data", "regiao") \
    .saveAsTable("techsmart.gold.fato_vendas")