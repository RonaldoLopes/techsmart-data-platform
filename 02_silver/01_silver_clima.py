# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ============================================================================
# NOTEBOOK: 01_silver_clima.py
# Módulo 10 — Transformar bronze.clima em silver.clima
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Ler dados brutos do clima
df_bronze = spark.table("techsmart.bronze.clima")

# Tipagem e validação
df_silver = (df_bronze
  .withColumn("data", F.col("data").cast("date"))
  .withColumn("temp_media", F.col("temp_media").cast("double"))
  .withColumn("chuva_mm", F.col("chuva_mm").cast("double"))
  .withColumn("umidade_pct", F.col("umidade_pct").cast("int"))
  .withColumn("regiao", F.initcap(F.trim(F.lower(F.col("regiao")))))
  
  # Deduplicação: última entrada por data + regiao
  .withColumn("rn", F.row_number().over(
    Window.partitionBy("data", "regiao").orderBy(F.desc("_ingest_ts"))
  ))
  .filter("rn = 1")
  .drop("rn")
  
  # Validação
  .withColumn("_valido", 
    (F.col("temp_media").isNotNull()) & 
    (F.col("chuva_mm") >= 0) & 
    (F.col("umidade_pct").between(0, 100)) &
    (F.col("regiao").isin("Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"))
  )
)

# Aprovados → silver.clima
(df_silver
  .filter("_valido = true")
  .select("data", "regiao", "temp_media", "chuva_mm", "umidade_pct", "_ingest_ts")
  .write.mode("overwrite")
  .saveAsTable("techsmart.silver.clima")
)

# Reprovados → silver.quarentena (mesma tabela que vendas)
(df_silver
  .filter("_valido = false")
  .select("data", "regiao", "temp_media", "chuva_mm", "umidade_pct", "_ingest_ts")
  .withColumn("_motivo_reprovacao", F.lit("clima_invalido"))
  .withColumn("_quarentena_ts", F.current_timestamp())
  .write.mode("append")
  .option("mergeSchema", "true")
  .saveAsTable("techsmart.silver.quarentena")
)

# Resultado
print(f"✓ Silver clima criado com sucesso")
display(spark.sql("SELECT COUNT(*) as total FROM techsmart.silver.clima"))