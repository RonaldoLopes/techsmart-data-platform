# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver = spark.read.table("techsmart.silver.vendas_enriquecidas")

# COMMAND ----------

# --- dim_produto
(silver.select("produto_id", "produto")
    .dropDuplicates(["produto_id"])
    .withColumn("categoria",
    F.when(F.col("produto").rlike("(?i)fone|headset"), "Audio")
    .when(F.col("produto").rlike("(?i)mouse|teclado"), "Perifericos")
    .when(F.col("produto").rlike("(?i)cabo|hub|carregador"), "Acessorios")
    .otherwise("Outros"))
    .withColumnRenamed("produto", "produto_nome")
    .write.mode("overwrite")
    .saveAsTable("techsmart.gold.dim_produto")
)

# COMMAND ----------

# --- dim_tempo
(silver.select(F.col("data").alias("data"))
    .distinct()
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("dia_semana", F.date_format("data", "EEEE"))
    .withColumn("num_dia_semana", F.dayofweek("data"))
    .withColumn("fim_de_semana", F.dayofweek("data").isin(1, 7))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .write.mode("overwrite")
    .saveAsTable("techsmart.gold.dim_tempo")
)

# COMMAND ----------

# --- dim_regiao
(silver.select("regiao")
    .distinct()
    .withColumn("macro_regiao",
    F.when(F.col("regiao").isin("Sudeste", "Sul"), "Sul-Sudeste")
    .otherwise("Norte-Nordeste-Centro"))
    .write.mode("overwrite")
    .saveAsTable("techsmart.gold.dim_regiao")
)