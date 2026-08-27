# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.sql("""
    SELECT k.data,
    k.regiao,
    k.receita_dia,
    k.total_pedidos,
    k.ticket_medio,
    t.num_dia_semana,
    t.fim_de_semana,
    t.mes
    FROM techsmart.gold.kpi_diario k
    JOIN techsmart.gold.dim_tempo t ON k.data = t.data
""")

# COMMAND ----------

janela = Window.partitionBy("regiao").orderBy("data")

features = (df
    .withColumn("receita_d1", F.lag("receita_dia", 1).over(janela))
    .withColumn("receita_d7", F.lag("receita_dia", 7).over(janela))
    .withColumn("media_movel_7",
        F.avg("receita_dia").over(janela.rowsBetween(-7, -1)))
    .dropna()
)

(features.write.mode("overwrite")
    .saveAsTable("techsmart.gold.features_demanda")
)