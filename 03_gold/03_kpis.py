# Databricks notebook source
from pyspark.sql import functions as F

# Ler o fato de vendas
fato = spark.table("techsmart.gold.fato_vendas")

# KPI diário: visão geral por dia e região
kpi_diario = (fato
  .groupBy("data", "regiao")
  .agg(
    F.count("*").alias("total_pedidos"),
    F.sum("valor").alias("receita_dia"),
    F.avg("valor").alias("ticket_medio"),
    F.min("valor").alias("venda_minima"),
    F.max("valor").alias("venda_maxima")
  )
  .orderBy(F.desc("data"))
)

(kpi_diario
  .write.mode("overwrite")
  .saveAsTable("techsmart.gold.kpi_diario")
)

# KPI por produto: ranking de vendas (sem nome, apenas produto_id)
kpi_produto = (fato
  .groupBy("produto_id")
  .agg(
    F.count("*").alias("total_pedidos"),
    F.sum("quantidade").alias("total_unidades"),
    F.sum("valor").alias("receita_total")
  )
  .orderBy(F.desc("receita_total"))
)

(kpi_produto
  .write.mode("overwrite")
  .saveAsTable("techsmart.gold.kpi_produto")
)

print(f"✓ kpi_diario: {kpi_diario.count():,} linhas")
print(f"✓ kpi_produto: {kpi_produto.count():,} linhas")
