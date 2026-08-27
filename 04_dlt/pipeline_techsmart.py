# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

VOLUME = "/Volumes/techsmart/landing/arquivos"

# COMMAND ----------

# ---------------------------------------------------------------- BRONZE
@dlt.table(
    name="bronze_vendas",
    comment="Vendas cruas do ERP, fiel a origem, com metadados de ingestao",
    table_properties={"quality": "bronze"},
)
def bronze_vendas():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{VOLUME}/vendas")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
# ---------------------------------------------------------------- SILVER
@dlt.table(
    name="silver_vendas",
    comment="Vendas tipadas, padronizadas e validadas",
    table_properties={"quality": "silver"},
)
# Linha reprovada e DESCARTADA e contabilizada nas metricas
@dlt.expect_or_drop("valor_positivo", "valor > 0")
@dlt.expect_or_drop("cliente_informado", "cliente_id IS NOT NULL")
# Linha reprovada e MANTIDA, so registra o aviso
@dlt.expect("quantidade_razoavel", "quantidade BETWEEN 1 AND 100")
def silver_vendas():
    return (dlt.read_stream("bronze_vendas")
            .withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))
            .withColumn("cliente_id", F.col("cliente_id").cast("int"))
            .withColumn("produto_id", F.col("produto_id").cast("int"))
            .withColumn("quantidade", F.col("quantidade").cast("int"))
            .withColumn("valor", F.col("valor").cast("decimal(12,2)"))
            .withColumn("regiao", F.initcap(F.trim(F.lower("regiao"))))
            )
# ---------------------------------------------------------------- GOLD
@dlt.table(
    name="gold_kpi_diario",
    comment="Receita, pedidos e ticket medio por dia e regiao",
    table_properties={"quality": "gold"},
)
def gold_kpi_diario():
    return (dlt.read("silver_vendas")
        .groupBy("data_venda", "regiao")
        .agg(
        F.countDistinct("pedido_id").alias("pedidos"),
        F.countDistinct("cliente_id").alias("clientes"),
        F.sum("valor").alias("receita"),
        F.round(F.avg("valor"), 2).alias("ticket_medio"),
        )
    )