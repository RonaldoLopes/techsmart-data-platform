# Databricks notebook source
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

bronze = spark.read.table("techsmart.bronze.vendas")

# COMMAND ----------

tipado = (bronze
    .withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))
    .withColumn("cliente_id", F.col("cliente_id").cast("int"))
    .withColumn("produto_id", F.col("produto_id").cast("int"))
    .withColumn("quantidade", F.col("quantidade").cast("int"))
    .withColumn("valor", F.col("valor").cast("decimal(12,2)"))
)
padronizado = (tipado
    # Regiao: "SUDESTE", "sudeste" e "Sudeste" viram a mesma coisa
    .withColumn("regiao", F.initcap(F.trim(F.lower(F.col("regiao")))))
    # Texto sem espaco sobrando
    .withColumn("produto", F.trim("produto"))
)

# COMMAND ----------

# DBTITLE 1,Cell 4

janela = Window.partitionBy("pedido_id").orderBy(F.col("_ingest_ts").desc())
deduplicado = (padronizado
    .withColumn("_rn", F.row_number().over(janela))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

df_bronze = spark.table("techsmart.bronze.vendas")

# Cast dos tipos corretos
df_tipado = (df_bronze
  .withColumn("valor", F.col("valor").cast("double"))
  .withColumn("quantidade", F.col("quantidade").cast("int"))
  .withColumn("cliente_id", F.col("cliente_id").cast("int"))
  .withColumn("data_venda", F.col("data_venda").cast("date"))
)

# Regras com tipos já corretos
regras_ok = {
    "valor_positivo": F.col("valor") > 0,
    "quantidade_valida": (F.col("quantidade") > 0) & (F.col("quantidade") <= 100),
    "cliente_informado": F.col("cliente_id").isNotNull(),
    "data_nao_futura": F.col("data_venda") <= F.current_date(),
    "regiao_conhecida": F.col("regiao").isin("Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"),
}

todas_ok = reduce(lambda a, b: a & b, regras_ok.values())

motivo_expr = F.concat_ws("; ",
    F.when(F.col("valor") <= 0, F.lit("valor_positivo")),
    F.when((F.col("quantidade") <= 0) | (F.col("quantidade") > 100), F.lit("quantidade_valida")),
    F.when(F.col("cliente_id").isNull(), F.lit("cliente_informado")),
    F.when(F.col("data_venda") > F.current_date(), F.lit("data_nao_futura")),
    F.when(~F.col("regiao").isin("Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"), F.lit("regiao_conhecida")),
)

colunas_finais = [
    "pedido_id", "data_venda", "cliente_id", "produto_id", "produto",
    "quantidade", "valor", "regiao", "_ingest_ts", "_source_file"
]

# Aprovados
(df_tipado
  .filter(todas_ok)
  .select(*colunas_finais)
  .coalesce(1)
  .write.mode("overwrite")
  .saveAsTable("techsmart.silver.vendas")
)

# Reprovados
(df_tipado
  .filter(~todas_ok)
  .select(*colunas_finais)
  .withColumn("_motivo_reprovacao", motivo_expr)
  .withColumn("_quarentena_ts", F.current_timestamp())
  .coalesce(1)
  .write.mode("append")
  .saveAsTable("techsmart.silver.vendas_quarentena")
)

print("✓ Concluído")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _motivo_reprovacao, COUNT(*) AS linhas
# MAGIC FROM techsmart.silver.vendas_quarentena
# MAGIC GROUP BY _motivo_reprovacao
# MAGIC ORDER BY linhas DESC