# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ============================================================================
# NOTEBOOK: 02_silver_vendas_clima.py
# Módulos 10.6 (Integração) + 10.7 (Incremental)
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce
from datetime import datetime

# ============================================================================
# MÓDULO 10.6 — INTEGRAÇÃO: CRUZANDO VENDAS COM CLIMA
# ============================================================================

# Ler as duas tabelas
vendas = spark.table("techsmart.silver.vendas")
clima = spark.table("techsmart.silver.clima")

# Join: vendas com clima pela data e regiao
vendas_clima = (vendas
  .join(
    clima,
    on=(F.col("vendas.data_venda") == F.col("clima.data")) & 
       (F.col("vendas.regiao") == F.col("clima.regiao")),
    how="left"
  )
  .select(
    F.col("vendas.pedido_id"),
    F.col("vendas.data_venda").alias("data"),
    F.col("vendas.cliente_id"),
    F.col("vendas.produto_id"),
    F.col("vendas.produto"),
    F.col("vendas.quantidade"),
    F.col("vendas.valor"),
    F.col("vendas.regiao"),
    F.col("clima.temp_media"),
    F.col("clima.chuva_mm"),
    F.col("clima.umidade_pct"),
    F.col("vendas._ingest_ts")
  )
)

# Validação: clima não pode ser NULL
com_flag = (vendas_clima
  .withColumn("_clima_encontrado", F.col("temp_media").isNotNull())
)

# Separar: com clima vs sem clima
com_clima = com_flag.filter("_clima_encontrado = true")
sem_clima = com_flag.filter("_clima_encontrado = false")

# Gravar: com clima (tabela principal)
(com_clima
  .select(
    "pedido_id", "data", "cliente_id", "produto_id", "produto",
    "quantidade", "valor", "regiao", "temp_media", "chuva_mm", "umidade_pct", "_ingest_ts"
  )
  .write.mode("overwrite")
  .saveAsTable("techsmart.silver.vendas_enriquecidas")
)

# Gravar: sem clima (quarentena)
(sem_clima
  .select(
    "pedido_id", "data", "cliente_id", "produto_id", "produto",
    "quantidade", "valor", "regiao", "_ingest_ts"
  )
  .withColumn("_motivo_reprovacao", F.lit("clima_nao_encontrado"))
  .withColumn("_quarentena_ts", F.current_timestamp())
  .write.mode("append")
  .option("mergeSchema", "true")
  .saveAsTable("techsmart.silver.quarentena")
)

print(f"✓ [10.6] Com clima: {com_clima.count():,}")
print(f"✓ [10.6] Sem clima (quarentena): {sem_clima.count():,}")

# ============================================================================
# MÓDULO 10.7 — TORNANDO O SILVER INCREMENTAL
# ============================================================================

# Ler último timestamp processado
try:
    estado = spark.sql("SELECT MAX(_ingest_ts) as ultimo_processamento FROM techsmart.silver.vendas_enriquecidas")
    ultimo_ts = estado.collect()[0]["ultimo_processamento"]
except:
    # Primeira execução: pega tudo
    ultimo_ts = datetime(2000, 1, 1)

print(f"\n✓ [10.7] Processando desde: {ultimo_ts}")

# Ler apenas dados novos do Bronze
df_novo = (spark.table("techsmart.bronze.vendas")
  .filter(F.col("_ingest_ts") > F.lit(ultimo_ts))
)

print(f"✓ [10.7] Linhas novas encontradas: {df_novo.count():,}")

# Aplicar transformações (tipagem)
df_tipado = (df_novo
  .withColumn("valor", F.col("valor").cast("double"))
  .withColumn("quantidade", F.col("quantidade").cast("int"))
  .withColumn("cliente_id", F.col("cliente_id").cast("int"))
  .withColumn("data_venda", F.col("data_venda").cast("date"))
)

# Deduplicação: última linha por pedido_id
deduplicado = (df_tipado
  .withColumn("rn", F.row_number().over(
    Window.partitionBy("pedido_id").orderBy(F.desc("_ingest_ts"))
  ))
  .filter("rn = 1")
  .drop("rn")
)

# Validação
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

aprovados = deduplicado.filter(todas_ok)
reprovados = deduplicado.filter(~todas_ok)

# Append incremental (não sobrescreve)
(aprovados
  .select(*colunas_finais)
  .write.mode("append")
  .option("mergeSchema", "true")
  .saveAsTable("techsmart.silver.vendas")
)

# Append para quarentena
(reprovados
  .select(*colunas_finais)
  .withColumn("_motivo_reprovacao", motivo_expr)
  .withColumn("_quarentena_ts", F.current_timestamp())
  .write.mode("append")
  .option("mergeSchema", "true")
  .saveAsTable("techsmart.silver.quarentena")
)

print(f"✓ [10.7] Aprovados incrementais: {aprovados.count():,}")
print(f"✓ [10.7] Reprovados incrementais: {reprovados.count():,}")