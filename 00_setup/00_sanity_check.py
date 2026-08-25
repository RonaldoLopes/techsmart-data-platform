# Databricks notebook source
# MAGIC %md
# MAGIC # Sanity check
# MAGIC # Confirma que o workspace, o catalogo e o compute estao funcionando

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalogo,
# MAGIC current_schema() AS schema,
# MAGIC current_user() AS usuario,
# MAGIC current_version() AS versao_runtime;

# COMMAND ----------

df = spark.read.table("samples.nyctaxi.trips")
print(f"Linhas: {df.count():,}")
display(df.limit(10))

# COMMAND ----------

dbutils.widgets.text("data_processamento", "2026-08-23", "Data")
print("Widget lido:", dbutils.widgets.get("data_processamento"))