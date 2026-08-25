# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG techsmart;
# MAGIC CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Arquivos brutos, como chegaram';
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Delta fiel a origem + metadados de ingestao';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Limpo, tipado, deduplicado, cruzado';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Modelado para negocio: fatos, dimensoes e KPIs';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS techsmart.landing.arquivos
# MAGIC     COMMENT 'Arquivos brutos recebidos das origens';

# COMMAND ----------

# Criar subpastas
dbutils.fs.mkdirs("/Volumes/techsmart/landing/arquivos/vendas")
dbutils.fs.mkdirs("/Volumes/techsmart/landing/arquivos/clima")
dbutils.fs.mkdirs("/Volumes/techsmart/landing/arquivos/estoque")
# Listar
display(dbutils.fs.ls("/Volumes/techsmart/landing/arquivos"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PASSO 3: Criar tabela de teste em bronze
# MAGIC CREATE TABLE IF NOT EXISTS techsmart.bronze.teste (
# MAGIC     id INT,
# MAGIC     valor STRING,
# MAGIC     data TIMESTAMP
# MAGIC )
# MAGIC COMMENT 'Tabela de teste para validar o schema';
# MAGIC
# MAGIC -- Inserir três linhas
# MAGIC INSERT INTO techsmart.bronze.teste VALUES
# MAGIC (1, 'linha_um', current_timestamp()),
# MAGIC (2, 'linha_dois', current_timestamp()),
# MAGIC (3, 'linha_tres', current_timestamp());
# MAGIC
# MAGIC -- Verificar os dados
# MAGIC SELECT * FROM techsmart.bronze.teste;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PASSO 4: Validar que é MANAGED
# MAGIC DESCRIBE EXTENDED techsmart.bronze.teste;
# MAGIC -- Procure pela linha: Type    MANAGED (não EXTERNAL)