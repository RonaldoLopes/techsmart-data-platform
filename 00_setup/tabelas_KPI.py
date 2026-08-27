# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE techsmart.gold.kpi_diario AS
# MAGIC SELECT
# MAGIC     f.data,
# MAGIC     f.regiao,
# MAGIC     COUNT(DISTINCT f.pedido_id) AS pedidos,
# MAGIC     COUNT(DISTINCT f.cliente_id) AS clientes_unicos,
# MAGIC     SUM(f.quantidade) AS itens_vendidos,
# MAGIC     SUM(f.valor) AS receita,
# MAGIC     ROUND(AVG(f.valor), 2) AS ticket_medio,
# MAGIC     ROUND(AVG(f.temp_media), 1) AS temp_media,
# MAGIC     ROUND(SUM(f.chuva_mm) / COUNT(DISTINCT f.pedido_id), 2) AS chuva_media
# MAGIC FROM techsmart.gold.fato_vendas f
# MAGIC GROUP BY f.data, f.regiao;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE techsmart.gold.kpi_produto AS
# MAGIC SELECT
# MAGIC     p.produto_id,
# MAGIC     p.produto_nome,
# MAGIC     p.categoria,
# MAGIC     SUM(f.quantidade) AS unidades,
# MAGIC     SUM(f.valor) AS receita,
# MAGIC     ROUND(AVG(f.valor_unitario), 2) AS preco_medio,
# MAGIC     COUNT(DISTINCT f.cliente_id) AS clientes
# MAGIC FROM techsmart.gold.fato_vendas f
# MAGIC JOIN techsmart.gold.dim_produto p USING (produto_id)
# MAGIC GROUP BY p.produto_id, p.produto_nome, p.categoria;