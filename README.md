# TechSmart Data Platform — Databricks

Plataforma de dados pronta para rodar pipelines, transformações e analytics. Foco em simplicidade: managed tables, sem Access Connectors, sem RBAC manual.

## 🏗️ Estrutura de dados

### Unity Catalog (three-level namespace)

```
techsmart                           ← Catálogo (catalog)
├── landing                         ← Schema de landing
│   ├── vendas                      ← Tabela (Bronze bruta)
│   ├── clima                       ← Tabela (Bronze bruta)
│   └── estoque                     ← Tabela (Streaming)
│
├── bronze                          ← Schema de Bronze
│   ├── vendas                      ← Auto Loader + metadados
│   ├── clima                       ← JSON ingerido
│   └── estoque                     ← Streaming + checkpoints
│
├── silver                          ← Schema de Silver (qualidade)
│   ├── vendas                      ← Tipado, padronizado, dedupado
│   ├── vendas_enriquecidas         ← Vendas + clima
│   └── quarentena                  ← Rejeitos da validação
│
└── gold                            ← Schema de Gold (negócio)
    ├── fato_vendas                 ← Star schema: centro
    ├── dim_produto                 ← Dimensão
    ├── dim_tempo                   ← Dimensão
    ├── dim_regiao                  ← Dimensão
    ├── kpi_diario                  ← Agregações pré-calculadas
    └── previsao_demanda            ← Saída de modelo ML
```

**Usar assim em SQL/Python:**

```sql
-- SQL
SELECT * FROM techsmart.gold.fato_vendas LIMIT 10;

-- Python
spark.sql("SELECT * FROM techsmart.gold.fato_vendas").display()
```

## 📥 Ingestão (Landing → Bronze)

### Auto Loader para arquivos CSV

```python
# Notebook: 01_bronze/01_bronze_vendas

spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", "/Volumes/techsmart/landing/checkpoint_vendas") \
  .schema("vendor_id INT, data DATE, regiao STRING, receita DOUBLE") \
  .load("/Volumes/techsmart/landing/vendas/") \
  .select(
    "*",
    F.current_timestamp().alias("_ingest_ts"),
    F.input_file_name().alias("_source_file"),
    F.input_file_size().alias("_file_size")
  ) \
  .writeStream \
  .mode("append") \
  .option("checkpointLocation", "/Volumes/techsmart/landing/checkpoint_vendas") \
  .toTable("techsmart.bronze.vendas")
```

**O que acontece:**
- Arquivo novo em `/Volumes/.../vendas/` → Auto Loader detecta
- Lê apenas os novos (checkpoint rastreia)
- Adiciona `_ingest_ts` (quando), `_source_file` (de onde), `_file_size` (tamanho)
- Insere na tabela Bronze de forma incremental

### JSON em tempo real (Event Hubs)

```python
# Notebook: 01_bronze/05_bronze_estoque_eventhub

connection_string = dbutils.secrets.get("techsmart", "eventhub-conn")

spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", f"{ns}.servicebus.windows.net:9093") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config", 
          f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string}";') \
  .option("subscribe", "eh-estoque") \
  .option("startingOffsets", "latest") \
  .load() \
  .select(F.col("value").cast("string").alias("json")) \
  .select(F.from_json("json", "produto STRING, quantidade INT, timestamp STRING").alias("data")) \
  .select("data.*", F.current_timestamp().alias("_ingest_ts")) \
  .writeStream \
  .option("checkpointLocation", "/Volumes/techsmart/landing/checkpoint_estoque") \
  .toTable("techsmart.bronze.estoque")
```

## 🔄 Transformação (Bronze → Silver → Gold)

### Silver: tipagem + limpeza

```python
# Notebook: 02_silver/01_silver_vendas

df = spark.table("techsmart.bronze.vendas")

df_silver = (df
  # Tipagem
  .withColumn("data", F.col("data").cast("date"))
  .withColumn("receita", F.col("receita").cast("decimal(10,2)"))
  
  # Padronização
  .withColumn("regiao", F.initcap(F.trim(F.lower(F.col("regiao")))))
  
  # Deduplicação (última linha por vendor_id + data)
  .withColumn("rn", F.row_number().over(
    Window.partitionBy("vendor_id", "data").orderBy(F.desc("_ingest_ts"))
  ))
  .filter("rn = 1")
  .drop("rn")
  
  # Validação: separa o bom do ruim
  .withColumn("_validado", 
    F.when((F.col("receita") > 0) & (F.col("regiao").isNotNull()), 1).otherwise(0)
  )
)

# Aprovados → Silver
(df_silver
  .filter("_validado = 1")
  .drop("_validado")
  .write.mode("overwrite")
  .option("mergeSchema", "true")
  .saveAsTable("techsmart.silver.vendas")
)

# Reprovados → Quarentena (com motivo)
(df_silver
  .filter("_validado = 0")
  .withColumn("_motivo_reprovacao",
    F.when(F.col("receita") <= 0, "receita_negativa_ou_zero")
     .when(F.col("regiao").isNull(), "regiao_vazia")
     .otherwise("motivo_desconhecido")
  )
  .write.mode("append")
  .saveAsTable("techsmart.silver.quarentena")
)
```

### Gold: modelagem de negócio (star schema)

```python
# Notebook: 03_gold/02_fato_vendas

fato = spark.sql("""
  SELECT 
    ROW_NUMBER() OVER (ORDER BY sv.data, sv.vendor_id) as id_venda,
    sv.data,
    sv.vendor_id,
    sv.produto_id,
    sv.regiao,
    sv.receita,
    sv.quantidade,
    dt.id_tempo,
    dr.id_regiao
  FROM techsmart.silver.vendas sv
  LEFT JOIN techsmart.gold.dim_tempo dt ON sv.data = dt.data
  LEFT JOIN techsmart.gold.dim_regiao dr ON sv.regiao = dr.regiao
""")

fato.write.mode("overwrite").option("replaceWhere", "data >= CURRENT_DATE - INTERVAL 7 DAYS").saveAsTable("techsmart.gold.fato_vendas")
```

## 📊 Agregações pré-calculadas (KPI)

```python
# Notebook: 03_gold/03_kpis

kpi_diario = spark.sql("""
  SELECT
    f.data,
    f.regiao,
    COUNT(*) as total_pedidos,
    SUM(f.receita) as receita_dia,
    AVG(f.receita) as ticket_medio,
    MIN(f.receita) as venda_minima,
    MAX(f.receita) as venda_maxima
  FROM techsmart.gold.fato_vendas f
  GROUP BY f.data, f.regiao
""")

kpi_diario.write.mode("overwrite").saveAsTable("techsmart.gold.kpi_diario")
```

## 🔧 Automação (Jobs + Workflows)

### Job diário

```json
{
  "name": "[DEV] techsmart-pipeline-diario",
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "America/Sao_Paulo"
  },
  "email_notifications": {
    "on_failure": ["seu-email@exemplo.com"]
  },
  "job_clusters": [
    {
      "job_cluster_key": "padrao",
      "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_D4pds_v6",
        "num_workers": 1,
        "aws_attributes": {
          "availability": "SPOT"
        }
      }
    }
  ],
  "tasks": [
    {
      "task_key": "bronze",
      "notebook_task": {
        "notebook_path": "/Repos/seu-usuario/techsmart/databricks/notebooks/01_bronze/01_bronze_vendas"
      }
    },
    {
      "task_key": "silver",
      "depends_on": [{"task_key": "bronze"}],
      "notebook_task": {
        "notebook_path": "/Repos/seu-usuario/techsmart/databricks/notebooks/02_silver/01_silver_vendas"
      }
    },
    {
      "task_key": "gold",
      "depends_on": [{"task_key": "silver"}],
      "notebook_task": {
        "notebook_path": "/Repos/seu-usuario/techsmart/databricks/notebooks/03_gold/03_kpis"
      }
    }
  ]
}
```

Salve como `databricks/jobs/pipeline_diario.json`.

Criar via CLI:

```bash
databricks jobs create --json-file databricks/jobs/pipeline_diario.json
```

## 📈 Dashboard e alertas

### Criar dashboard SQL

```
Databricks > SQL > Create > Dashboard
  ↓
Add visualization: nova query
  ↓
SELECT data, regiao, SUM(receita) as total
FROM techsmart.gold.kpi_diario
GROUP BY 1, 2
ORDER BY 1 DESC
  ↓
Type: Line chart, X = data, Y = total
```

### Alerta: "o pipeline rodou hoje?"

```sql
SELECT CASE
  WHEN MAX(data) = CURRENT_DATE THEN 'OK'
  ELSE 'FALHOU'
END as status,
MAX(data) as ultima_execucao
FROM techsmart.gold.kpi_diario
```

Alerta dispara se `status ≠ 'OK'`.

## 🔐 Segredos e permissões

### Guardar chaves

```bash
# Uma única vez
databricks secrets create-scope techsmart

# Adicionar segredo
databricks secrets put-secret techsmart api-clima
# (abrirá editor; cole o valor e Ctrl+D)

# Listar (nunca mostra valor)
databricks secrets list-secrets techsmart
```

### Usar no notebook

```python
api_key = dbutils.secrets.get("techsmart", "api-clima")
print(api_key)  # imprime [REDACTED]
```

### Permissões de tabela

```sql
-- Dar acesso de leitura ao grupo analytics
GRANT SELECT ON TABLE techsmart.gold.fato_vendas TO `analytics@empresa.com`;

-- Revogar
REVOKE SELECT ON TABLE techsmart.gold.fato_vendas FROM `analytics@empresa.com`;
```

## 📁 Estrutura de notebooks

```
databricks/notebooks/
├── 00_setup/
│   └── 00_sanity_check.py          ← testar conexão
│
├── 01_bronze/
│   ├── 00_gerar_dados_ficticios.py ← seed data
│   ├── 01_bronze_vendas.py         ← Auto Loader
│   ├── 02_bronze_clima.py          ← API
│   └── 05_bronze_estoque_eventhub.py ← Kafka
│
├── 02_silver/
│   ├── 01_silver_vendas.py
│   └── 02_silver_vendas_clima.py
│
├── 03_gold/
│   ├── 01_dimensoes.py
│   ├── 02_fato_vendas.py
│   └── 03_kpis.py
│
├── 04_dlt/
│   └── pipeline_techsmart.py       ← Declarative Pipeline
│
└── 05_ml/
    ├── 01_preparar_features.py
    ├── 02_treinar_modelo.py
    └── 03_prever.py
```

## 🧪 Validações rápidas

Rode no notebook de exploração:

```python
# Contar linhas por camada
for schema in ["landing", "bronze", "silver", "gold"]:
    count = spark.sql(f"SELECT COUNT(*) FROM techsmart.{schema}.*").collect()
    print(f"{schema}: ~X linhas")

# Checar que Landing tem os arquivos
dbutils.fs.ls("/Volumes/techsmart/landing/vendas/")

# Validar que Silver → Gold sem duplicatas
spark.sql("""
  SELECT COUNT(*) as fato, COUNT(DISTINCT id_venda) as unicos
  FROM techsmart.gold.fato_vendas
""").display()
```

## 🆘 Troubleshooting

### Auto Loader parou com `OutOfMemory`

Apagou checkpoint? Ele reprocessa tudo. Solução:
1. Truncar tabela Bronze
2. Apagar arquivo de checkpoint
3. Rodar o notebook novamente

```python
spark.sql("TRUNCATE TABLE techsmart.bronze.vendas")
dbutils.fs.rm("/Volumes/techsmart/landing/checkpoint_vendas", recurse=True)
```

### Schema evoluiu e job quebrou

Coluna nova ou tipo mudou? Ativar schema evolution:

```python
.option("mergeSchema", "true")
```

Próxima execução registra a coluna automaticamente.

### Streaming parado com "grupo em estado inconsistente"

State do streaming corrompeu. Solução:
1. Parar o notebook (quadrado vermelho)
2. Apagar checkpoint
3. Rodar novamente

### Soma do KPI ≠ soma do fato

Join duplicando linhas. Debugar:

```python
spark.sql("""
  SELECT regiao, COUNT(*) 
  FROM techsmart.gold.fato_vendas 
  GROUP BY regiao 
  HAVING COUNT(*) > 1000
  ORDER BY 2 DESC
""").display()
```

## ✅ Checklist de primeira execução

- [ ] Workspace criado (Terraform)
- [ ] Catálogo `techsmart` existe (criado via script)
- [ ] Notebooks clonados (Git folder)
- [ ] Dados fictícios gerados (sanity check)
- [ ] Bronze ingerindo (Auto Loader)
- [ ] Silver dedupado e validado
- [ ] Gold com star schema
- [ ] Job rodando diariamente
- [ ] Dashboard mostrando dados
- [ ] Alerta enviando e-mail

## 📚 Próximos passos

1. ✅ **Você está aqui** — pipelines de dados
2. **Módulo 14** — Databricks SQL e dashboards
3. **Módulo 13** — DLT (Declarative Pipelines)
4. **Módulo 20** — Git + CI/CD

## 🔗 Referência rápida

### SQL essencial

```sql
-- Contexto
USE CATALOG techsmart;
USE SCHEMA gold;

-- Exploração
SHOW TABLES;
DESCRIBE fato_vendas;
SHOW GRANTS ON TABLE fato_vendas;

-- Delta
SELECT * FROM fato_vendas VERSION AS OF 5;
OPTIMIZE fato_vendas ZORDER BY (data, regiao);
VACUUM fato_vendas RETAIN 7 DAYS;

-- Manutenção
ALTER TABLE fato_vendas ADD CONSTRAINT chk_receita CHECK (receita > 0);
COMMENT ON TABLE fato_vendas IS 'Tabela central de vendas';
```

### PySpark essencial

```python
# Ler
df = spark.table("techsmart.gold.fato_vendas")

# Transformar
df.filter(F.col("receita") > 0) \
  .withColumn("regiao_upper", F.upper(F.col("regiao"))) \
  .groupBy("regiao").agg(F.sum("receita")) \
  .orderBy(F.desc("sum(receita)"))

# Escrever
.write.mode("overwrite").saveAsTable("novo_nome")

# Stream
spark.readStream.format("cloudFiles") \
  .load("/path") \
  .writeStream.toTable("nome")
```

### dbutils essencial

```python
# Arquivos
dbutils.fs.ls("/Volumes/techsmart/landing/")
dbutils.fs.put("/tmp/arquivo.txt", "conteudo")
dbutils.fs.rm("/caminho", recurse=True)

# Widgets
dbutils.widgets.text("data_inicio", "2026-01-01")
data = dbutils.widgets.get("data_inicio")

# Segredos
chave = dbutils.secrets.get("techsmart", "api-key")

# Notebooks
dbutils.notebook.run("./outro_notebook", timeout_seconds=600)
```

---

**Versão:** 1.0  
**Última atualização:** Agosto 2026  
**Próximas partes do README:** Data Factory + GitHub (Módulo 16), Event Hubs (Módulo 17), MLflow (Módulo 19)
