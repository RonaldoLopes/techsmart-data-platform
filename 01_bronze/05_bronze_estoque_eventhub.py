# Databricks notebook source
from pyspark.sql import functions as F, types as T

# COMMAND ----------

conn = dbutils.secrets.get(scope="techsmart", key="eventhub-conn")

# COMMAND ----------

opcoes = {
    "kafka.bootstrap.servers": "ehns-techsmart-dev.servicebus.windows.net:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{conn}";'
    ),
    "subscribe": "eh-estoque",
    "startingOffsets": "earliest",
}

schema = T.StructType([
    T.StructField("evento_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("loja_id", T.StringType()),
    T.StructField("produto_id", T.IntegerType()),
    T.StructField("quantidade", T.IntegerType()),
    T.StructField("tipo_evento", T.StringType()),
])

stream = (
    spark.readStream.format("kafka")
    .options(**opcoes)
    .load()
    # O payload vem em binario na coluna "value"
    .select(
        F.from_json(F.col("value").cast("string"), schema).alias("dados"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("partition"),
        F.col("offset")
    )
    .select("dados.*", "kafka_ts", "partition", "offset")
    .withColumn("evento_ts", F.to_timestamp("timestamp"))
    .withColumn("_ingest_ts", F.current_timestamp())
)

(stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",
    "/Volumes/techsmart/landing/arquivos/_checkpoints/eh_estoque")
    .trigger(availableNow=True)
    .toTable("techsmart.bronze.estoque_eventos_eh")
 )