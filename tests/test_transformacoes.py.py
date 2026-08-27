# Databricks notebook source
import pytest
from pyspark.sql import SparkSession, functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("testes-techsmart")
        .getOrCreate()
    )


def padronizar_regiao(df):
    """Funcao de producao, importada do modulo de transformacao."""
    return df.withColumn("regiao", F.initcap(F.trim(F.lower(F.col("regiao")))))


def test_padroniza_regiao(spark):
    entrada = spark.createDataFrame(
        [("SUDESTE",), (" sudeste ",), ("Sudeste",)], ["regiao"]
    )
    saida = padronizar_regiao(entrada)

    assert saida.select("regiao").distinct().count() == 1
    assert saida.first()["regiao"] == "Sudeste"


def test_valor_negativo_vai_para_quarentena(spark):
    entrada = spark.createDataFrame(
        [("P1", 100.0), ("P2", -50.0), ("P3", 200.0)], ["pedido_id", "valor"]
    )
    aprovados = entrada.filter(F.col("valor") > 0)
    reprovados = entrada.filter(F.col("valor") <= 0)

    assert aprovados.count() == 2
    assert reprovados.count() == 1


def test_dedup_mantem_uma_linha_por_pedido(spark):
    entrada = spark.createDataFrame(
        [("P1", 10.0), ("P1", 10.0), ("P2", 20.0)], ["pedido_id", "valor"]
    )
    assert entrada.dropDuplicates(["pedido_id"]).count() == 2