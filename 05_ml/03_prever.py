# Databricks notebook source
from mlflow import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")

# pega a última versão registrada do modelo
versoes = client.search_model_versions("name='techsmart.gold.modelo_demanda'")
ultima_versao = max(versoes, key=lambda v: int(v.version)).version

client.set_registered_model_alias(
    name="techsmart.gold.modelo_demanda",
    alias="campeao",
    version=ultima_versao
)

print(f"Alias 'campeao' apontando para a versão {ultima_versao}")