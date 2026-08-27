# Databricks notebook source
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

mlflow.set_registry_uri("databricks-uc")  # registra no Unity Catalog
mlflow.set_experiment("/Users/email@gmail.com/techsmart-demanda")

pdf = spark.table("techsmart.gold.features_demanda").toPandas()
pdf = pd.get_dummies(pdf, columns=["regiao"], drop_first=True)

# Divisao TEMPORAL, nao aleatoria
corte = pdf["data"].max() - pd.Timedelta(days=2)  # ajustado de 7 para 2 dias
treino = pdf[pdf["data"] <= corte]
teste = pdf[pdf["data"] > corte]

colunas = [c for c in pdf.columns if c not in ("data", "receita_dia")]
X_tr, y_tr = treino[colunas], treino["receita_dia"]
X_te, y_te = teste[colunas], teste["receita_dia"]

with mlflow.start_run(run_name="gbr_baseline"):
    params = {"n_estimators": 200, "max_depth": 4,
              "learning_rate": 0.05, "random_state": 42}
    mlflow.log_params(params)
    
    modelo = GradientBoostingRegressor(**params).fit(X_tr, y_tr)
    pred = modelo.predict(X_te)
    
    mlflow.log_metrics({
        "mae": mean_absolute_error(y_te, pred),
        "mape": mean_absolute_percentage_error(y_te, pred),
        "n_treino": len(treino),
        "n_teste": len(teste),
    })
    mlflow.sklearn.log_model(
        modelo, "model",
        input_example=X_te.head(3),
        registered_model_name="techsmart.gold.modelo_demanda"
        )
    print(f"MAE : {mean_absolute_error(y_te, pred):,.2f}")
    print(f"MAPE: {mean_absolute_percentage_error(y_te, pred):.1%}")