# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import json, random, time
from datetime import datetime

VOLUME = "/Volumes/techsmart/landing/arquivos/estoque"

lojas = [f"LOJA-{i:03d}" for i in range(1, 21)]

for ciclo in range(10):
    eventos = []
    for _ in range(200):
        eventos.append({
            "evento_id": f"EVT-{int(time.time()*1000)}-{random.randint(0,9999)}",
            "timestamp": datetime.utcnow().isoformat(),
            "loja_id": random.choice(lojas),
            "produto_id": random.randint(1, 8),
            "quantidade": random.randint(0, 50),
            "tipo_evento": random.choices(
            ["contagem", "reposicao", "venda"],
            weights=[60, 15, 25])[0],
        })
    caminho = f"{VOLUME}/estoque_{int(time.time())}.json"
    dbutils.fs.put(caminho, "\n".join(json.dumps(e) for e in eventos), overwrite=True)
    print(f"Ciclo {ciclo+1}: {caminho}")
    time.sleep(5)