# Databricks notebook source
import json, random
from datetime import date, timedelta

# COMMAND ----------

random.seed(7)
VOLUME = "/Volumes/techsmart/landing/arquivos"
regioes = ["Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"]
base = {"Sudeste": 24, "Sul": 19, "Nordeste": 29, "Norte": 30, "Centro-Oeste": 26}

# COMMAND ----------

hoje = date.today()

for d in range(14):
    dia = hoje - timedelta(days=d)
    registros = []
    
    for r in regioes:
        registros.append({
            "data": dia.isoformat(),
            "regiao": r,
            "temp_media": round(base[r] + random.uniform(-5, 5), 1),
            "chuva_mm": round(max(0, random.gauss(3, 6)), 1),
            "umidade_pct": random.randint(40, 95),
        })
    
    caminho = f"{VOLUME}/clima/clima_{dia.isoformat()}.json"
    dbutils.fs.put(caminho, "\n".join(json.dumps(r) for r in registros), overwrite=True)
    print(f"✓ Gravado: {caminho}")

print(f"\n✓ Gerados 14 dias de clima em {VOLUME}/clima/")