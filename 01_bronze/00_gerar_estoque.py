# Databricks notebook source
import json
import random
from datetime import datetime, timedelta
import time

VOLUME = "/Volumes/techsmart/landing/arquivos"
produtos = [1, 2, 3, 4, 5, 6, 7, 8]
regioes = ["Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"]

print("Gerando 800 eventos de estoque (~50 segundos)...")
inicio = time.time()

eventos = []
for i in range(800):
    if i % 100 == 0:
        print(f"  {i}/800 eventos...")
    
    eventos.append({
        "timestamp": datetime.now().isoformat(),
        "produto_id": random.choice(produtos),
        "regiao": random.choice(regioes),
        "quantidade": random.randint(1, 100),
        "tipo": random.choice(["entrada", "saida"]),
    })
    
    # Simula delay para espalhar os eventos
    if i % 50 == 0:
        time.sleep(0.1)

# Grava num arquivo JSON Lines
caminho = f"{VOLUME}/estoque/estoque_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
conteudo = "\n".join(json.dumps(e) for e in eventos)
dbutils.fs.put(caminho, conteudo, overwrite=True)

duracao = time.time() - inicio
print(f"✓ Gerados {len(eventos)} eventos em {duracao:.1f}s")
print(f"✓ Arquivo: {caminho}")