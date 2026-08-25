# Databricks notebook source
import random
from datetime import date, timedelta
from pyspark.sql import functions as F, types as T

random.seed(42)
VOLUME = "/Volumes/techsmart/landing/arquivos"
regioes = ["Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"]
produtos = [
    (1, "Fone Bluetooth", 199.90), (2, "Mouse sem fio", 89.90),
    (3, "Teclado mecanico", 349.90), (4, "Webcam Full HD", 259.90),
    (5, "Hub USB-C", 129.90), (6, "Carregador 65W", 179.90),
    (7, "Suporte notebook", 99.90), (8, "Cabo HDMI 2m", 49.90),
]

def gerar_vendas(dia: date, n: int = 800):
    linhas = []
    for i in range(n):
        pid, nome, preco = random.choice(produtos)
        qtd = random.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
        valor = round(preco * qtd, 2)
        
        cliente = random.randint(1000, 9999)
        if random.random() < 0.02:  # 2% sem cliente
            cliente = None
        if random.random() < 0.01:  # 1% com valor negativo
            valor = -valor
        
        regiao = random.choice(regioes)
        if random.random() < 0.03:  # 3% com regiao mal digitada
            regiao = regiao.upper()
        
        linhas.append({
            "pedido_id": f"PED-{dia.strftime('%Y%m%d')}-{i:05d}",
            "data_venda": dia.isoformat(),
            "cliente_id": cliente,
            "produto_id": pid,
            "produto": nome,
            "quantidade": qtd,
            "valor": valor,
            "regiao": regiao,
        })
    
    for _ in range(int(n * 0.015)):
        linhas.append(random.choice(linhas).copy())
    
    return linhas

# Gera 14 dias de vendas, um CSV por dia
hoje = date.today()
for d in range(14):
    dia = hoje - timedelta(days=d)
    dados = gerar_vendas(dia)
    df = spark.createDataFrame(dados)
    caminho = f"{VOLUME}/vendas/vendas_{dia.isoformat()}.csv"
    (df.coalesce(1)
     .write.mode("overwrite")
     .option("header", True)
     .csv(caminho + "_tmp"))
    
    parte = [f.path for f in dbutils.fs.ls(caminho + "_tmp") 
             if f.name.startswith("part-")][0]
    dbutils.fs.mv(parte, caminho)
    dbutils.fs.rm(caminho + "_tmp", recurse=True)

print(f"✓ Gerados 14 dias de vendas em {VOLUME}/vendas/")
display(dbutils.fs.ls(f"{VOLUME}/vendas"))