# Databricks notebook source
import requests

url = "https://dadosabertos.camara.leg.br/api/v2/deputados"

print("Testando conexão...")

try:

    response = requests.get(
        url,
        params={"itens": 1},
        timeout=30
    )

    print("Status:", response.status_code)

    print("Resposta:", response.text[:200])

except Exception as e:

    print("Erro:", str(e))

# COMMAND ----------

