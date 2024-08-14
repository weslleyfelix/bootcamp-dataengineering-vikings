# Databricks notebook source
dbutils.widgets.text("nome", "")
nome = dbutils.widgets.get("nome")

# COMMAND ----------

print(nome)
