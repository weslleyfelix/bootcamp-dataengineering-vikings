# Databricks notebook source
# MAGIC %md
# MAGIC # Dim Calendario
# MAGIC * Nesta camada o objetivo é enviesar os dados para atender a perguntas de negócio, é aqui que nos filtramos e aplicamos as regras especificadas pelo stakeholder;
# MAGIC * Podemos criar datasets mais neste caso vamos seguir com fatos e dimensões.

# COMMAND ----------

# DBTITLE 1,Bibliotecas
import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

mountpoint = "/mnt/delta"
camada_escrita = "gold" 
dominio_negocio = "universal"
dim_ou_fato = "dim"
nome_tabela_destino = 'dim_calendario'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+dim_ou_fato+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Cria dataframe com o rang de data especifico
df_calendario = spark.createDataFrame([(1,)],["id"])

df_calendario = df_calendario.withColumn(
  "date",
  f.explode(f.expr("sequence(to_date('1900-01-01'),to_date('2040-01-01'), interval 1 day)"))
)

# COMMAND ----------

df_calendario.display()

# COMMAND ----------

df_calendario = df_calendario.drop('id')
df_calendario = df_calendario.withColumn('ANO' , f.date_format(f.col('date'), 'yyyy').cast(IntegerType()))
df_calendario = df_calendario.withColumn('MES' , f.date_format(f.col('date'), 'MM').cast(IntegerType()))
df_calendario = df_calendario.withColumn('DIA' , f.date_format(f.col('date'), 'dd').cast(IntegerType()))
df_calendario = df_calendario.withColumn('SAFRA', f.date_format(f.col('date'), 'yyyy-MM').cast(StringType()))
df_calendario = df_calendario.withColumn('NR_ANO_MES', f.date_format(f.col('date'), 'yyyyMM').cast(IntegerType()))
df_calendario = df_calendario.withColumn('NR_ANO_MES_DIA', f.date_format(f.col('date'), 'yyyyMMdd').cast(IntegerType()))
df_calendario = df_calendario.withColumn('DATA', f.col('date'))
df_calendario = df_calendario.drop('date')

df_calendario = df_calendario.select('DATA','DIA','MES','ANO','SAFRA','NR_ANO_MES','NR_ANO_MES_DIA')

# COMMAND ----------

df_calendario.display()

# COMMAND ----------

# DBTITLE 1,Salvar os dados
df_calendario.write \
             .mode("overwrite") \
             .format("delta") \
             .option("path",dir_destino) \
             .option("mergeSchema","True") \
             .partitionBy("ANO") \
             .saveAsTable(camada_escrita+"."+nome_tabela_destino)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(data) dt_minima, max(data) dt_maxima, count(*) qt_linhas 
# MAGIC from gold.dim_calendario
