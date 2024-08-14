# Databricks notebook source
# MAGIC %md
# MAGIC # Contas
# MAGIC * O objetivo dessa camada é refinar os dados limpar e transformar, alem disso é aqui na silver que unificamos varias tabelas bronze para enriquecer os registros e consolidar domínios de negócio.

# COMMAND ----------

# DBTITLE 1,Carrega as bibliotecas.
from pyspark.sql import functions as f
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Variáveis
camada_leitura = "bronze"  

mountpoint = "/mnt/delta"
camada_escrita = "silver" 
dominio_negocio = "cliente_conta"
produto_de_dado = "perfil_financeiro"
nome_tabela_destino = 'conta'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+produto_de_dado+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Refinando os dados de Contas
df = (
   
    spark.table(camada_leitura+'.conta__transacao__core_financeiro__sqlserver')
          .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_conta'
                     ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
          .filter(f.col('REGISTRO_RECENTE')==1)
          .alias('conta')
   

    .join(spark.table(camada_leitura+'.status_conta__transacao__core_financeiro__sqlserver')
               .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_status_conta'
                          ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
               .filter(f.col('REGISTRO_RECENTE')==1)
               .alias('status_conta'),
          (f.col('status_conta.id_status_conta') == f.col('conta.id_status_conta')), 'inner')

    .select(

         'conta.id_conta',
         'conta.id_login',
         'conta.cd_conta',
         'status_conta.id_status_conta',
         'status_conta.ds_status_conta',
         f.expr('date_format(current_date(),"yyyy-MM-dd")').alias('dt_importacao')

    ).distinct()

)

df.display()

# COMMAND ----------

df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("path",dir_destino) \
  .option("mergeSchema","True") \
  .partitionBy("DT_IMPORTACAO") \
  .saveAsTable(camada_escrita+"."+nome_tabela_destino)
