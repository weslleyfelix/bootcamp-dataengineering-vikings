# Databricks notebook source
# MAGIC %md
# MAGIC # Perfil do Cliente
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
nome_tabela_destino = 'perfil_cliente'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+produto_de_dado+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Refinando os dados de Clientes
df = (
 
    spark.table(camada_leitura+'.login__cliente__core_financeiro__sqlserver')
          .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_login'
                     ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
          .filter(f.col('REGISTRO_RECENTE')==1)
          .alias('login') 

   
    .join(spark.table(camada_leitura+'.localizacao__cliente__core_financeiro__sqlserver')
               .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_localizacao'
                          ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
               .filter(f.col('REGISTRO_RECENTE')==1)
               .alias('localizacao'),
          (f.col('localizacao.id_localizacao') == f.col('login.id_localizacao')), 'inner')
   

    .join(spark.table(camada_leitura+'.sexo__cliente__core_financeiro__sqlserver')
               .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_sexo'
                          ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
               .filter(f.col('REGISTRO_RECENTE')==1)
               .alias('sexo'),
          (f.col('sexo.id_sexo') == f.col('login.id_sexo')), 'inner')

    .select(
         'login.id_login',
         'login.nm_cliente',
         'sexo.id_sexo',
         'sexo.ds_sexo',
         'localizacao.id_localizacao',
         'localizacao.nm_estado',
         'localizacao.sg_estado',
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
