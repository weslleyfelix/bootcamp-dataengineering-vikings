# Databricks notebook source
# MAGIC %md
# MAGIC # Transações
# MAGIC * O objetivo dessa camada é refinar os dados limpar e transformar, alem disso é aqui na silver que unificamos varias tabelas bronze para enriquecer os registros e consolidar domínios de negócio.

# COMMAND ----------

# DBTITLE 1,Carrega as bibliotecas.
from pyspark.sql import functions as f
from pyspark.sql import Window

# COMMAND ----------

camada_leitura = "bronze"  

mountpoint = "/mnt/delta"
camada_escrita = "silver" 
dominio_negocio = "cliente_conta"
produto_de_dado = "perfil_financeiro"
nome_tabela_destino = 'transacao'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+produto_de_dado+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Refinando os dados de Transações
df = (
    
    spark.table(camada_leitura+'.transacao__core_financeiro__sqlserver')
          .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_transacao'
                     ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
          .filter(f.col('REGISTRO_RECENTE')==1)
          .alias('transacao') 

   
    .join(spark.table(camada_leitura+'.motivo_transacao__transacao__core_financeiro__sqlserver')
               .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_motivo'
                          ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
               .filter(f.col('REGISTRO_RECENTE')==1)
               .alias('motivo_transacao'),
          (f.col('motivo_transacao.id_motivo') == f.col('transacao.id_motivo')), 'inner')
   

    .join(spark.table(camada_leitura+'.natureza_transacao__transacao__core_financeiro__sqlserver')
               .withColumn('REGISTRO_RECENTE', f.row_number().over(Window.partitionBy('id_natureza'
                          ).orderBy(f.col('DT_INSERCAO_LAKE').desc())))
               .filter(f.col('REGISTRO_RECENTE')==1)
               .alias('natureza_transacao'),
          (f.col('natureza_transacao.id_natureza') == f.col('motivo_transacao.id_natureza')), 'inner')

    .select(

        'transacao.id_transacao',
        'transacao.id_conta',
        'transacao.dt_transacao',	
        'transacao.vl_transacao',
        'motivo_transacao.id_motivo',
        'motivo_transacao.ds_transacao_motivo',
        'natureza_transacao.id_natureza',
        'natureza_transacao.ds_natureza_operacao',
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
