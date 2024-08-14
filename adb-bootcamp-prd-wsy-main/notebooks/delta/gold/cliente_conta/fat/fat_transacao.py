# Databricks notebook source
# MAGIC %md
# MAGIC # Fat Transação
# MAGIC * Nesta camada o objetivo é enviesar os dados para atender a perguntas de negócio, é aqui que nos filtramos e aplicamos as regras especificadas pelo stakeholder;
# MAGIC * Podemos criar datasets mais neste caso vamos seguir com fatos e dimensões.

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,Variáveis
camada_leitura = "silver"
write_mode = "overwrite"  
mountpoint = "/mnt/delta"
camada_escrita = "gold" 
dominio_negocio = "cliente_conta"
dim_ou_fato = "fat"
nome_tabela_destino = 'fat_transacao'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+dim_ou_fato+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

display(spark.table(camada_leitura+".transacao"))

# COMMAND ----------

# DBTITLE 1,Sumarizando os valores da fat
df_stg_fat = (spark.table(camada_leitura+".transacao")
           .select(
               'id_transacao',
               'id_natureza',
               'id_motivo',
               'dt_transacao',
               'vl_transacao'
 
           ) 
           .groupby('id_natureza', 
                    'id_motivo',
                    'dt_transacao'
           ).agg(
               f.count('id_transacao').alias('qt_transacao'),
               f.sum('vl_transacao').alias('vl_transacao'),
           )         
)
df_stg_fat.display()

# COMMAND ----------

# DBTITLE 1,Pegando SK nas Dims
df = (
    df_stg_fat.alias('stg')
    
              .join(spark.table('gold.dim_natureza_operacao') 
                         .alias('dim_nat'),
                         (f.col('dim_nat.id_natureza') == f.col('stg.id_natureza')),'inner')
    
             .join(spark.table('gold.dim_motivo_transacao') 
                    .alias('dim_mot'),
                    (f.col('dim_mot.id_motivo') == f.col('stg.id_motivo')),'inner')
    
             .select(
                     'dim_nat.sk_natureza',
                     'dim_mot.sk_motivo',
                     'stg.dt_transacao',
                     'stg.qt_transacao',
                     'stg.vl_transacao'
             )
             .groupby(
                      'dim_nat.sk_natureza',
                      'dim_mot.sk_motivo',
                      'stg.dt_transacao',
              )
              .agg(
                   f.sum('stg.qt_transacao').alias('qt_transacao'),
                   f.sum('stg.vl_transacao').alias('vl_transacao')
              ) 
              .withColumn('dt_atualizacao',f.expr('date_format(current_date(),"yyyy-MM-dd")'))

)
df.display()

# COMMAND ----------

# DBTITLE 1,Persiste os dados na camada Gold.
df.write \
  .mode(write_mode) \
  .format("delta") \
  .option("path",dir_destino) \
  .option("mergeSchema","True") \
  .partitionBy("dt_atualizacao") \
  .saveAsTable(camada_escrita+"."+nome_tabela_destino)

# COMMAND ----------

display(spark.table(camada_escrita+"."+nome_tabela_destino))
