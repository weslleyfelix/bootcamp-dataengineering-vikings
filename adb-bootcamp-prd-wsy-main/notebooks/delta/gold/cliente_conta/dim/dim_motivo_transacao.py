# Databricks notebook source
# MAGIC %md
# MAGIC # Dim Motivo Transação
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
#write_mode = "overwrite"
write_mode = "append"   
mountpoint = "/mnt/delta"
camada_escrita = "gold" 
dominio_negocio = "cliente_conta"
dim_ou_fato = "dim"
nome_tabela_destino = 'dim_motivo_transacao'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+dim_ou_fato+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Refinando os dados para montar a dim
df_origem = (
    
   spark.table(camada_leitura+".transacao")
        .select(
             'id_motivo',
             'ds_transacao_motivo'
        )
        .distinct()
        .orderBy('id_motivo')
         
)
df_origem.display()

# COMMAND ----------

# DBTITLE 1,Coletando a maior chave da tabela existente
try:
    v_nr_linhas = spark.table(camada_escrita+"."+nome_tabela_destino).agg(f.count('sk_motivo')).collect()[0][0]

    if v_nr_linhas > 0:
        maior_sk = spark.read.table(camada_escrita+"."+nome_tabela_destino).select('sk_motivo').agg({'sk_motivo':'max'}).collect()[0][0]

    else:
        maior_sk = 0


except AnalysisException as ex:
    maior_sk = 0

print(maior_sk)

# COMMAND ----------

# DBTITLE 1,Validação e carga do dataset que sera persistido no destino
try:
    df_registros_novos = (
            
        df_origem
        #sk incremental a partir da maior existente no destino
        .withColumn('sk_motivo',f.row_number().over(Window.partitionBy().orderBy('id_motivo')) + maior_sk)
        .alias('stg')

        #faremos um join do df_origem com a tabela de destino atraves do campo chave 
        .join(spark.table(camada_escrita+"."+nome_tabela_destino).alias('dim'),
                (f.col('stg.id_motivo')==f.col('dim.id_motivo')),"leftouter")  

        #o filtro abaixo só retorna valores caso ele não exista no destino         
        .where(f.col('dim.id_motivo').isNull())
        .select(
            'stg.sk_motivo',
            'stg.id_motivo',
            'stg.ds_transacao_motivo'
        )
        .withColumn('dt_atualizacao',f.expr('date_format(current_date(),"yyyy-MM-dd")'))

    )
    df = df_registros_novos

except:

    df_primeira_carga = (

        df_origem
        #sk incremental a partir da maior existente no destino
        .withColumn('sk_motivo',f.row_number().over(Window.partitionBy().orderBy('id_motivo')) + maior_sk)
        .alias('stg')
        .select(
            'stg.sk_motivo',
            'stg.id_motivo',
            'stg.ds_transacao_motivo'
        )
        .withColumn('dt_atualizacao',f.expr('date_format(current_date(),"yyyy-MM-dd")'))

    )
    df = df_primeira_carga
    
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
