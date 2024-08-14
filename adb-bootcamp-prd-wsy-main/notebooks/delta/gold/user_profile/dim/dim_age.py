# Databricks notebook source
# MAGIC %md
# MAGIC # Dim Age
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
dominio_negocio = "user_profile"
dim_ou_fato = "dim"
nome_tabela_destino = 'dim_age'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+dim_ou_fato+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Refinando os dados para montar a dim
df = (spark.table(camada_leitura+".users_app")
           .select(
               f.when((f.col('age')>=18) & (f.col('age') <= 20) ,1)
                .when((f.col('age')>=21) & (f.col('age') <= 25) ,2)
                .when((f.col('age')>=26) & (f.col('age') <= 30) ,3)
                .when((f.col('age')>=31) & (f.col('age') <= 35) ,4)
                .when((f.col('age')>=36) & (f.col('age') <= 40) ,5)
                .when((f.col('age')>=41) & (f.col('age') <= 45) ,6)
                .when((f.col('age')>=46) & (f.col('age') <= 50) ,7)
                .when((f.col('age')>=51) & (f.col('age') <= 55) ,8)
                .when((f.col('age')>=56) & (f.col('age') <= 60) ,9)
                .when(f.col('age') > 60 ,f.lit(10))
                .otherwise(0).alias("codeAge"),
               
               f.when((f.col('age')>=18) & (f.col('age') <= 20) ,f.lit("18 à 20"))
                .when((f.col('age')>=21) & (f.col('age') <= 25) ,f.lit("21 à 25"))
                .when((f.col('age')>=26) & (f.col('age') <= 30) ,f.lit("26 à 30"))
                .when((f.col('age')>=31) & (f.col('age') <= 35) ,f.lit("31 à 35"))
                .when((f.col('age')>=36) & (f.col('age') <= 40) ,f.lit("36 à 40"))
                .when((f.col('age')>=41) & (f.col('age') <= 45) ,f.lit("41 à 45"))
                .when((f.col('age')>=46) & (f.col('age') <= 50) ,f.lit("46 à 50"))
                .when((f.col('age')>=51) & (f.col('age') <= 55) ,f.lit("51 à 55"))
                .when((f.col('age')>=56) & (f.col('age') <= 60) ,f.lit("56 à 60" ))
                .when(f.col('age') > 60 ,f.lit("60 +"))
                .otherwise(0).alias("rangeAge")
                  
           ).distinct()
            .orderBy('codeAge')
            
            #surrogate key
            .withColumn('skAge', f.row_number().over(Window.partitionBy().orderBy('codeAge')) + 0)
            .select('skAge','codeAge','rangeAge',f.expr('date_format(current_date(),"yyyy-MM-dd")').alias('DT_ATUALIZACAO'))
     
)

# COMMAND ----------

# DBTITLE 1,Analisando os dados
df.display()

# COMMAND ----------

# DBTITLE 1,Persiste os dados na camada Gold.
df.write \
  .mode(write_mode) \
  .format("delta") \
  .option("path",dir_destino) \
  .option("mergeSchema","True") \
  .partitionBy("DT_ATUALIZACAO") \
  .saveAsTable(camada_escrita+"."+nome_tabela_destino)
