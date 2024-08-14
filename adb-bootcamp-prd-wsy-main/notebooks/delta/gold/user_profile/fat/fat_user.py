# Databricks notebook source
# MAGIC %md
# MAGIC # Fat User
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
dim_ou_fato = "fat"
nome_tabela_destino = 'fat_user'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+dim_ou_fato+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

display(spark.table(camada_leitura+".users_app"))

# COMMAND ----------

# DBTITLE 1,Sumarizando os valores da fat
df_stg_fat = (spark.table(camada_leitura+".users_app")
           .select(
               'id',
               'city', 
               'gender',
               'companyDepartment',
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
                .otherwise(0).alias("codeAge")
 
           ) 
           .groupby('city', 
                    'gender',
                    'companyDepartment',
                    'codeAge'
           ).agg(f.countDistinct('id').alias('nrUsers'))         
)

# COMMAND ----------

# DBTITLE 1,Dados sumarizados
df_stg_fat.display()

# COMMAND ----------

# DBTITLE 1,Pegando SK nas Dims
df = (
    df_stg_fat.alias('stg')
    
              .join(spark.table('gold.dim_address') 
                         .alias('dim_ad'),
                         (f.col('dim_ad.city') == f.col('stg.city')),'inner')
    
             .join(spark.table('gold.dim_age') 
                    .alias('dim_ag'),
                    (f.col('dim_ag.codeAge') == f.col('stg.codeAge')),'inner')
    
             .join(spark.table('gold.dim_gender') 
                    .alias('dim_gd'),
                    (f.col('dim_gd.genderUser') == f.col('stg.gender')),'inner')
    
             .join(spark.table('gold.dim_profession') 
                    .alias('dim_pf'),
                    (f.col('dim_pf.companyDepartment') == f.col('stg.companyDepartment')),'inner')
             
             .select('stg.nrUsers'
                    ,'dim_ad.skAddress'
                    ,'dim_ag.skAge'
                    ,'dim_gd.skGender'
                    ,'dim_pf.skProfession'
             )
             .groupby(
                      'dim_ad.skAddress',
                      'dim_ag.skAge',
                      'dim_gd.skGender',
                      'dim_pf.skProfession'
              )
              .agg(f.sum('stg.nrUsers').alias('nrUsers')) 
              .withColumn('DT_ATUALIZACAO',f.expr('date_format(current_date(),"yyyy-MM-dd")'))

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
