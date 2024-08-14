# Databricks notebook source
# MAGIC %md
# MAGIC # Users App
# MAGIC * O objetivo dessa camada é refinar os dados limpar e transformar, alem disso é aqui na silver que unificamos varias tabelas bronze para enriquecer os registros e consolidar domínios de negócio.

# COMMAND ----------

# DBTITLE 1,Carrega as bibliotecas.
from pyspark.sql import functions as f

# COMMAND ----------

# DBTITLE 1,Variáveis
camada_leitura = "bronze"  

mountpoint = "/mnt/delta"
camada_escrita = "silver" 
dominio_negocio = "customer_success"
produto_de_dado = "web_application"
nome_tabela_destino = 'users_app'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+produto_de_dado+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Entendendo os dados da camada anterior
display(spark.table(camada_leitura+'.users__dummy_js__web_api'))

# COMMAND ----------

# DBTITLE 1,Refinando os dados da API
df = (
    spark.table(camada_leitura+'.users__dummy_js__web_api')
         .withColumn('user_explode',f.explode('users') )
         .select(
             
             #perfil
             f.col('user_explode.id').alias('id'),  
             
             f.expr('concat(user_explode.firstName," ",user_explode.maidenName," ",user_explode.lastName)').alias('Name'),
             f.col('user_explode.birthDate').alias('birthDate'),
             f.floor(f.datediff(f.current_date(), f.to_date(f.col('user_explode.birthDate'), "yyyy-MM-dd"))/365).alias("Age"),
             f.col('user_explode.gender').alias('gender'), 
             f.col('user_explode.height').alias('height'), 
             f.col('user_explode.weight').alias('weight'),
             f.col('user_explode.bloodGroup').alias('bloodGroup'), 
             f.col('user_explode.eyeColor').alias('eyeColor'), 
             f.col('user_explode.hair.color').alias('hairColor'),
             f.col('user_explode.hair.type').alias('hairType'),
             
             #endereço
             f.col('user_explode.address.city').alias('city'), 
             f.col('user_explode.address.state').alias('state'),
             f.col('user_explode.address.coordinates.lng').alias('lat'),
             f.col('user_explode.address.coordinates.lng').alias('long'),
             f.col('user_explode.address.postalCode').alias('postalCode'), 
             
             #contato         
             f.col('user_explode.email').alias('email'),
             f.col('user_explode.phone').alias('phone'), 
             
             #dados bancarios
             #f.col('user_explode.bank').alias('bank'), 
             
             #profissão
             f.col('user_explode.company.name').alias('companyName'), 
             f.col('user_explode.company.department').alias('companyDepartment'), 
             f.col('user_explode.company.title').alias('companyTitle'),
             
             
             #f.col('user_explode.domain').alias('domain'),  
             #f.col('user_explode.image').alias('image'),  
             
             #dados de acesso
             f.col('user_explode.username').alias('username'),
             f.col('user_explode.password').alias('password'),
             f.col('user_explode.ip').alias('ip'),
             f.col('user_explode.macAddress').alias('macAddress'),
             f.col('user_explode.ssn').alias('ssn'), 
                          
             f.col('user_explode.university').alias('university'),  
             f.col('user_explode.userAgent').alias('userAgent'),
             f.expr('date_format(current_date(),"yyyy-MM-dd")').alias('DT_IMPORTACAO')
             
         ).distinct()
)

# COMMAND ----------

# DBTITLE 1,Analisando os dados
df.display()

# COMMAND ----------

# DBTITLE 1,Persiste os dados na camada Silver.
df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("path",dir_destino) \
  .option("mergeSchema","True") \
  .partitionBy("DT_IMPORTACAO") \
  .saveAsTable(camada_escrita+"."+nome_tabela_destino)

# COMMAND ----------

# MAGIC %sql select * from silver.users_app
