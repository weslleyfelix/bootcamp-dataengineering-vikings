# Databricks notebook source
# MAGIC %md
# MAGIC # Dim Profession
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
nome_tabela_destino = 'dim_profession'

#destino onde iremos persistir o dado
dir_destino = mountpoint+"/"+camada_escrita+"/"+dominio_negocio+"/"+dim_ou_fato+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Refinando os dados para montar a dim
df = (spark.table(camada_leitura+".users_app")
           .select(
                   'companyDepartment'
             
                  
           ).distinct()
            .orderBy('companyDepartment')
            
            #surrogate key
            .withColumn('skProfession', f.row_number().over(Window.partitionBy().orderBy('companyDepartment')) + 0)
            .select('skProfession','companyDepartment',
                    f.expr('date_format(current_date(),"yyyy-MM-dd")').alias('DT_ATUALIZACAO')
             )
     
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
