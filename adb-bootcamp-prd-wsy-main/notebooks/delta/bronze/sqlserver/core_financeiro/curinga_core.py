# Databricks notebook source
# MAGIC %md
# MAGIC # Generico sql
# MAGIC * O objetivo dessa camada é aplicar performance quando carregamos os dados da raw para a bronze, aqui utilizamos arquivos otimizados como delta.
# MAGIC * Obs.: Como este notebook monta o nome das tabelas em tempo de execução podemos utilizar para outras origens.

# COMMAND ----------

# DBTITLE 1,Carrega as bibliotecas.
from datetime import datetime
from pyspark.sql import functions as f

spark.sql("set spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

# DBTITLE 1,Recebe o diretório de ingestão passado como parâmetro no Data Factory.
dbutils.widgets.text("pn_diretorio_raw", "")
dir_arquivo = dbutils.widgets.get("pn_diretorio_raw")

# traz o diretorio ao qual este notebook está inserido
dir_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

try:
    if dir_arquivo is None or not dir_arquivo:
        dir_arquivo = f"{dir_notebook.rsplit('/', 1)[0].split('bronze/')[1]}"
except NameError:
    dir_arquivo = f"{dir_notebook.rsplit('/', 1)[0].split('bronze/')[1]}"
    
print(dir_arquivo)

# COMMAND ----------

# DBTITLE 1,Recebe o rascunho do nome da tabela delta passado como parâmetro pelo Data Factory.
dbutils.widgets.text("pn_rascunho_nome_tabela_delta", "")
rascunho_nome_tabela_delta = dbutils.widgets.get("pn_rascunho_nome_tabela_delta")
lista_rascunho_nome_tabela_delta = rascunho_nome_tabela_delta.split("/")

for parte in reversed(lista_rascunho_nome_tabela_delta):
    if lista_rascunho_nome_tabela_delta[-1] == parte:
        tabela_delta = parte
    else:
        tabela_delta += "__" + parte

print(tabela_delta)

# COMMAND ----------

# DBTITLE 1,Recebe a data de partição passada como parâmetro no Data Factory.
dbutils.widgets.text("pn_data_particao", "")
data_particao = dbutils.widgets.get("pn_data_particao") # espera receber uma data no formato YYYY/MM/DD.

try:
    if data_carga is None or not data_carga:
      data_carga = datetime.today().strftime('%Y/%m/%d')
except NameError:
  data_carga = datetime.today().strftime('%Y/%m/%d')
  
print(data_particao)

# COMMAND ----------

# DBTITLE 1,Recebe o nome do arquivo passado como parâmetro no Data Factory.
dbutils.widgets.text("pn_arquivo", "")
nome_arquivo = dbutils.widgets.get("pn_arquivo")

print(nome_arquivo)

# COMMAND ----------

# DBTITLE 1,Monta os diretórios de origem e destino
dir_arquivo_completo = f"{dir_arquivo}/{nome_arquivo}"

# monta os diretórios de origem e destino
dir_origem = f"/mnt/raw/{dir_arquivo_completo}"
dir_destino = f"/mnt/delta/bronze/{rascunho_nome_tabela_delta}"

print(dir_origem)
print(dir_destino)

# COMMAND ----------

# DBTITLE 1,Atribui ao dataframe df a leitura do arquivo parquet especificado no diretório da variável path.
df = spark.read.parquet(dir_origem)

# COMMAND ----------

# DBTITLE 1,Adiciona as colunas de data para controle da tabela.
df_adiciona_datas_controle = ( df.withColumn('DT_PARTICAO', f.lit(data_particao.replace('/','-')))
                                 .withColumn('DT_INSERCAO_LAKE', f.lit(datetime.today()))
)

# COMMAND ----------

# DBTITLE 1,Persiste os dados na camada Bronze.
df_adiciona_datas_controle.write.format("delta").mode("append").option("mergeSchema","true").partitionBy('DT_PARTICAO').save(dir_destino)

# COMMAND ----------

# DBTITLE 1,Cria a tabela no banco de dados Bronze usando o formato delta.
param = {
  "local": dir_destino,
  "tabela": tabela_delta,
  "esquema": "bronze"
}

spark.sql("DROP TABLE IF EXISTS {esquema}.{tabela}".format(**param))
spark.sql("CREATE TABLE {esquema}.{tabela} USING DELTA LOCATION '{local}'".format(**param))
spark.sql("REFRESH TABLE {esquema}.{tabela}".format(**param))

# COMMAND ----------

display(spark.sql("SELECT count(*) FROM {esquema}.{tabela}".format(**param)))
