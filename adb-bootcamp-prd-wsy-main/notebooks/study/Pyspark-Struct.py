# Databricks notebook source
# MAGIC %md #Manipulando struct com pyspark

# COMMAND ----------

# MAGIC %md O objetivo deste notebook é dar um overview em alguns comandos da API alta do spark o Pyspark.

# COMMAND ----------

# MAGIC %md #### Struct

# COMMAND ----------

# MAGIC %md Geralmente quando salvamos os jsons, as colunas ficam no formato struct, abaixo temos um exemplo de tratamentos para colunas deste tipo

# COMMAND ----------

# DBTITLE 1,bibliotecas
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import Window 
from datetime import date, datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Dataframe de teste
data = [Row("Odemir","Depieri",25,[10,56,43,20],{"status":"Inactive"}), 
        Row("Ronisson","Lucas",29,[18,50,32],{"status":"Active"}), 
        Row("Weslley","Felix",30,[60,87,3],{"status":"Active"}) 
      ]

rdd = spark.sparkContext.parallelize(data)

# COMMAND ----------

# DBTITLE 1,Schema para o dataframe
scheme = StructType([
         StructField('Nome', StringType(), True),
         StructField('SobreNome', StringType(), True),
         StructField('Idade', IntegerType(), True),
         StructField("Pontos", ArrayType(StringType()), True),
         StructField("StatusUsuario", MapType(StringType(),StringType()), True)        
         ])

# COMMAND ----------

# DBTITLE 1,Conversão de RDD para DataFrame
df_origem = rdd.toDF(schema=scheme)

# COMMAND ----------

df_origem.printSchema()

# COMMAND ----------

df_origem.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Transformar array em linhas
df_com_pontos_em_linhas = (
    
  df_origem.withColumn("Ponto", f.explode("Pontos"))
           .select("Nome","Ponto","StatusUsuario.status")   
    
)

display(df_com_pontos_em_linhas)

# COMMAND ----------

# DBTITLE 1,filtrando array de uma coluna struct
#filtrando array de uma coluna struct
df_pontos_com_segundo_valor = (
    
   df_origem.filter(
           f.array_contains(f.col("Pontos"), "50")
    )
    
   #nova coluna com o segundo valor do array
   .withColumn("ObterPonto", f.element_at(f.col("Pontos"), 2))
)  

display(df_pontos_com_segundo_valor)

# COMMAND ----------

# DBTITLE 1,criar coluna com o primeiro valor de um array em uma coluna struct
df_pontos_com_primeiro_valor = (
    
    df_origem.withColumn("ObterPonto", f.element_at(f.col("Pontos"), 1))
    
)

display(df_pontos_com_primeiro_valor)

# COMMAND ----------

# DBTITLE 1,agrupando valores e sumarizando pontos para uma unica celula
df_agrupamento = (
  
  df_pontos_com_primeiro_valor.groupBy("StatusUsuario.status")
                              .agg(f.collect_set("ObterPonto").alias("Pontos"))
)
display(df_agrupamento)

# COMMAND ----------

# DBTITLE 1,Renomear coluna
df_renomear_novoGrupo = df_agrupamento.withColumnRenamed("Pontos","PontosNovos")
df_renomear_novoGrupo.show(truncate=False)

# COMMAND ----------

# MAGIC %md ### Json

# COMMAND ----------

# MAGIC %md O objetivo do exemplo abaixo é simular um case onde precisamos tratar um json para criar uma tabela normalizada atraves da API alta do spark o Pyspark.

# COMMAND ----------

# DBTITLE 1,lendo arquivo de origem
dir_origem = '/mnt/raw/apis/instrutores/jsonViking.json'
df_origem = spark.read.option("multiline", "true").json(dir_origem)
df_origem.display()

# COMMAND ----------

# DBTITLE 1,dando um explode nos atributos do json
df_explode_colecao = (
  df_origem.select(
    
     'anoDeInicio',
     'curso',
     'emAtividade',
    
     #explode no array para transformar em linhas
     f.explode(f.col('instrutores')).alias('instrutor')
  )
  
  #contando o numero de coleções que foram geradas no explode
  .withColumn('idInstrutor',f.row_number().over(Window.partitionBy().orderBy('instrutor')))
  
)

df_explode_colecao.display()

# COMMAND ----------

# DBTITLE 1,Selecionando as colunas struct
df_colunas_colecao = (
  df_explode_colecao
    .select(
       f.col('anoDeInicio').alias('ano_inicio'), #padrão de nomenclatura do cliente "_"
       'curso',
       f.col('emAtividade').alias('curso_ativo'),  
       f.col('idInstrutor').alias('id_instrutor'),
       'instrutor.nome',
       'instrutor.profissao',
       'instrutor.idade',
       'instrutor.conhecimento',
       'instrutor.infoAdicional.key',
       'instrutor.infoAdicional.value'
    )   
)

display(
    df_colunas_colecao
       #.where(f.col('instrutor.nome')=='Weslley')
)

# COMMAND ----------

# MAGIC %md #aqui

# COMMAND ----------

# DBTITLE 1,Recebe data de ingestão passada como parâmetro 
dbutils.widgets.text("data_carga", "")
data_carga = dbutils.widgets.get("data_carga")

try:
    if data_carga is None or not data_carga:
        data_carga = datetime.today().strftime('%Y')
except NameError:
    data_carga = datetime.today().strftime('%Y')
    print(data_carga)
else:
    print(data_carga)

# COMMAND ----------

df_save = (
    df_colunas_colecao
      .where(f.col('ano_inicio')>=data_carga)
     
      #sempre incluir a data de importação para controle dos dados
      .withColumn('data_importacao',
         f.expr('date_format(current_date(),"yyyy-MM-dd")')
      )
)

df_save.display()

# COMMAND ----------

# DBTITLE 1,variáveis de destino da tabela 
mount = "/mnt" 
camada_escrita = "sandbox" 
dominio_negocio = "instrutores"
nome_tabela_destino = 'intrutores_viking'

#destino onde iremos persistir o dado
dir_destino = mount+"/"+camada_escrita+"/"+dominio_negocio+"/"+nome_tabela_destino+"/"

print(dir_destino)

# COMMAND ----------

# MAGIC %sql  drop table  sandbox.intrutores_viking

# COMMAND ----------

# DBTITLE 1,Persistindo uma tabela delta
df_save.write \
       .mode("overwrite") \
       .format("delta") \
       .option("path",dir_destino) \
       .option("mergeSchema","True") \
       .partitionBy("data_importacao") \
       .option("replaceWhere", "ano_inicio >= "+str(data_carga)) \
       .saveAsTable(camada_escrita+"."+nome_tabela_destino)

# COMMAND ----------

spark.table('sandbox.intrutores_viking').display()

# COMMAND ----------

# MAGIC %md No exemplo abaixo os stakeholders solicitaram os documentos do intrutor weslley

# COMMAND ----------

# DBTITLE 1,dando um explode nos atributos da coluna struct
df_explode_result = (
  spark.table('sandbox.intrutores_viking')
    .where(f.col('nome')=='Weslley')
    .select(   
    'nome',
    'key',
    'value'
    )
  
    #explode no campo value
    .withColumn("dadoValue",f.explode(f.col("value")))
    
    #eliminando a uf
    .where(f.length(f.col('dadoValue'))>2)  
    
    #explode documentos
    .withColumn('coluna_dinamica', f.explode( f.split(f.col('dadoValue'),"#@#")))
    .withColumn('nr_coluna_dinamica',f.row_number().over(Window.partitionBy('nome').orderBy('coluna_dinamica'))) 
    .withColumn('colunas_dinamicas', f.concat_ws('_',f.lit("COL"),f.col('nr_coluna_dinamica').cast(f.StringType())))
  
)

df_explode_result.display()

# COMMAND ----------

# DBTITLE 1,explode delimitador = ":"
df_explode_result_delimitador = (
   
   df_explode_result
    
      #Combinando o split podemos escolher o delimitador no explode
      .withColumn('linha_dinamica',f.explode(f.split(f.col('coluna_dinamica'),':')))
    
      #Note que na linha gerada todo metadado ficou com o numero 1 e o dado com o numero 2
      .withColumn('nr_linha_dinamica',f.row_number().over(Window.partitionBy('nome','coluna_dinamica').orderBy(f.desc('linha_dinamica')))) 
                                            
)
df_explode_result_delimitador.display()
#df_explode_result_delimitador.orderBy('nome','nr_coluna_dinamica','nr_linha_dinamica').display()

# COMMAND ----------

# DBTITLE 1,pivotando valores
#metadados
df_colunas = (df_explode_result_delimitador.filter((f.col('nr_linha_dinamica').isin(1)))
                .groupby(  'nome',
                           'key',
                           'value',
                           'nr_linha_dinamica'
                )                
                .pivot('colunas_dinamicas').agg(f.max('linha_dinamica'))
)

df_colunas.display()

#dados
df_linhas = (df_explode_result_delimitador.filter(~f.col('nr_linha_dinamica').isin(1))
               .groupby( 'nome',
                         'key',
                         'value',
                         'nr_linha_dinamica'
               )               
               .pivot('colunas_dinamicas').agg(f.max('linha_dinamica'))
)

df_linhas.display()

#unir linhas e colunas
df_union = df_colunas.unionAll(df_linhas)
df_union.display()

# COMMAND ----------

# DBTITLE 1,variáveis auxiliares para renomear as colunas dinamicas
df_contagem = df_union.drop('nr_linha_dinamica')

linhas_dinamicas = df_contagem.count()
colunas_dinamicas = len(df_contagem.columns)-3

print(f'Dimensão do Dataframe: {(linhas_dinamicas,colunas_dinamicas)}')
print(f'Número de Linhas: {linhas_dinamicas}')
print(f'Número de Colunas: {colunas_dinamicas}')

# COMMAND ----------

# DBTITLE 1,função para padronizar colunas
def multipleReplace(text):
    for char in ".-!?/,_ ":
        text = str(text).replace(char, "")   
    return text
  
multipleReplaceUDF = f.udf(lambda x: multipleReplace(x), f.StringType())
spark.udf.register("multipleReplaceUDF", multipleReplace, f.StringType())

# COMMAND ----------

# DBTITLE 1,renomeando colunas dinâmicas
for coluna in range(colunas_dinamicas):
    
    numero_da_coluna = "COL_"+str(coluna+1)+""  
    
    #pegando o nome da coluna para renomear o col_ 
    nome_da_coluna = (df_union.filter(f.col('nr_linha_dinamica').isin(1))
                            .select(multipleReplaceUDF(numero_da_coluna).alias(numero_da_coluna))
                            .collect()[0][0]
                   )
    
    #renomeando a coluna 
    df_union = df_union.withColumnRenamed(numero_da_coluna,nome_da_coluna)
    print(f'Alteração na coluna : {(numero_da_coluna,nome_da_coluna)}')

# COMMAND ----------

df_union.display()

# COMMAND ----------

df = (df_union.filter(~f.col('nr_linha_dinamica').isin(1))
      
              #deletando a coluna guia nr_linha_dinamica
              .drop('nr_linha_dinamica','none')
     )

df.display()
