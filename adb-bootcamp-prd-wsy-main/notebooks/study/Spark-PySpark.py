# Databricks notebook source
# MAGIC %md
# MAGIC # **PySpark**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ementa da aula

# COMMAND ----------

# MAGIC %md
# MAGIC * Comandos Básicos
# MAGIC * Filtro
# MAGIC * Manipulação de colunas
# MAGIC * SQL no Spark
# MAGIC * Duplicidade
# MAGIC * Agregação
# MAGIC * Agrupamento
# MAGIC * Visualização de Dados
# MAGIC * Join
# MAGIC * Pivot e concat
# MAGIC * UDF
# MAGIC * expr()
# MAGIC * Manipulação de datas
# MAGIC * Manipulação de texto

# COMMAND ----------

# MAGIC %md
# MAGIC #### PySpark
# MAGIC
# MAGIC
# MAGIC ###### **Uma Breve Introdução ao PySpark**
# MAGIC
# MAGIC **PySpark** é uma linguagem de programação criada pela *Apache Software Foundation* que permite a análise e processamento de grandes conjuntos de dados de forma distribuída em um cluster de computadores. 
# MAGIC <br>
# MAGIC Isso torna o PySpark uma excelente opção para lidar com dados em grande escala (Big Data) e para realizar tarefas como:
# MAGIC 1. Análise exploratória de dados;
# MAGIC 2. Construção de pipelines de dados;
# MAGIC 3. Criação de modelos de aprendizado de máquina;
# MAGIC 4. Criação de ETLs.
# MAGIC
# MAGIC A principal vantagem do **PySpark** é sua capacidade de lidar com grandes conjuntos de dados de maneira eficiente e escalável. Ele usa o ***Apache Spark***, um motor de **computação distribuído**, para dividir o processamento de dados em várias máquinas. Isso permite que o PySpark processe grandes quantidades de dados muito mais rapidamente do que seria possível com uma única máquina.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comandos Básicos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.dados_enem_2021_sp USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_sp.csv", encoding "UTF8" ,header "true", inferSchema "true");
# MAGIC CREATE TABLE default.tb_payments USING csv OPTIONS (path "/FileStore/tables/tb_payments.csv", encoding "UTF8", header "true", inferSchema "true");
# MAGIC CREATE TABLE default.dados_enem_2021_ba USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_ba.csv", encoding "UTF8", header "true", inferSchema "true");
# MAGIC CREATE TABLE default.dados_enem_2021_sp_quest USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_sp_quest.csv", encoding "UTF8", header "true", inferSchema "true");
# MAGIC CREATE TABLE default.dados_enem_2021_ba_quest USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_ba_quest.csv", encoding "UTF8", header "true", inferSchema "true");

# COMMAND ----------

# imports
# biblioteca com funções SQL
import pyspark.sql.functions as F

# load data
df          = spark.table('default.dados_enem_2021_ba')
df_ba_quest = spark.table('default.dados_enem_2021_ba_quest')
df_sp_quest = spark.table('default.dados_enem_2021_sp_quest')
df_sp       = spark.table('default.dados_enem_2021_sp')
payments = spark.table('default.tb_payments')

# COMMAND ----------

# estrutura da base
df.printSchema()

# COMMAND ----------

# colunas
df.columns

# COMMAND ----------

# quantidade de colunas
len(df.columns)

# COMMAND ----------

# type
type(df)

# COMMAND ----------

# mostrando os valores
df.limit(10).show()

# COMMAND ----------

# conversão para Pandas
df_pandas = df.toPandas()

# COMMAND ----------

df_pandas.head()

# COMMAND ----------

# conversão Pandas para Spark
df_spark = spark.createDataFrame(df_pandas)
display(df_spark)

# COMMAND ----------

# Selecionando colunas

# COMMAND ----------

df.select('NU_INSCRICAO').display()

# COMMAND ----------

df.select(['NU_INSCRICAO']).display()

# COMMAND ----------

df.select(F.col('NU_INSCRICAO')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Selecionando várias colunas

# COMMAND ----------

df.select(F.col('NU_INSCRICAO'), F.col('TP_SEXO')).display()

# COMMAND ----------

df.select(['NU_INSCRICAO','TP_SEXO']).display()

# COMMAND ----------

df.select('NU_INSCRICAO','TP_SEXO').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Sumário Estatístico

# COMMAND ----------

(df
 .select(['NU_NOTA_MT', 'NU_NOTA_CH', 'NU_NOTA_CN', 'NU_NOTA_LC'])
 .describe()
 .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Para obter os valores únicos de uma coluna podemos usar o comando `.distinct()`.

# COMMAND ----------

# distinct - similar ao unique do Pandas
df.select(F.col('TP_SEXO')).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtro

# COMMAND ----------

# MAGIC %md
# MAGIC Um procedimento muito comum ao manipular dados é a necessidade de filtrar bases por algum condição ou mesmo por um conjunto de condições. Para tanto, podemos usar os comandos `.filter()` e `.where()`.

# COMMAND ----------

# condição simples
df.filter(F.col('TP_SEXO') == 'F').display()

# Outras sintaxes:
# df.filter(df.TP_SEXO == 'F').display()
# df.filter('TP_SEXO == "F"').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Vejamos agora como filtrar os dados a partir de uma condição múltipla. Podemos usar os operadores `AND` (**&**) ou `OR` (**|**). Lembre-se que o operador `AND` retorna True se todas as condições forem verdadeiras, ao passo que o operador `OR` retorna True se pelo menos uma condição for True.

# COMMAND ----------

subset = (
    df
    .filter(
        (F.col('TP_SEXO') == 'F') &
        (F.col('NU_NOTA_MT') >= 300 ) &
        (F.col('NO_MUNICIPIO_PROVA') == 'Salvador')
    )
    .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
)

display(subset)

# COMMAND ----------

# MAGIC %md
# MAGIC Para filtrar missing values podemos usar `isNull()` e `isNotNull()`.

# COMMAND ----------

# filtrando missing values - isNull()
subset2 = (
    df
    .filter(F.col('NU_NOTA_MT').isNull())
    .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
)
display(subset2)

# COMMAND ----------

# filtrando missing values - isNotNull()
subset3 = (
    df
    .filter(F.col('NU_NOTA_MT').isNotNull())
    .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
)
display(subset3)

# COMMAND ----------

# MAGIC %md
# MAGIC Na próxima query vamos usar o operador `.isin()` para filtrar os candidatos que residem apenas nas cidades que iremos definir dentro da lista.

# COMMAND ----------

(
    df
    .filter((F.col('NO_MUNICIPIO_PROVA').isin(['Salvador', 'Feira de Santana'])) &
            (F.col('NU_NOTA_MT').isNotNull()) &
            (F.col('TP_SEXO') == 'F') &
            (F.col('NU_NOTA_MT') >= 450)
           )
    .select(['NU_INSCRICAO', 'TP_SEXO', 'NO_MUNICIPIO_PROVA', 'NU_NOTA_MT'])
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos fazer o mesmo comando no SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   NU_INSCRICAO as id_aluno, 
# MAGIC   TP_SEXO      as sexo, 
# MAGIC   NO_MUNICIPIO_PROVA as municipio, 
# MAGIC   NU_NOTA_MT as nota_mt
# MAGIC from default.dados_enem_2021_ba
# MAGIC where 
# MAGIC   NO_MUNICIPIO_PROVA IN ('Salvador', 'Feira de Santana')
# MAGIC   AND NU_NOTA_MT IS NOT NULL
# MAGIC   AND TP_SEXO = 'F'
# MAGIC   AND NU_NOTA_MT >= 450
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos negar uma condição com o operador `~` (similar ao pandas).

# COMMAND ----------

(
    df.filter(~(F.col('NO_MUNICIPIO_PROVA').isin(['Salvador'])))
    .select('NO_MUNICIPIO_PROVA').distinct().display()
)

# COMMAND ----------

# outra sintaxe possível:
(
    df.filter(F.col('NO_MUNICIPIO_PROVA').isin(['Salvador']) == False)
    .select('NO_MUNICIPIO_PROVA').distinct().display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos usar ainda a função `where()` para efetuar fazer filtros. Ambas as funções produzem os mesmos resultados.

# COMMAND ----------

(df.filter((F.col('TP_SEXO') == 'M') & (F.col('NU_NOTA_MT') >= 650))
 .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
 .display()
)

# COMMAND ----------

(
    df
    .where('(TP_SEXO == "M") AND (NU_NOTA_MT >= 650)')
    .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
    .display()
)

# COMMAND ----------

# Outra sintaxe:
(
    df
    .where((F.col('TP_SEXO') == 'M') & (F.col('NU_NOTA_MT') >= 650))
    .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
    .display()
)

# COMMAND ----------

# Outra sintaxe:
(
    df
    .where((df.TP_SEXO == "M") & (df.NU_NOTA_MT >= 650))
    .select('NU_INSCRICAO', 'TP_SEXO', 'NU_NOTA_MT')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos usar o operador LIKE de forma análoga ao que fizemos no SQL.

# COMMAND ----------

(
    payments.filter(
        F.lower(F.col('payment_type')).like('%pix%')
                   )
    .display()
)


# COMMAND ----------

# MAGIC %md
# MAGIC Para filtrar um dado textual que inicia ou finaliza com um determinado padrão podemos usar as funções `startswith()` ou `endswith()`.

# COMMAND ----------

(
    payments.filter(F.col('payment_type').startswith('pix')).display()
)

(
    payments.filter(F.col('payment_type').endswith('pix')).display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Com o operador `BETWEEN` podemos filtrar um intervalo. Vamos utilizar este comando para filtrar os candidatos cque tenham idade dentro de um determinado intervalo.

# COMMAND ----------

(
    df.filter(F.col('TP_FAIXA_ETARIA').between(1, 3)).display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manipulação de colunas

# COMMAND ----------

# MAGIC %md
# MAGIC Renomear colunas

# COMMAND ----------

# altera o nome de todas as colunas do dF para o formato lower()
new_cols_name = [col.lower() for col in df.columns]
newDF = df.toDF(*new_cols_name)
newDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Para renomear colunas podemos usar o comando `.withColumnRenamed(old_name, new_name)`.

# COMMAND ----------

(
    df
    .withColumnRenamed('NU_INSCRICAO', 'id')
    .withColumnRenamed('NU_ANO', 'ano')
    .withColumnRenamed('TP_SEXO', 'sexo')
    .select('id', 'ano', 'sexo')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Remoção de colunas

# COMMAND ----------

cols_to_drop = [
 'TX_RESPOSTAS_CN',
 'TX_RESPOSTAS_CH',
 'TX_RESPOSTAS_LC',
 'TX_RESPOSTAS_MT',
 'TX_GABARITO_CN',
 'TX_GABARITO_CH',
 'TX_GABARITO_LC',
 'TX_GABARITO_MT',
 'CO_PROVA_CN',
 'CO_PROVA_CH',
 'CO_PROVA_LC',
 'CO_PROVA_MT'
]

df = df.drop(*cols_to_drop)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Criando colunas

# COMMAND ----------

# MAGIC %md
# MAGIC A função `.withColumn()` possibilia a criação de colunas, vejamos alguns exemplos. Com a função `lit()` podemos criar uma coluna com uma constante ou valor literal.

# COMMAND ----------

(
    df
    .withColumn('log_nota_mt', F.log1p('NU_NOTA_MT'))
    .withColumn('flag_enem', F.lit(1))
    .withColumnRenamed('NU_INSCRICAO', 'id_aluno')
    .withColumnRenamed('NU_NOTA_MT', 'nota_mt')
    .select('id_aluno', 'nota_mt', 'log_nota_mt', 'flag_enem')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC A instrução `when()` permite criar uma estrutura `if-else-then` de forma análoga ao SQL. 

# COMMAND ----------

# TP_SEXO
# F - Feminino
# M - Masculino

# TP_ESCOLA
# 1 - Não respondeu
# 2 - Pública
# 3 - Privada

(
    df
    .withColumn('TP_SEXO', F.when(F.col('TP_SEXO') == 'F', 'Feminino')
                .when(F.col('TP_SEXO') == 'M', 'Masculino')
                .when(F.col('TP_SEXO').isNull(), 'missing')
                .otherwise(F.lit('verificar'))
               )
    .withColumn('TP_ESCOLA', F.when(F.col('TP_ESCOLA') == 1, 'Não respondeu')
                .when(F.col('TP_ESCOLA') == 2, 'Pública')
                .when(F.col('TP_ESCOLA') == 3, 'Privada')
                .when(F.col('TP_ESCOLA').isNull(), 'missing')
                .otherwise(F.lit('verificar'))
               )
    .select('NU_INSCRICAO', 'TP_SEXO', 'TP_ESCOLA')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC No SQL temos que:

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   NU_INSCRICAO,
# MAGIC   case
# MAGIC     when TP_SEXO = 'F' then 'Feminino'
# MAGIC     when TP_SEXO = 'M' then 'Masculino'
# MAGIC     when TP_SEXO IS NULL then 'missing'
# MAGIC     else 'verificar'
# MAGIC   end as TP_SEXO,
# MAGIC   case 
# MAGIC     when TP_ESCOLA = 1 then 'Não respondeu'
# MAGIC     when TP_ESCOLA = 2 then 'Pública'
# MAGIC     when TP_ESCOLA = 3 then 'Privada'
# MAGIC     when TP_ESCOLA IS NULL then 'missing'
# MAGIC     else 'verificar'
# MAGIC   end as TP_ESCOLA
# MAGIC from default.dados_enem_2021_ba;

# COMMAND ----------

# MAGIC %md
# MAGIC Outra possibilidade para o CASE WHEN no PySpark seria utilizando a função `expr()`, que permite executar expressões SQL em um DataFrame PySpark. Iremos implementar o CASE WHEN utilizando esta função.

# COMMAND ----------

(
    df
    .select('NU_INSCRICAO', 'TP_SEXO', 'TP_ESCOLA')
    .withColumn('TP_SEXO', F.expr("""
    case
         when TP_SEXO = 'M' then 'Masculino'
         when TP_SEXO = 'F' then 'Feminino'
         when TP_SEXO is null then 'missing'
         else 'verificar' 
    end
    """))
    .withColumn('TP_ESCOLA', F.expr("""
    case 
        when TP_ESCOLA = 1 then 'Não respondeu'
        when TP_ESCOLA = 2 then 'Pública'
        when TP_ESCOLA = 3 then 'Privada'
        when TP_ESCOLA IS NULL then 'missing'
        else 'verificar'
    end
    """))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC O PySpark fornece as funções fillna() e fill() para substituir valores NULL/None.
# MAGIC
# MAGIC * fillna() e fill()
# MAGIC
# MAGIC ```python
# MAGIC fillna(value, subset)
# MAGIC
# MAGIC fill(value, subset)
# MAGIC ```

# COMMAND ----------

(
    df
    .fillna({'NU_NOTA_MT': -1, 'NU_NOTA_CH': -1})
    .select('NU_INSCRICAO', 'NU_NOTA_MT', 'NU_NOTA_CH')
    .display()
)

# COMMAND ----------

(
    df
    .na.fill({'NU_NOTA_MT': -1, 'NU_NOTA_CH': -1})
    .select('NU_INSCRICAO', 'NU_NOTA_MT', 'NU_NOTA_CH')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL no Spark

# COMMAND ----------

# cria uma tabela cujo nome é df_enem
df.createOrReplaceTempView('df_enem')

# COMMAND ----------

newSubset = sqlContext.sql("""
select * from df_enem
where TP_SEXO = 'F'
""")

# COMMAND ----------

newSubset.display()

# COMMAND ----------

type(newSubset)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Duplicidade

# COMMAND ----------

# MAGIC %md
# MAGIC A funçao`.dropDuplicates()` permite remover duplicidade no Spark, enquanto que podemos usar a função `countDistinct()` para identificar duplicidade. Vejamos alguns exemplos.

# COMMAND ----------

(
    df.agg(F.count('NU_INSCRICAO').alias('count'),
           F.countDistinct('NU_INSCRICAO').alias('countDistinct')
          )
    .display()
)

# COMMAND ----------

df.dropDuplicates(subset = ['NU_INSCRICAO']).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Vejamos agora na tabela payments:

# COMMAND ----------

(
    payments.agg(
        F.count('consumer_id').alias('count'),
        F.countDistinct('consumer_id').alias('countDistinct')
    )
    .display()
)

# COMMAND ----------

(
    payments
    .withColumn('anomes', F.date_format('payment_date', 'yyyy-MM'))
    .groupBy('anomes')
    .agg(
        F.count('consumer_id').alias('count'),
        F.countDistinct('consumer_id').alias('countDistinct')
    )
    .display()
)

# COMMAND ----------

paymentsNEW = payments.withColumn('anomes', F.date_format('payment_date', 'yyyy-MM'))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicidade no consumer id:

# COMMAND ----------

display(paymentsNEW.count())

# COMMAND ----------

display(paymentsNEW.dropDuplicates(subset = ['consumer_id']).count())

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicidade no consumer_id, anomes:

# COMMAND ----------

paymentsNEWwithoutDuplicates = paymentsNEW.dropDuplicates(subset = ['consumer_id', 'anomes'])
display(paymentsNEWwithoutDuplicates)

# COMMAND ----------

(
    paymentsNEWwithoutDuplicates
    .groupBy('anomes')
    .agg(
        F.count('consumer_id').alias('count'),
        F.countDistinct('consumer_id').alias('countDistinct')
    )
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Agregação

# COMMAND ----------

# MAGIC %md
# MAGIC O PySpark fornece um conjunto de funções para efetuarmos operaçõres de agregação. Algumas funções:
# MAGIC     
# MAGIC Função|Descrição|
# MAGIC ------|---------|
# MAGIC mean(column)|Média dos valores|
# MAGIC max(column)|Valor máximo|
# MAGIC min(column)|Valor mínimo|
# MAGIC sum(column)|Soma|
# MAGIC avg(column)|Média|
# MAGIC agg(column)|Calcular mais de um valor agregado por vez|
# MAGIC count(column)|Contagem de valores|
# MAGIC countDistinct(column)|Contagem de valores distintos|

# COMMAND ----------

# aplica funções de agregação:

(
    df.select(F.mean('NU_NOTA_MT').alias('avg_nu_nota_mt'),
              F.max('NU_NOTA_MT').alias('max_nu_nota_mt')
             ).display()
)

# COMMAND ----------

# aplica mais de uma função de agregação usando .agg():
(
    df
    .agg(
        F.count('NU_INSCRICAO').alias('QuantidadeInscritos'),
        F.countDistinct('NU_INSCRICAO').alias('QuantidadeInscritosDistintos'),
        F.min('NU_NOTA_MT').alias('min_nota_mt'),
        F.mean(F.col('NU_NOTA_MT')).alias('mean_nota_mt'),
        F.median(F.col('NU_NOTA_MT')).alias('median_nota_mt'),
        F.stddev_samp(F.col('NU_NOTA_MT')).alias('std_nota_mt'),
        F.max(F.col('NU_NOTA_MT')).alias('max_nota_mt') 
    )
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Agrupamento

# COMMAND ----------

# MAGIC %md
# MAGIC De forma parecida com o `GROUP BY` do SQL podemos usar a função `groupBy()` do PySpark para criar grupos e aplicar funções, como soma, máximo, mínimo, por grupos.
# MAGIC
# MAGIC A lógica por trás do Group By é similar ao Pandas, em que usamos o método split()-apply()-combine().
# MAGIC
# MAGIC <img src = 'https://www.w3resource.com/w3r_images/pandas-groupby-split-apply-combine.svg' width = 500/>

# COMMAND ----------

# conta a quantidade de inscritos por sexo
(
    df.groupBy('TP_SEXO')
    .agg(F.count('NU_INSCRICAO'))
    .display( )
)

# outra sintaxe: df.groupBy('TP_SEXO').count().display()

# COMMAND ----------

# estatística descritiva do desempenho em Matemática por gênero e tipo de escola
(
    df
    .filter(F.col('TP_ESCOLA') != 1)
    .groupBy('TP_SEXO', 'TP_ESCOLA')
    .agg(
        F.min('NU_NOTA_MT').alias('min_nota_mt'),
        F.mean('NU_NOTA_MT').alias('mean_nota_mt'),
        F.median('NU_NOTA_MT').alias('median_nota_mt'),
        F.max('NU_NOTA_MT').alias('max_nota_mt')
    )
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ordenação

# COMMAND ----------

# MAGIC %md
# MAGIC O PySpark fornece as funções `.sort()` e `.orderBy()` para ordenar o DataFrame, em forma ascendente ou descendente, por uma ou mais colunas.

# COMMAND ----------

# ordena por uma única coluna
(
    df.select('NU_INSCRICAO', 'NU_NOTA_MT')
    .sort('NU_NOTA_MT')
    .display()
)

# usando orderBy:
# df.select('NU_INSCRICAO', 'NU_NOTA_MT').orderBy('NU_NOTA_MT').display()

# COMMAND ----------

# ordena por mais de uma coluna
(
    df.select('NU_INSCRICAO', 'NU_NOTA_MT', 'NU_NOTA_CH')
    .filter(F.col('NU_NOTA_MT').isNotNull())
    .sort('NU_NOTA_MT', 'NU_NOTA_CH')
    .display()
)

# Usando orderBy:
# (df.select('NU_INSCRICAO', 'NU_NOTA_MT', 'NU_NOTA_CH')
# .filter(F.col('NU_NOTA_MT').isNotNull())
# .orderBy('NU_NOTA_MT', 'NU_NOTA_CH').display())

# COMMAND ----------

# ordena por mais de uma coluna, de forma descendente para Matemática e ascendente para Natureza
(
    df
    .filter(F.col('NU_NOTA_MT').isNotNull())
    .select(['NU_INSCRICAO','NU_NOTA_MT', 'NU_NOTA_CH'])
    .sort(F.desc('NU_NOTA_MT'), F.asc('NU_NOTA_CH'))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos fazer a mesma query anterior mas usando a função `orderBy()`:

# COMMAND ----------

(
    df
    .filter(F.col('NU_NOTA_MT').isNotNull())
    .select(['NU_INSCRICAO','NU_NOTA_MT', 'NU_NOTA_CH'])
    .orderBy(F.desc('NU_NOTA_MT'), F.asc('NU_NOTA_CH'))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Outra forma de usar as funções `asc` e `desc`:

# COMMAND ----------

display(df.sort(F.col('NU_NOTA_MT').asc(), F.col('NU_NOTA_CH').desc()))
#display(df.orderBy(F.col('NU_NOTA_MT').asc(), F.col('NU_NOTA_CH').desc())) 

# COMMAND ----------

# MAGIC %md
# MAGIC Com o que vimos até agora vamos criar uma visão município para a base, que seria criar agregações para o nível municipal.

# COMMAND ----------

(
    df.groupBy('NO_MUNICIPIO_PROVA')
    .agg(
        F.count('NU_INSCRICAO').alias('quantidade_inscritos'),
        F.mean('NU_NOTA_MT').alias('avg_nota_mt'),
        F.mean('NU_NOTA_CH').alias('avg_nota_ch'),
        F.mean('NU_NOTA_LC').alias('avg_nota_lc'),
        F.mean('NU_NOTA_CN').alias('avg_nota_cn')
    )
    .orderBy(F.col('quantidade_inscritos').desc(), F.col('avg_nota_mt').desc())
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Além das funções `asc` e `desc` temos ainda as funções :
# MAGIC
# MAGIC Função|Descrição|
# MAGIC ------|---------|
# MAGIC asc_nulls_first(column)|Similar com a função asc mas primeiro retorna os valores nulos e seguidamente os valores não-nulos
# MAGIC asc_nulls_last(column)|Similar com a função asc mas primeiro retorna os valores não-nulos e em seguida os valores nulos
# MAGIC desc_nulls_first(column)|Similar com a função desc mas retorna primeiro os valores nulos e seguidamente os valores não-nulos
# MAGIC desc_nulls_last(column)|Similar com a função desc mas retorna primeiro os valores não-nulos e seguidamente os valores nulos

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# exemplo - asc_nulls_first
(df
 .select('NU_INSCRICAO', 'NU_NOTA_MT')
 .sort(F.asc_nulls_first('NU_NOTA_MT')).display())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualização de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos criar cubos e a partir destes criar visualizações de Dados no DataBricks.

# COMMAND ----------

dfNew = (df
 .withColumn(
    'TP_SEXO', 
    F.when(F.col('TP_SEXO') == 'F', 'Feminino')
    .when(F.col('TP_SEXO') == 'M', 'Masculino')
    .when(F.col('TP_SEXO').isNull(), 'missing')
    .otherwise('Verificar'))
 .withColumn('TP_ESCOLA', 
            F.when(F.col('TP_ESCOLA') == 1, 'Não respondeu')
             .when(F.col('TP_ESCOLA') == 2, 'Pública')
             .when(F.col('TP_ESCOLA') == 3, 'Privada')
             .when(F.col('TP_ESCOLA').isNull(), 'missing')
             .otherwise('Verificar')
            )
)

# COMMAND ----------

(
    dfNew
    .filter(F.col('NU_ANO') == 2021)
    .withColumnRenamed('TP_SEXO', 'Sexo')
    .groupBy('Sexo')
    .agg(F.count('NU_INSCRICAO').alias('Quantidade')
        )
    .display()
)

# COMMAND ----------

(
    dfNew
    .groupBy('TP_SEXO', 'TP_ESCOLA')
    .agg(F.count(F.col('NU_INSCRICAO')).alias('Quantidade inscritos'))
    .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   NO_MUNICIPIO_PROVA,
# MAGIC   MAX(NU_NOTA_MT)
# MAGIC from default.dados_enem_2021_ba
# MAGIC where NU_NOTA_MT IS NOT NULL AND NU_NOTA_MT != 0 
# MAGIC group by 1
# MAGIC order by 2 DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join

# COMMAND ----------

# MAGIC %md
# MAGIC Usamo `JOIN` para fazer junção de dados de duas ou mais tabelas, a partir de uma coluna em comum entre que estas tabelas compartilhem.
# MAGIC
# MAGIC É muito comum buscarmos informações em várias tabelas distintas, para a produção de relatórios, dashboards e modelagem.
# MAGIC
# MAGIC
# MAGIC Diferentes tipos de JOINS:
# MAGIC
# MAGIC * INNER JOIN: retorna os registros com intersecção em ambas as tabelas. 
# MAGIC * LEFT JOIN: retorna todos os registros da tabela da esquerda e os registros em comum com a tabela da direta.
# MAGIC * RIGHT JOIN: retorna todos os registros da tabela da direita e os registros em comum com a tabela da esquerda.
# MAGIC * FULL JOIN: retorna todos os registros quando existe correspondência na tabela da esquerda ou da direita.
# MAGIC
# MAGIC <img src = 'https://www.w3schools.com/sql/img_innerjoin.gif' />
# MAGIC
# MAGIC <img src = 'https://www.w3schools.com/sql/img_leftjoin.gif' />
# MAGIC
# MAGIC <img src = 'https://www.w3schools.com/sql/img_rightjoin.gif' />
# MAGIC
# MAGIC <img src = 'https://www.w3schools.com/sql/img_fulljoin.gif' />

# COMMAND ----------

df_joined = (df.join(df_ba_quest, ['NU_INSCRICAO'], 'inner')
            )


display(df_joined)
display(df.count())
display(df_joined.agg(F.count('NU_INSCRICAO').alias('count'), 
                      F.countDistinct('NU_INSCRICAO').alias('countDistinct')
                     )
       )

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos simular um caso de uso do `left join`, para tanto vamos fazer uma amostra aleatória da nossa base de questões do ENEM e selecionar apenas algumas colunas. Note então que para a base amostral teremos um subset menor de candidatos. Ao fazer um left join da base `df` com a base amostral não teremos retorno das informações do questionário sócioeconômico para todos candidatos que estão na tabela `df`.
# MAGIC Resumindo, iremos trazer todos os registros da nossa table da esquerda `df` e os registros em comum com a tabela da direita `df_ba_quest_sample`.

# COMMAND ----------

df_subset          = df.select('NU_INSCRICAO', 'NU_ANO', 'TP_SEXO', 'NU_NOTA_MT')

df_ba_quest_sample = (df_ba_quest.sample(fraction = 0.6, seed = 100)
                      .select('NU_INSCRICAO', 'Q001', 'Q002', 'Q006')
                      .withColumnRenamed('Q001', 'escolaridade_pai')
                      .withColumnRenamed('Q002', 'escolaridade_mae')
                      .withColumnRenamed('Q006', 'faixa_renda_familiar')
                      .withColumn('NU_ANO', F.lit(2021))
                     )
# volumetria das bases
print(df_subset.count())
print(df_ba_quest_sample.count())

# COMMAND ----------

# JOIN

df_subset_joined = (df_subset.join(df_ba_quest_sample, 
          ['NU_INSCRICAO'],
          'left'
         ))
    
# MISSING SIMBÓLICO : -2 representa o missing oriundo do join
# MISSING SIMBÓLICO : -1 representa o candidato que não possui informação da nota

df_subset_joined = (df_subset_joined
         .na.fill(-1, subset = ['NU_NOTA_MT'])
         .na.fill('missing', subset = ['escolaridade_pai', 'escolaridade_mae', 'faixa_renda_familiar'])
)

display(df_subset_joined)
# conta volumetria da base pós join
display(df_subset_joined.agg(F.count('NU_INSCRICAO'), F.countDistinct('NU_INSCRICAO')))

# COMMAND ----------

# outra sintaxe para o Join:
(df_subset.join(df_ba_quest_sample, [df_subset.NU_INSCRICAO == df_ba_quest_sample.NU_INSCRICAO], 'left')
 .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos simular mais uma situação que poderia ocorrer, quando por exemplo tivermos mais de uma chave de cruzamento entre as tabelas, por meio do id e do ano de realização do Exame.

# COMMAND ----------

# JOIN

df_subset_joined = (df_subset.join(df_ba_quest_sample, 
          ['NU_INSCRICAO', 'NU_ANO'],
          'left'
         ))
    
# MISSING SIMBÓLICO : -2 representa o missing oriundo do join
# MISSING SIMBÓLICO : -1 representa o candidato que não possui informação da nota

df_subset_joined = (df_subset_joined
         .na.fill(-1, subset = ['NU_NOTA_MT'])
         .na.fill('missing', subset = ['escolaridade_pai', 'escolaridade_mae', 'faixa_renda_familiar'])
)

display(df_subset_joined)
# conta volumetria da base pós join
display(df_subset_joined.agg(F.count('NU_INSCRICAO'), F.countDistinct('NU_INSCRICAO')))

# COMMAND ----------

# outra sintaxe possível:
(df_subset.join(df_ba_quest_sample, 
                [df_subset.NU_INSCRICAO == df_ba_quest_sample.NU_INSCRICAO,
                 df_subset.NU_ANO == df_ba_quest_sample.NU_ANO
                ],
                'left'
               )
 .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pivot e concat

# COMMAND ----------

# MAGIC %md
# MAGIC O comando `pivot`  é uma forma de fazer uma agregação horizontal dos dados, permitindo que você analise e visualize os dados de uma maneira mais fácil e flat.
# MAGIC
# MAGIC No primeiro exemplo vamos usar este conceito para gerar uma agregação para os nossos dados, de modo que vamos contar a quantidade de inscritos no ENEM por sexo e se o candidato é ou não treineiro. Resumidamente o que vamos fazer é :
# MAGIC
# MAGIC - Agrupar o DataFrame pela coluna `TP_SEXO`.
# MAGIC - Aplicar o comando `pivot` para criar novas colunas no DataFrame para os valores únicos encontrados na coluna `IN_TREINEIRO`.
# MAGIC - A função `agg` é aplicada para agregar o número de inscrições para cada combinação de `TP_SEXO` e `IN_TREINEIRO`.

# COMMAND ----------

(
    df
    .withColumn('IN_TREINEIRO', F.when(F.col('IN_TREINEIRO') == 0, 'NÃO')
                .when(F.col('IN_TREINEIRO') == 1, 'SIM')
               )
    .groupBy('TP_SEXO')
    .pivot('IN_TREINEIRO')
    .agg(F.count('NU_INSCRICAO'))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos contar a quantidade de inscritos em cada faixa de renda e média em matemática:

# COMMAND ----------

df_pivot = (
    df.join(df_ba_quest, ['NU_INSCRICAO'], 'inner')
    .withColumnRenamed('Q006', 'renda_familiar')
    .groupBy('NO_MUNICIPIO_PROVA')
    .pivot('renda_familiar')
    .agg(
        F.count('NU_INSCRICAO')
    )
    .na.fill(-1)
)
display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos melhorar a análise anterior e diminuir as faixas de renda, criando 4 faixas:
# MAGIC * Nenhuma renda e até R$ 1.650 (A, B, C)
# MAGIC * Entre R$ 1.650,01 e R$ 3.300,00 (D, E, F)
# MAGIC * Entre R$ 3.300,01 e R$ 9.900,00 (G, H, I, J, K)
# MAGIC * Acima de R$ 9.900,01 (M, N, O, P, Q)

# COMMAND ----------

# join
df_joined = (df.join(df_ba_quest, ['NU_INSCRICAO'], 'inner'))

# Novo agrupamento de faixas de renda
df_joined = (
    df_joined
    .withColumn('renda', F.when(F.col('Q006').isin(['A', 'B', 'C']), 'A')
                .when(F.col('Q006').isin(['D', 'E', 'F']), 'B')
                .when(F.col('Q006').isin(['G', 'H', 'I', 'J', 'K']), 'C')
                .when(F.col('Q006').isin(['M', 'N', 'O', 'P', 'Q']), 'D')
                .when(F.col('Q006').isNull(), 'missing')
               )
)

# pivot table

df_pivot = (
    df_joined
    .groupBy('NO_MUNICIPIO_PROVA')
    .pivot('renda')
    .agg(F.count('NU_INSCRICAO'))
)


# missing imputation

df_pivot = df_pivot.na.fill(-1)

display(df_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC Agora criaremos uma visão diferente. Vamos ver a nota média por município em Matemática por gênero.

# COMMAND ----------

(df_joined
 .groupBy('NO_MUNICIPIO_PROVA')
 .pivot('TP_SEXO')
 .agg(F.mean(F.col('NU_NOTA_MT')))
 .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC A função `union()` pode ser usada para concatenar DataFrames que possuem a mesma estrutura/schema.
# MAGIC
# MAGIC Caso o schema seja diferente, teremos erro.

# COMMAND ----------

df_concat = (
    df.union(df_sp)
)

# COMMAND ----------

df_aux= df_concat.union(df)

# COMMAND ----------

# o método union() retorna linhas com duplicidade
df_concat.count(), df_aux.count()

# COMMAND ----------

# remove a duplicidade
(
    df_concat.union(df).distinct().count()
)

# COMMAND ----------

df_concat.count()

# COMMAND ----------

df_aux.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDF

# COMMAND ----------

# MAGIC %md
# MAGIC UDF (User-Defined Function) é uma função definida pelo usuário que pode ser utilizada no PySpark para manipulação de dados em colunas de um DataFrame.
# MAGIC
# MAGIC Os UDFs são úteis quando precisamos aplicar uma operação personalizada em uma ou mais colunas de um DataFrame e essa operação não pode ser realizada com as funções padrão do PySpark.
# MAGIC
# MAGIC Para criar um UDF no PySpark, podemos utilizar a função udf do módulo pyspark.sql.functions. 
# MAGIC
# MAGIC Por exemplo, para criar um UDF que retorna o quadrado de um número, podemos fazer o seguinte:
# MAGIC     
# MAGIC ```python
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import DoubleType
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC def square_udf(x):
# MAGIC     return x**2
# MAGIC
# MAGIC square_udf = udf(square_udf, DoubleType())
# MAGIC
# MAGIC df = df.withColumn("square_value", square_udf(col("value")))
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Resumidamente temos:
# MAGIC
# MAGIC
# MAGIC - Importação dos módulos `udf` e `DoubleType` do pacote `pyspark.sql.functions`.
# MAGIC - Importação do módulo `col` do pacote `pyspark.sql.functions`.
# MAGIC - Definição de uma função `square_udf` que recebe um valor `x` como parâmetro e retorna o valor de `x` elevado ao quadrado.
# MAGIC - Criação de um objeto UDF com a função `udf` e o tipo de retorno `DoubleType()`.
# MAGIC - Criação de uma nova coluna no DataFrame `df` com a função `withColumn`, onde a nova coluna é chamada de "square_value" e seu valor é definido como o resultado da aplicação do UDF `square_udf` na coluna "value" do DataFrame `df`.

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos criar um exemplo prático nos dados do ENEM.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def nota_final(nota):
    if nota is None:
        return "missing"
    elif nota > 600:
        return "Aprovado"
    else:
        return "Reprovado"

nota_final_udf = udf(nota_final, StringType())

df = df.withColumn("nota_final", nota_final_udf(F.col("NU_NOTA_MT")))

# COMMAND ----------

display(df.select('NU_INSCRICAO', 'NU_NOTA_MT', 'nota_final'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### expr()

# COMMAND ----------

# MAGIC %md
# MAGIC * `expr()` que permite executar expressões SQL em um DataFrame PySpark. Já vimos anterior como fazer um CASE WHEN com esta função, vejamos agora outras possibilidades.

# COMMAND ----------

# cria uma nova coluna a partir do concat de colunas existentes
(df.withColumn('NO_CIDADE_UF_PROVA', F.expr("NO_MUNICIPIO_PROVA || '-' || SG_UF_PROVA"))
 .select('NU_INSCRICAO', 'NO_CIDADE_UF_PROVA')
 .display()
)

# outra sintaxe: df.withColumn('NO_CIDADE_UF_PROVA', F.expr("CONCAT(NO_MUNICIPIO_PROVA,'-', SG_UF_PROVA)")

# COMMAND ----------

payments_copy = (payments
# adiciona 1 mês
 .withColumn('payment_date_3m', F.expr("add_months(payment_date, 3)"))
# cast 
 .withColumn('payment_date_cast', F.expr('cast(payment_date as date)'))
# operação 
 .withColumn('log_payment_amount', F.expr('log(payment_amount)'))
# filtro da tabela
 .filter(F.expr('consumer_id == 524612336731'))
)

display(payments_copy)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manipulação de datas

# COMMAND ----------

# MAGIC %md
# MAGIC Principais funções para trabalhar com datas no SQL:
# MAGIC
# MAGIC     
# MAGIC Função|Descrição|    
# MAGIC ------|---------|
# MAGIC date(expr)|Casts the value expr to DATE.
# MAGIC date_add(startDate, numDays)|Returns the date numDays after startDate.
# MAGIC add_months(startDate, numMonths)|Returns the date that is numMonths after startDate.
# MAGIC date_part(field, exp)| Extracts a part of the date, timestamp, or interval.
# MAGIC date_format(expr, fmt)| Converts a timestamp to a string in the format fmt.
# MAGIC date_sub(startDate, numDays)|Returns the date numDays before startDate.
# MAGIC dateadd(startDate, numDays)|Returns the date numDays after startDate.
# MAGIC dateadd(unit, value, expr)|Adds value units to a timestamp expr.
# MAGIC datediff(enDate, startDate)|Returns the number of days from startDate to endDate.
# MAGIC datediff(unit, start, stop) | Returns the difference between two timestamps measured in units.
# MAGIC day(expr)|Returns the day of month of the date or timestamp.
# MAGIC dayofmonth(expr)|Returns the day of month of the date or timestamp.
# MAGIC dayofweek(expr)|Returns the day of week of the date or timestamp.
# MAGIC dayofyear(expr)|Returns the day of year of the date or timestamp.
# MAGIC hour(expr)|Returns the hour component of a timestamp.
# MAGIC minute(expr)|Returns the minute component of the timestamp in expr.
# MAGIC second(expr)|Returns the second component of the timestamp in expr.
# MAGIC month(expr)|Returns the month component of the timestamp in expr.
# MAGIC months_between(expr1, expr2)|Returns the number of months elapsed between dates or timestamps in expr1 and expr2.
# MAGIC weekday(expr)|Returns the day of the week of expr.
# MAGIC weekofyear(expr)|Returns the week of the year of expr.
# MAGIC year(expr)|Returns the year component of expr.
# MAGIC now()|Returns the current timestamp at the start of query evaluation.
# MAGIC quarter(expr)|Returns the quarter of the year for expr in the range 1 to 4.
# MAGIC timestamp(expr)|Casts expr to TIMESTAMP.
# MAGIC to_date(expr, [,fmt])|Returns expr cast to a date using an optional formatting.
# MAGIC to_timestamp(expr, [,fmt])|Returns expr cast to a timestamp using an optional formatting.

# COMMAND ----------

# MAGIC %md
# MAGIC Vejamos um exemplo de uso das funções de data:

# COMMAND ----------

(
    payments
    .withColumn('current_date', F.current_date())
    .withColumn('anomes', F.date_format(F.col('payment_date'), 'yyyy-MM'))
    .withColumn('year', F.year(F.col('payment_date')))
    .withColumn('month', F.month(F.col('payment_date')))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manipulação de texto

# COMMAND ----------

# MAGIC %md
# MAGIC Principais funções para trabalhar com texto:
# MAGIC
# MAGIC Função|Descrição|
# MAGIC ------|---------|
# MAGIC concat(expr1, expr2)|Concatena duas strings
# MAGIC contains(expr, SubExpr)|Retorna true se a string expr possui o padrão contido na string SubExpr
# MAGIC startswith(expr, startExpr)|Returns true if expr STRING or BINARY starts with startExpr.
# MAGIC endswith(expr,endExpr)| Returns true if expr STRING or BINARY ends with endExpr.
# MAGIC length(expr)| Returns the character length of string data or number of bytes of binary data.
# MAGIC lower(expr)|Returns expr with all characters changed to lowercase.
# MAGIC upper(expr)|Returns expr with all characters changed to uppercase.
# MAGIC replace(str, search [, replace])|Replaces all occurrences of search with replace.
# MAGIC reverse(expr)|Returns a reversed string or an array with reverse order of elements.
# MAGIC substr(expr, pos, [, len])|Returns the substring of expr that starts at pos and is of length len.
# MAGIC to_char()|Returns numExpr cast to STRING using formatting fmt.”

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos usar esses comandos para tratar a tabela payments e criar uma tabela a ser consumida pelo time de Modelagem e Analytics:

# COMMAND ----------

payments_new = (
    payments
    .withColumn('payment_amount', F.expr("cast(replace(payment_amount, 'R$ ', '') AS float)"))
    .withColumn('flag_pix', F.when(F.lower('payment_type').contains('pix') == True, 1)
                           .otherwise(0)
                )
    .withColumn('flag_debit', F.when(F.lower('payment_type').contains('debit') == True, 1)
                              .otherwise(0)
               )
    .withColumn('flag_credit', F.when(F.lower('payment_type').contains('credit') == True, 1)
                              .otherwise(0)
               )
    .withColumn('flag_boleto', F.when(F.lower('payment_type').contains('boleto') == True, 1)
                              .otherwise(0)
               )
    .withColumn('payment_category', F.when(F.lower(F.col('payment_type')).like('%pix%'), 'pix')
                .when(F.lower(F.col('payment_type')).like('%boleto%'), 'boleto')
                .when(F.lower(F.col('payment_type')).like('%credit%'), 'credit')
                .when(F.lower(F.col('payment_type')).like('%debit%'), 'debit')
                .when(F.col('payment_type').isNull(), 'missing')
                .otherwise('verificar')
               )
    .withColumn('year',  F.substring('payment_date', 1, 4))
    .withColumn('month', F.substring('payment_date', 6, 2))
    .withColumn('day',   F.substring('payment_date', 9, 2))
    .withColumn('year_month', F.concat(F.col('year'), F.lit('-'), F.col('month')))
    .withColumnRenamed('payment_type', 'payment_description')
)

display(payments_new)
