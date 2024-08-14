# Databricks notebook source
# MAGIC %md
# MAGIC # Spark-SQL
# MAGIC * * *
# MAGIC Neste notebook passaremos pelos principais comandos da linguagem de consulta estruturada “SQL” utilizando uma das APIs altas do Apache Spark na plataforma unificada de dados do databricks

# COMMAND ----------

# MAGIC %md
# MAGIC <img src = 'https://www.databricks.com/wp-content/uploads/2019/03/gloss-spark1.png'  width="400" height="100" />

# COMMAND ----------

# MAGIC %md
# MAGIC ### O que é databricks?
# MAGIC * O databricks se intitula como uma plataforma unificada de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>Arquitectura de alto nivel do <a href="https://learn.microsoft.com/pt-br/azure/databricks/getting-started/overview" target="_blank" rel="databricks">databricks</a>.</p>
# MAGIC
# MAGIC <img src = 'https://learn.microsoft.com/pt-br/azure/databricks/_static/images/getting-started/databricks-architecture-azure.png'  width="800" height="300" />
# MAGIC
# MAGIC <p>Mais detalhes do <a href="https://azure.microsoft.com/pt-br/products/databricks" target="_blank" rel="azure databricks">azure databricks</a>.</p> 

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é SQL?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC * Um Banco de Dados é uma estrutura arquitetada para armazenar e manipular dados (inclusive para grandes volumes de dados), de modo que as tabelas podem ou não se relacionar.
# MAGIC * Como o SQL pode ser útil para um profissional Dados?
# MAGIC     * Geralmente os dados em um banco relacional serão o principal insumo para a criação de dashboards, relatórios e projetos de Data Science (podemos fazer consultas e trazer os dados para o Pandas, e então limpar e analisar os dados, gerar visualizações e construir modelos de Machine Learning).
# MAGIC * SQL (Structured Query Language ou Linguagem de Consulta Estrutura) é a linguagem padrão para administrar e consultar banco de dados relacional.
# MAGIC * Neste sentido, o profissional de Dados precisa construir consultas (query) sólidas e eficientes para acessar os dados da empresa em que estiver inserido, para então criar projetos de dados e agregar valor para a empresa.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subconjuntos do SQL

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Podemos dividir a linguagem SQl de acordo com as operações realizadas no banco de dados, vejamos os principais comandos:
# MAGIC
# MAGIC DML - Linguagem de Manipulação de Dados
# MAGIC
# MAGIC * INSERT (inserção de um registro)
# MAGIC * UPDATE (atualização de valores)
# MAGIC * DELETE (remoção de linhas)
# MAGIC
# MAGIC DDL - Linguagem de Definição de Dados
# MAGIC
# MAGIC * CREATE (criar objetos no banco de dados, tabela por exemplo)
# MAGIC * DROP (apagar objetos no banco de dados, tabela por exemplo)
# MAGIC * ALTER (alterar um objeto no banco de dados, adicionar uma coluna em uma tabela)
# MAGIC
# MAGIC DCL - Linguagem de Controle de Dados
# MAGIC
# MAGIC * GRANT (autoriza o usuário a realizar operações)
# MAGIC * REVOKE (remove/restringe as operações que um usuário pode realizar)
# MAGIC
# MAGIC DTL - Linguagem de Transação de Dados
# MAGIC
# MAGIC * COMMIT (finaliza uma transação)
# MAGIC * ROLLBACK (descarta mudanças desde o último COMMIT ou ROLLBACK)
# MAGIC
# MAGIC
# MAGIC DQL - Linguagem de Consulta de Dados
# MAGIC
# MAGIC * SELECT (realiza consultas)
# MAGIC
# MAGIC Resumindo, podemos dizer que os principais comandos são:
# MAGIC
# MAGIC * SELECT
# MAGIC * INSERT
# MAGIC * UPDATE
# MAGIC * DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos DDL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

# MAGIC %md
# MAGIC O comando `DROP` é responsável pela deleção da tabela ou banco de dados. Sintaxe:
# MAGIC
# MAGIC ```sql
# MAGIC DROP DATABASE DB1;
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC DROP TABLE DB1;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sandbox.dados_enem_2021_sp;

# COMMAND ----------

# MAGIC %md
# MAGIC O comando `CASCADE` força a deleção do banco caso ele tenha tabelas com dados:

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP DATABASE IF EXISTS sandbox CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create

# COMMAND ----------

# MAGIC %md
# MAGIC O comando `CREATE` é responsável pela criação de bancos ou tabelas. Sintaxe:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE DATABASE DB1;
# MAGIC ```
# MAGIC ou 
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE TB1;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sandbox;

# COMMAND ----------

# MAGIC %sql describe database sandbox

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sandbox.dados_enem_2021_sp USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_sp.csv", encoding "UTF8" ,header "true", inferSchema "true");
# MAGIC CREATE TABLE sandbox.tb_payments USING csv OPTIONS (path "/FileStore/tables/tb_payments.csv", encoding "UTF8", header "true", inferSchema "true");
# MAGIC CREATE TABLE sandbox.dados_enem_2021_ba USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_ba.csv", encoding "UTF8", header "true", inferSchema "true");
# MAGIC CREATE TABLE sandbox.dados_enem_2021_sp_quest USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_sp_quest.csv", encoding "UTF8", header "true", inferSchema "true");
# MAGIC CREATE TABLE sandbox.dados_enem_2021_ba_quest USING csv OPTIONS (path "/FileStore/tables/dados_enem_2021_ba_quest.csv", encoding "UTF8", header "true", inferSchema "true");

# COMMAND ----------

# MAGIC %md
# MAGIC Note que ao contrário dos bancos de dados relacionais não precisamos inferir um schema dizendo o tipo do dado.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE sandbox.dados_enem_2021_sp;

# COMMAND ----------

# MAGIC %md
# MAGIC Note que as nossas tabelas estão virtualizadas como o hive, impala ou polybase mais no final das contas estamos lendo um csv como mostra o campo location abaixo:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sandbox.dados_enem_2021_sp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos DML

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert

# COMMAND ----------

# MAGIC %sql select * from sandbox.tb_payments limit 3

# COMMAND ----------

# MAGIC %md Abaixo vamos inserir uma nova linha na tabela tb_payments

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sandbox.tb_payments 
# MAGIC (
# MAGIC     consumer_id,
# MAGIC     payment_type,
# MAGIC     payment_date,
# MAGIC     payment_amount
# MAGIC )
# MAGIC VALUES(
# MAGIC     123,
# MAGIC     'crédito teste',
# MAGIC     now(),
# MAGIC     'R$ 2'
# MAGIC )

# COMMAND ----------

# MAGIC %md Tentando novamente com uma tabela delta

# COMMAND ----------

df_query = spark.sql("select * from sandbox.tb_payments") 
df_query.write \
        .mode("overwrite") \
        .format("delta") \
        .option("path","/FileStore/tables/tb_payments_delta") \
        .option("mergeSchema","True") \
        .saveAsTable('sandbox.tb_payments_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sandbox.tb_payments_delta 
# MAGIC (
# MAGIC     consumer_id,
# MAGIC     payment_type,
# MAGIC     payment_date,
# MAGIC     payment_amount
# MAGIC )
# MAGIC VALUES(
# MAGIC     123,
# MAGIC     'crédito insert',
# MAGIC     now(),
# MAGIC     'R$ 2'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.tb_payments_delta WHERE consumer_id = 123

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE sandbox.tb_payments_delta 
# MAGIC SET payment_type = 'teste aula'
# MAGIC WHERE consumer_id = 123

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.tb_payments_delta WHERE consumer_id = 123

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE sandbox.tb_payments_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM sandbox.tb_payments_delta WHERE consumer_id = 123

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sandbox.tb_payments_delta --WHERE consumer_id = 123

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos DQL

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos aprender vários comandos SQL para buscar dados na tabela que criamos anteriormente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select

# COMMAND ----------

# MAGIC %md
# MAGIC Usado para selecionar colunas específicas:
# MAGIC     
# MAGIC ```sql
# MAGIC SELECT column_1, column_2, ..., column_n
# MAGIC FROM my_table;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Para selecionar todas as colunas:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM my_table;
# MAGIC ````

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM sandbox.dados_enem_2021_ba;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   NU_INSCRICAO, 
# MAGIC   NU_ANO,
# MAGIC   TP_SEXO
# MAGIC FROM sandbox.dados_enem_2021_ba;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtro

# COMMAND ----------

# MAGIC %md
# MAGIC E se precisarmos fazer algum filtro? Por exemplo, filtrar a base de acordo com alguma variável.
# MAGIC
# MAGIC Em termos genéricos temos a seguinte sintaxe para cláusula WHERE:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT column_1, column_2, column_n
# MAGIC FROM my_table
# MAGIC WHERE condition_1
# MAGIC     AND/OR condition_2
# MAGIC     AND/OR condition_3;
# MAGIC ```
# MAGIC
# MAGIC Sintaxe do operador AND:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT column_1, ..., column_n
# MAGIC FROM my_table
# MAGIC WHERE
# MAGIC     condition_1 AND condition_2 AND condition_3;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Sintaxe do operador OR:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * column_1, ..., column_n
# MAGIC FROM my_table
# MAGIC WHERE
# MAGIC     condition_1 OR condition_2 OR condition_3;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Para negar uma condição podemos utilizar o operador NOT:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM my_table
# MAGIC WHERE
# MAGIC     NOT condition;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Lembre-se que:
# MAGIC
# MAGIC * O operator AND retorna o registro se todas as condições separadas por AND são TRUE;
# MAGIC * O operador OR retorna o registro de pelo menos uma das condições separadas por OR for TRUE.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, TP_ENSINO
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE 
# MAGIC     TP_SEXO = 'F'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, TP_ENSINO
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE 
# MAGIC     NOT TP_SEXO = 'M'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Retorna apenas candidatos do sexo feminino, que fizeram prova em Salvador:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   NU_INSCRICAO, TP_SEXO, TP_ENSINO, NO_MUNICIPIO_PROVA
# MAGIC FROM  sandbox.dados_enem_2021_ba
# MAGIC WHERE 
# MAGIC     TP_SEXO = 'F' 
# MAGIC     AND NO_MUNICIPIO_PROVA = 'Salvador'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md 
# MAGIC Retorna apenas candidatos do sexo feminino, que fizeram prova em Salvador (usando o operador IN)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, TP_ENSINO, NO_MUNICIPIO_PROVA
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE 
# MAGIC     TP_SEXO NOT IN  ('M') 
# MAGIC     AND NO_MUNICIPIO_PROVA IN ('Salvador')
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Retorna apenas candidatos do sexo feminino, que fizeram prova em Salvador ou Feira de Santana

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, TP_ENSINO, NO_MUNICIPIO_PROVA
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE 
# MAGIC     (NO_MUNICIPIO_PROVA = 'Salvador' OR NO_MUNICIPIO_PROVA = 'Feira de Santana') 
# MAGIC     AND TP_SEXO = 'F'
# MAGIC --LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Filtra candidatos que possuem nota em matemática maior que 700 e fizeram prova em Salvador

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, NU_NOTA_MT, NO_MUNICIPIO_PROVA
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE
# MAGIC     NU_NOTA_MT >= 700
# MAGIC     AND NO_MUNICIPIO_PROVA = 'Salvador'
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC * Between

# COMMAND ----------

# MAGIC %md
# MAGIC O operador `BETWEEN` seleciona valores dentre de um range (numérico, textual ou de temporal). Neste operador, os valores inicial e final são incluídos.
# MAGIC
# MAGIC Sintaxe operador `BETWEEN`:
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT column_1
# MAGIC FROM my_table
# MAGIC WHERE column_1 BETWEEN value_1 AND value_2;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Seleciona registros cujos alunos tenham nota em Matemática entre 400 e 700

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, NU_NOTA_MT
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE NU_NOTA_MT BETWEEN 400 AND 700;

# COMMAND ----------

# MAGIC %md
# MAGIC Para filtrar dados nulos podemos usar IS NULL/IS NOT NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, NU_NOTA_MT FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE NU_NOTA_MT IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct

# COMMAND ----------

# MAGIC %md
# MAGIC O comando `SELECT DISTINCT` retorna apenas os valores únicos, equivalante ao comando `.unique()` do Pandas ou ainda `.distinct()` do PySpark. Sintaxe:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT DISTINCT column
# MAGIC FROM my_table;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Dados distintos da coluna NO_MUNICIPIO_PROVA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT NO_MUNICIPIO_PROVA
# MAGIC FROM sandbox.dados_enem_2021_ba;

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos verificar duplicidade na base payments:

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   count(consumer_id),
# MAGIC   count(distinct consumer_id)
# MAGIC from sandbox.tb_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC Precisamos entender então a natureza desta duplicidade, que neste caso refere-se a clientes que efetuaram mais de 1 pagamento.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from sandbox.tb_payments
# MAGIC where consumer_id = 524612336731;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ordenação

# COMMAND ----------

# MAGIC %md
# MAGIC O comando `ORDER BY` é utilizado para ordernar, em ordem ascendente ou descendente, os da consulta.
# MAGIC
# MAGIC O default é ordenar os resultado na ordem ascendente. Sintaxe:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT column_1, column_2
# MAGIC FROM my_table
# MAGIC ORDER BY column_1, column_2 ASC|DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NU_INSCRICAO, TP_SEXO, NU_NOTA_MT, NU_NOTA_CN
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE
# MAGIC     NO_MUNICIPIO_PROVA = 'Salvador'
# MAGIC ORDER BY NU_NOTA_MT DESC, NU_NOTA_CN DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agregação

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos aprender a usar funções para resumir os dados. Por exemplo, obter o valor mínimo, máximo ou a média de uma variável.
# MAGIC
# MAGIC Funções MIN() e MAX()
# MAGIC
# MAGIC
# MAGIC * A função MIN() retorna o menor valor de uma coluna selecionada.
# MAGIC * A função MAX() retorna o maior valor de uma coluna selecionada.
# MAGIC
# MAGIC Sintaxe MIN
# MAGIC
# MAGIC ```sql
# MAGIC SELECT MIN(column)
# MAGIC FROM my_table
# MAGIC WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
# MAGIC Sintaxe MAX
# MAGIC
# MAGIC ```sql
# MAGIC SELECT MAX(column)
# MAGIC FROM my_table
# MAGIC WHERE condition;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Nota mínima e máxima de matemática

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     MIN(NU_NOTA_MT), MAX(NU_NOTA_MT)
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE
# MAGIC     NU_NOTA_MT != 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Nota mínima e máxima de matemática e ciências da natureza

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     MIN(NU_NOTA_MT) as min_nota_mt, 
# MAGIC     MAX(NU_NOTA_MT) as max_nota_mt,
# MAGIC     MIN(NU_NOTA_CN) as min_nota_cn,
# MAGIC     MAX(NU_NOTA_CN) as max_nota_cn
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE
# MAGIC     NU_NOTA_MT != 0
# MAGIC     AND NU_NOTA_CN != 0 
# MAGIC     AND NO_MUNICIPIO_PROVA = 'Salvador';

# COMMAND ----------

# MAGIC %md
# MAGIC COUNT(), AVG(), SUM()

# COMMAND ----------

# MAGIC %md
# MAGIC * A função COUNT() retorna o número de linhas que corresponde a um critério especificado;
# MAGIC * A função AVG() retorna o valor médio de uma coluna numérica;
# MAGIC * A função SUM() retorna a soma de uma coluna numérica.
# MAGIC
# MAGIC
# MAGIC Sintaxe COUNT
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(column)
# MAGIC FROM my_table
# MAGIC WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC Sintaxe AVG
# MAGIC
# MAGIC ```sql
# MAGIC SELECT AVG(column)
# MAGIC FROM my_table
# MAGIC WHERE condition;
# MAGIC ```
# MAGIC
# MAGIC Sintaxe SUM
# MAGIC
# MAGIC ```sql
# MAGIC SELECT SUM(column)
# MAGIC FROM my_table
# MAGIC WHERE condition;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC * Sumário descritivo da nota de matemática

# COMMAND ----------

# MAGIC %md
# MAGIC Nota mínima e máxima de matemática

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     MIN(NU_NOTA_MT) as min_nota_mt,
# MAGIC     ROUND(AVG(NU_NOTA_MT), 2) as avg_nota_mt,
# MAGIC     MAX(NU_NOTA_MT) as max_nota_mt
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE
# MAGIC     NU_NOTA_MT != 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Quantidade de inscritos em Salvador

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(NU_INSCRICAO)
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE NO_MUNICIPIO_PROVA = 'Salvador';

# COMMAND ----------

# MAGIC %md
# MAGIC Quantidade de municípios distintos dos candidatos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT NO_MUNICIPIO_PROVA) as unique_muni
# MAGIC FROM sandbox.dados_enem_2021_ba;

# COMMAND ----------

# MAGIC %md
# MAGIC [Clique aqui](https://docs.databricks.com/sql/language-manual/sql-ref-functions-builtin.html#date-timestamp-and-interval-functions) para ver mais funções de agregação.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agrupamento

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos agrupar os dados e seguidamente aplicar funções de agregação, como média, soma, máximo, mínimo, dentre outras. Sintaxe:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM my_table
# MAGIC WHERE condition
# MAGIC GROUP BY column;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Proporção de inscritos por gênero

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     TP_SEXO as sexo,
# MAGIC     COUNT(NU_INSCRICAO) as quantidade_inscritos
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC GROUP BY TP_SEXO
# MAGIC ORDER BY quantidade_inscritos DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Quantidade de inscritos e desempenho escolar por município

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NO_MUNICIPIO_PROVA            as municipio_prova,
# MAGIC     COUNT(NU_INSCRICAO)           as quantidade_inscritos,
# MAGIC     MIN(NU_NOTA_MT)               as min_nota_mt,
# MAGIC     ROUND(AVG(NU_NOTA_MT), 2)     as avg_nota_mt,
# MAGIC     MAX(NU_NOTA_MT)               as max_nota_mt,
# MAGIC     MIN(NU_NOTA_CN)               as min_nota_cn,
# MAGIC     ROUND(AVG(NU_NOTA_CN), 2)     as avg_nota_cn,
# MAGIC     MAX(NU_NOTA_CN)               as max_nota_cn
# MAGIC FROM sandbox.dados_enem_2021_ba
# MAGIC WHERE 
# MAGIC     NU_NOTA_MT != 0 
# MAGIC     AND NU_NOTA_CN != 0
# MAGIC GROUP BY municipio_prova
# MAGIC ORDER BY quantidade_inscritos DESC, max_nota_mt DESC, max_nota_cn DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **Having**

# COMMAND ----------

# MAGIC %md
# MAGIC A instrução `HAVING` pode ser utilizada em uma query quando não podemos usar uma função de agregação em um filtro `WHERE`.
# MAGIC
# MAGIC Sintaxe:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT column_1, column_2
# MAGIC FROM my_table
# MAGIC WHERE condition
# MAGIC GROUP BY 1
# MAGIC HAVING condition
# MAGIC ORDER BY 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   NO_MUNICIPIO_PROVA,
# MAGIC   MAX(NU_NOTA_MT) as max_nu_nota_Mt
# MAGIC from sandbox.dados_enem_2021_ba
# MAGIC group by 1
# MAGIC having avg(NU_NOTA_MT) > 600
# MAGIC order by 2 desc
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins

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
# MAGIC
# MAGIC Sintaxe básica:
# MAGIC
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT 
# MAGIC     t1.column_1
# MAGIC     t2.column_2
# MAGIC FROM my_table_1       as t1
# MAGIC INNER JOIN my_table_2 as t2
# MAGIC ON t1.column_1 = t2.column_2;
# MAGIC ```
# MAGIC
# MAGIC Fonte: W3Schools.

# COMMAND ----------

# MAGIC %md
# MAGIC * Join da tabela do ENEM com as informações sócioeconômicas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     t1.NO_MUNICIPIO_PROVA  as municipio,
# MAGIC     count(t1.NU_INSCRICAO) as quantidade_inscritos,
# MAGIC     min(t1.NU_NOTA_MT)     as min_nota_mt,
# MAGIC     avg(t1.NU_NOTA_MT)     as avg_nota_mt,
# MAGIC     max(t1.NU_NOTA_MT)     as max_nota_mt
# MAGIC FROM sandbox.dados_enem_2021_ba as t1
# MAGIC LEFT JOIN sandbox.dados_enem_2021_ba_quest  as t2
# MAGIC ON t1.NU_INSCRICAO = t2.NU_INSCRICAO
# MAGIC WHERE 
# MAGIC     t2.Q001 NOT IN ('F', 'G', 'H')
# MAGIC     AND t2.Q002 NOT IN ('F', 'G', 'H')
# MAGIC     AND t2.Q006 IN ('A', 'B', 'C', 'D')
# MAGIC     AND NU_NOTA_MT != 0
# MAGIC GROUP BY municipio
# MAGIC ORDER BY quantidade_inscritos DESC, max_nota_mt DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case When

# COMMAND ----------

# MAGIC %md
# MAGIC Quando queremos aplicar regras baseadas em condições podemos utilizar a expressão `CASE`. Se a condição for verdadeira, o retorno será o valor especificado. Funciona como uma instrução if-then-else. Caso nenhuma condição seja verdadeira, a expressão retorna o valor especificado no `ELSE`.

# COMMAND ----------

# MAGIC %md
# MAGIC Sintaxe:
# MAGIC     
# MAGIC ```sqlite
# MAGIC
# MAGIC SELECT 
# MAGIC     column_1,
# MAGIC     CASE
# MAGIC         WHEN column_2 > 10 THEN 'A'
# MAGIC         WHEN column_3 = 10 THEN 'B'
# MAGIC         ELSE 'C'
# MAGIC     END AS column_cat
# MAGIC FROM my_table;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Trata algumas colunas usando CASE WHEN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     NU_INSCRICAO,
# MAGIC     CASE
# MAGIC         WHEN TP_SEXO = 'F' THEN 'Feminino'
# MAGIC         WHEN TP_SEXO = 'M' THEN 'Masculino'
# MAGIC         WHEN TP_SEXO IS NULL THEN 'missing'
# MAGIC         ELSE 'verificar'
# MAGIC     END AS TP_SEXO,
# MAGIC     CASE
# MAGIC         WHEN TP_ESTADO_CIVIL = 0 THEN 'Não informado'
# MAGIC         WHEN TP_ESTADO_CIVIL = 1 THEN 'Solteiro(a)'
# MAGIC         WHEN TP_ESTADO_CIVIL = 2 THEN 'Casado(a)/Mora com companheiro(a)'
# MAGIC         WHEN TP_ESTADO_CIVIL = 3 THEN 'Divorciado(a)/Desquitado(a)/Separado(a)'
# MAGIC         WHEN TP_ESTADO_CIVIL = 4 THEN 'Viúvo(a)'
# MAGIC         WHEN TP_ESTADO_CIVIL IS NULL THEN 'missing'
# MAGIC         ELSE 'verificar'
# MAGIC     END AS TP_ESTADO_CIVIL,
# MAGIC     CASE
# MAGIC         WHEN TP_COR_RACA = 0 THEN 'Não declarado'
# MAGIC         WHEN TP_COR_RACA = 1 THEN 'Branca'
# MAGIC         WHEN TP_COR_RACA = 2 THEN 'Preta'
# MAGIC         WHEN TP_COR_RACA = 3 THEN 'Parda'
# MAGIC         WHEN TP_COR_RACA = 4 THEN 'Amarela'
# MAGIC         WHEN TP_COR_RACA = 5 THEN 'Indígena'
# MAGIC         WHEN TP_COR_RACA = 6 THEN 'Não dispõe da informação'
# MAGIC         WHEN TP_COR_RACA IS NULL THEN 'missing'
# MAGIC     END AS TP_COR_RACA,
# MAGIC     CASE 
# MAGIC         WHEN TP_ESCOLA = 1 THEN 'Não respondeu'
# MAGIC         WHEN TP_ESCOLA = 2 THEN 'Pública'
# MAGIC         WHEN TP_ESCOLA = 3 THEN 'Privada'
# MAGIC         ELSE 'missing'
# MAGIC     END AS TP_ESCOLA,
# MAGIC     CASE
# MAGIC         WHEN TP_DEPENDENCIA_ADM_ESC = 1 THEN 'Federal'
# MAGIC         WHEN TP_DEPENDENCIA_ADM_ESC = 2 THEN 'Estadual'
# MAGIC         WHEN TP_DEPENDENCIA_ADM_ESC = 3 THEN 'Municipal'
# MAGIC         WHEN TP_DEPENDENCIA_ADM_ESC = 4 THEN 'Privada'
# MAGIC         WHEN TP_DEPENDENCIA_ADM_ESC IS NULL THEN 'missing'
# MAGIC         ELSE 'verificar'
# MAGIC     END AS TP_DEPENDENCIA_ADM_ESC,
# MAGIC     CASE
# MAGIC         WHEN TP_LOCALIZACAO_ESC = 1 THEN 'Urbana'
# MAGIC         WHEN TP_LOCALIZACAO_ESC = 2 THEN 'Rural'
# MAGIC         WHEN TP_LOCALIZACAO_ESC IS NULL THEN 'missing'
# MAGIC         ELSE 'verificar'
# MAGIC     END AS TP_LOCALIZACAO_ESC
# MAGIC FROM sandbox.dados_enem_2021_ba;

# COMMAND ----------

# MAGIC %md
# MAGIC Como poderíamos melhorar o código anterior?

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION

# COMMAND ----------

# MAGIC %md
# MAGIC O operador `UNION`é utilizado para concatenar dados de uma ou mais instruções do tipo `SELECT`. Para tanto precisamos verificar:
# MAGIC * Cada instrução `SELECT` deve conter o mesmo número de colunas.
# MAGIC * Cada coluna deve ter data types similares.
# MAGIC * As colunas em cada instrução `SELECT` devem possuir a mesma ordem.
# MAGIC
# MAGIC Um ponto de atenção é que o operador `UNION` seleciona apenas valores distintos por padrão, ao passo que para permitir duplicidade de valores precisamos usar o operador `UNION ALL`.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH tb_enem AS (select * from sandbox.dados_enem_2021_ba
# MAGIC   where NU_NOTA_MT > 600
# MAGIC union
# MAGIC select * from sandbox.dados_enem_2021_sp
# MAGIC )
# MAGIC select * from tb_enem
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trabalhando com Texto

# COMMAND ----------

# MAGIC %md
# MAGIC Principais funções para trabalhar com texto no SQL:
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
# MAGIC Vejamos agora a demonstração de uso destas funções:

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   contains('Spark SQL', 'Spark'),
# MAGIC   contains('SparkSQL', 'Spark'),
# MAGIC   reverse('Spark SQL'),
# MAGIC   startswith('Spark SQL', 'Sp'),
# MAGIC   startswith('Spark SQ', 'SP'),
# MAGIC   endswith('Spark SQL', 'SQL'),
# MAGIC   replace('PAYMENT PIX', 'PAYMENT', 'PAGAMENTO')
# MAGIC   ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   substr('2020-12-31 00:00:00', 1, 4) as year,
# MAGIC   substr('2020-12-31 00:00:00', 6, 2) as month,
# MAGIC   substr('2020-12-31 00:00:00', 9, 2) as day,
# MAGIC   concat(substr('2020-12-31 00:00:00', 1, 4), '-', substr('2020-12-31 00:00:00', 6, 2)) as year_month
# MAGIC   ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   NO_MUNICIPIO_PROVA ||  '-' || SG_UF_PROVA AS MUN_UF,
# MAGIC   CONCAT(NO_MUNICIPIO_PROVA, '-', SG_UF_PROVA) AS MUN_UF,
# MAGIC   LOWER(NO_MUNICIPIO_PROVA) AS lower_mun,
# MAGIC   UPPER(NO_MUNICIPIO_PROVA) AS upper_mun,
# MAGIC   CHAR_LENGTH(NO_MUNICIPIO_PROVA) AS len_mun
# MAGIC FROM sandbox.dados_enem_2021_ba ;

# COMMAND ----------

# MAGIC %md
# MAGIC **Operador LIKE**

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos usar o operador `LIKE` dentro de filtros com `WHERE` para identificar padrões dentro das colunas. Por exemplo, se quisermos dentro de uma coluna identificar o padrão `Salvador`.
# MAGIC
# MAGIC
# MAGIC Existem dois wildcards que podem ser utilizados com o operador LIKE:
# MAGIC * % : representa zero, um ou vários caracteres.
# MAGIC * _ : representa um único caractere.
# MAGIC
# MAGIC Exemplo:
# MAGIC
# MAGIC
# MAGIC LIKE | Descrição|
# MAGIC -----|---------|
# MAGIC WHERE PaymentsType LIKE 'PIX%' | Encontra valores que iniciam com PIX
# MAGIC WHERE PaymentsType LIKE '%PIX' | Encontra valores que terminam com PIX
# MAGIC WHERE PaymentsType LIKE '%PIX%' | Encontra valores que contenham PIX em qualquer posição

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos ilustrar essas funções utilizando uma base fake que simula diversos problemas que enfrentamos na vida real ao manipular bases. Iremos categorizar as formas de pagamento em quatro categorias.

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *,
# MAGIC   case
# MAGIC     when lower(payment_type) like '%pix%'    then 'pix'
# MAGIC     when lower(payment_type) like '%boleto%' then 'boleto'
# MAGIC     when lower(payment_type) like '%credit%' then 'credit'
# MAGIC     when lower(payment_type) like '%debit%'  then 'debit'
# MAGIC     when payment_type is null then 'missing'
# MAGIC     else 'verificar'
# MAGIC   end as payment_category,
# MAGIC   substr(payment_date, 1, 4) as year,
# MAGIC   substr(payment_date, 6, 2) as month,
# MAGIC   cast(replace(payment_amount, 'R$ ', '') AS float) as payment_amount_number
# MAGIC from sandbox.tb_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trabalhando com Datas

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
# MAGIC Vejamos alguns exemplos de uso destas funções.
# MAGIC
# MAGIC
# MAGIC Conversão de datas:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   '2021-03-01'       as dt_str,
# MAGIC   date('2021-03-01') as dt1, 
# MAGIC   date_format('2020-12-31', 'yyyy-MM')    as year_month,
# MAGIC   date_format('2020-12-31', 'yyyy/MM')    as dt,
# MAGIC   date_format('2020-12-31', 'y')          as year_str,
# MAGIC   date_format('2020-12-31', 'M')          as month_str,
# MAGIC   date_format('2020-12-31', 'd')          as day_str,
# MAGIC   date_format('2020-12-31 12:30:43', 'H') as hour_str,
# MAGIC   date_format('2020-12-31 12:30:43', 'm') as minute_str,
# MAGIC   date_format('2020-12-31 12:30:43', 's') as second_str
# MAGIC   ;

# COMMAND ----------

# MAGIC %md
# MAGIC [Clique aqui](https://docs.databricks.com/sql/language-manual/sql-ref-datetime-pattern.html) para ver os padrões de date e timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC Vejamos agora como trabalhar com shift e diferença em date e timestamp.

# COMMAND ----------

# MAGIC %md
# MAGIC Obs.: A função `date_add()` é análoga a função `dateadd()`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   date_format('2020-12-31', 'dd/MM/yyyy') as df,
# MAGIC   date_add('2020-12-31', 1)   as add_day,
# MAGIC   date_add('2020-12-31', -1)  as sub_day_1,
# MAGIC   date_sub('2020-12-31', 1)   as sub_day_2,
# MAGIC   dateadd('2020-12-31', 1)    as add_day_,
# MAGIC   dateadd('2020-12-31', -1)   as sub_day_,
# MAGIC   datediff('2021-01-31', '2020-12-31') as diff,
# MAGIC   date_format(add_months('2020-01-01', 6), 'yyyy-MM') as dt
# MAGIC   ;

# COMMAND ----------

# MAGIC %md
# MAGIC Com a função `dateadd(unit, value, expr)` podemos adicioanr unidades selecionadas para uma expressão timestamp. De forma análoga podemos usar este conceito para calcular diferença entre datas com a função `datediff(unit, start, end)`.
# MAGIC
# MAGIC ```SQL
# MAGIC dateadd(unit, value, expr)
# MAGIC
# MAGIC unit
# MAGIC  { MICROSECOND |
# MAGIC    MILLISECOND |
# MAGIC    SECOND |
# MAGIC    MINUTE |
# MAGIC    HOUR |
# MAGIC    DAY | DAYOFYEAR |
# MAGIC    WEEK |
# MAGIC    MONTH |
# MAGIC    QUARTER |
# MAGIC    YEAR }
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   dateadd(MONTH, 3, DATE '2020-12-31')                 as add_monh,
# MAGIC   dateadd(DAY, 15, DATE '2021-01-01')                  as add_day,
# MAGIC   dateadd(DAY, -15, DATE '2021-01-01')                 as sub_day,
# MAGIC   dateadd(WEEK, 1, DATE '2021-01-01')                  as add_week,
# MAGIC   dateadd(QUARTER, 1, DATE '2021-01-01')               as add_quarter, 
# MAGIC   dateadd(HOUR, 4, TIMESTAMP '2021-01-01 00:00:00')    as add_hour, 
# MAGIC   dateadd(MINUTE, 15, TIMESTAMP '2021-01-01 00:00:00') as add_min,
# MAGIC   datediff(MONTH, DATE '2020-01-01', '2020-03-31')     as diffmonth,
# MAGIC   datediff(DAY, DATE '2020-01-01', '2020-01-15')     as diffday
# MAGIC   ;

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos usar funções específicar para extrair o dia,mês, ano, etc., de uma date/timestamp:

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   consumer_id,
# MAGIC   payment_date,
# MAGIC   day(payment_date)     as payment_day,
# MAGIC   month(payment_date)   as payment_month,
# MAGIC   year(payment_date)    as payment_year,
# MAGIC   quarter(payment_date) as payment_quarter,
# MAGIC   datediff(DAY, payment_date, now())   as day_delta,
# MAGIC   datediff(MONTH, payment_date, now()) as month_delta
# MAGIC from sandbox.tb_payments
# MAGIC   ;

# COMMAND ----------

# MAGIC %md
# MAGIC Agora vamos verificar duplicadade na base em cada ano-mês:

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH payments AS (
# MAGIC select 
# MAGIC     *,
# MAGIC     date_format(payment_date, 'yyyy-MM') as anomes
# MAGIC from sandbox.tb_payments
# MAGIC )
# MAGIC select 
# MAGIC   anomes,
# MAGIC   count(consumer_id)          as count,
# MAGIC   count(distinct consumer_id) as count_distinct
# MAGIC from payments
# MAGIC group by 1;

# COMMAND ----------

# MAGIC %md
# MAGIC **DESAFIO: para a última compra de cada consumer, mensure o delta em dias entre a data da última compra e a data corrente.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SQL vs Pandas**

# COMMAND ----------

# MAGIC %md
# MAGIC **Consulta, Valores únicos, count e limit**
# MAGIC
# MAGIC
# MAGIC Seleção de colunas
# MAGIC ```sqlite
# MAGIC SELECT column 
# MAGIC FROM my_table;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df['column']
# MAGIC
# MAGIC df.column
# MAGIC
# MAGIC df.loc[:, 'column']
# MAGIC ```
# MAGIC
# MAGIC Head
# MAGIC ```sqlite
# MAGIC SELECT * FROM my_table
# MAGIC LIMIT 5;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.head()
# MAGIC
# MAGIC df.head(n = 5)
# MAGIC ```
# MAGIC
# MAGIC Valores distintos
# MAGIC ```sqlite
# MAGIC SELECT DISTINCT column_1 
# MAGIC FROM my_table;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.column_1.unique()
# MAGIC ```
# MAGIC
# MAGIC Contagem de valores únicos
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT COUNT(DISTINCT column_1) 
# MAGIC FROM my_table;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.column.nunique()
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Filtros**
# MAGIC
# MAGIC
# MAGIC Condição simples
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT * 
# MAGIC FROM my_table
# MAGIC WHERE column_1 = 10;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.query('column_1 == 10')
# MAGIC
# MAGIC df[(df.column_1 == 10)]
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Condição múltipla
# MAGIC ```sqlite
# MAGIC SELECT column_3 
# MAGIC FROM my_table
# MAGIC WHERE 
# MAGIC     column_1 > 10
# MAGIC     AND column_2 = 'A';
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.query("(column_1 > 10) & (column_2 == 'A')")['column_3']
# MAGIC
# MAGIC df[(df.column_1 > 10) & (df.column_2 == 'A')]['column_3']
# MAGIC ```
# MAGIC
# MAGIC Operador IN
# MAGIC
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT * FROM my_table
# MAGIC WHERE column_1 IN ('A', 'B');
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df[df.column_1.isin(['A', 'B'])]
# MAGIC ```
# MAGIC
# MAGIC NOT IN
# MAGIC
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT column_1, column_2 FROM my_table
# MAGIC WHERE column_1 NOT IN ('A', 'B');
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df[~(df.column_1.isin(['A', 'B']))]['column_1', 'column_2']
# MAGIC ```
# MAGIC
# MAGIC Agregação
# MAGIC
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC     MIN(column_1) AS min,
# MAGIC     AVG(column_1) AS avg,
# MAGIC     MAX(column_1) AS max
# MAGIC FROM my_table;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC df.agg({'column_1': ['min', 'mean', 'max']})
# MAGIC ```
# MAGIC
# MAGIC Ordenação
# MAGIC
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     column_1,
# MAGIC     column_2
# MAGIC FROM my_table
# MAGIC ORDER BY column_1 DESC, column_2 DESC;
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC df.sort_values(['column_1', 'column_2'], ascending = [False, False])
# MAGIC ```
# MAGIC
# MAGIC Agrupamento
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     column_1,
# MAGIC     column_2,
# MAGIC     count(column_3)
# MAGIC FROM my_table
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY 3 DESC;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC df.groupby(by = ['column_1', 'column_2'])['column_3'].count().sort_values(ascending = False)
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Join
# MAGIC
# MAGIC
# MAGIC ```sqlite
# MAGIC SELECT 
# MAGIC     t1.column_1,
# MAGIC     t2.column_2
# MAGIC FROM my_table_1 AS t1
# MAGIC INNER JOIN my_table_2 AS t2
# MAGIC     ON t1.column_1 = t2.column_2;
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC my_table_1.merge(my_table_2,
# MAGIC                  left_on  = 'column_1',
# MAGIC                  right_on = 'column_2',
# MAGIC                  how      = 'inner'
# MAGIC                 )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows Functions

# COMMAND ----------

# MAGIC %md
# MAGIC As windows functions operam em grupos de linhas retornando um único valor para cada uma delas, para entender a função um pouco melhor observemos a base de operações financeiras abaixo:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM sandbox.tb_payments_delta
# MAGIC WHERE consumer_id = 18941584806
# MAGIC ORDER BY 1 ASC,3 DESC

# COMMAND ----------

# MAGIC %md #### RANK
# MAGIC * O comando monta um ranking a partir dos valores informados nos parâmetros;
# MAGIC * Utilizaremos o rank para obter os clientes que fizeram operação nas datas com maior volume financeiro.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT consumer_id, 
# MAGIC        payment_date, 
# MAGIC        payment_amount,
# MAGIC        RANK() OVER (PARTITION BY payment_date ORDER BY payment_amount) AS rank
# MAGIC
# MAGIC FROM sandbox.tb_payments_delta
# MAGIC order by 1,4

# COMMAND ----------

# MAGIC %md #### ROW_NUMBER
# MAGIC * O comando monta um ranking a partir dos valores informados nos parâmetros, classificando um registro por colunas especificas;
# MAGIC * Utilizaremos o row_number para obter a última operação de cada cliente.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT consumer_id, 
# MAGIC        payment_type, 
# MAGIC        payment_date, 
# MAGIC        payment_amount, 
# MAGIC        ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY payment_date DESC) AS registro_mais_recente 
# MAGIC
# MAGIC FROM sandbox.tb_payments_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### CTE

# COMMAND ----------

# MAGIC %md Uma CTE "Common Table Expression" ou "expressão de tabela comum" é uma estrutura utilizada temporariamente para tratar valores, assim como a subquery porem em bancos relacionais a CTE tem um desempenho melhor e uma estrutura mais legível.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH payments AS (
# MAGIC     SELECT consumer_id, 
# MAGIC            payment_type, 
# MAGIC            payment_date, 
# MAGIC            payment_amount, 
# MAGIC            ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY payment_date DESC) AS registro_mais_recente 
# MAGIC
# MAGIC     FROM sandbox.tb_payments_delta
# MAGIC )
# MAGIC SELECT  *
# MAGIC FROM payments
# MAGIC WHERE registro_mais_recente = 1 ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### View

# COMMAND ----------

# MAGIC %md A view é um conjunto de procedimentos que podem ser armazenados para consultarmos de forma rápida.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW sandbox.vw_transacao_mais_recente_cliente
# MAGIC AS 
# MAGIC WITH payments AS (
# MAGIC     SELECT consumer_id, 
# MAGIC            payment_type, 
# MAGIC            payment_date, 
# MAGIC            payment_amount, 
# MAGIC            ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY payment_date DESC) AS registro_mais_recente 
# MAGIC
# MAGIC     FROM sandbox.tb_payments_delta
# MAGIC )
# MAGIC SELECT  *
# MAGIC FROM payments
# MAGIC WHERE registro_mais_recente = 1 

# COMMAND ----------

# MAGIC %md Note que agora eu não preciso mais escrever uma query, posso simplesmente consultar o resultado.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(cast(replace(payment_amount,'R$ ','') as double)) FROM sandbox.vw_transacao_mais_recente_cliente

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot

# COMMAND ----------

# MAGIC %md Neste exemplo utilizaremos o pivot para mostrar as últimas três transações de cada cliente.

# COMMAND ----------

# MAGIC %sql 
# MAGIC WITH payments AS (
# MAGIC     SELECT consumer_id, 
# MAGIC            payment_amount, 
# MAGIC            ROW_NUMBER() OVER (PARTITION BY consumer_id ORDER BY payment_date DESC) AS registro_mais_recente 
# MAGIC
# MAGIC     FROM sandbox.tb_payments_delta
# MAGIC )
# MAGIC SELECT  *
# MAGIC FROM payments 
# MAGIC PIVOT (
# MAGIC        sum(cast(replace(payment_amount,'R$ ','') as double))
# MAGIC        FOR (registro_mais_recente) IN (1, 2, 3)
# MAGIC )    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referências

# COMMAND ----------

# MAGIC %md
# MAGIC [SQLBolt](https://sqlbolt.com/)
# MAGIC
# MAGIC [W3Schools](https://www.w3schools.com/)
# MAGIC
# MAGIC [spark.apache.org](https://spark.apache.org/docs/latest/api/sql/index.html#_1)
