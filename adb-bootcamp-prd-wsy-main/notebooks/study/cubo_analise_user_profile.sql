-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Analisando o Data Mart
-- MAGIC * Neste notebook vamos analisar os dados do data mart user_profile formando um cubo que é a junção das fatos e dimensões;

-- COMMAND ----------

-- DBTITLE 1,Cubo 
select 
      --endereço
       dim_address.city	
      ,dim_address.state	
      
      --faixa idade
      ,dim_age.codeAge	
      ,dim_age.rangeAge
      
      --sexo
      ,dim_gender.genderUser
      
      --profissão
      ,companyDepartment
      
      --metricas
      ,sum(fat_user.nrUsers) as nrUsers

from gold.fat_user              as fat_user
inner join gold.dim_address     as dim_address    on dim_address.skAddress = fat_user.skAddress
inner join gold.dim_age         as dim_age        on dim_address.skAddress = fat_user.skAddress
inner join gold.dim_gender      as dim_gender     on dim_gender.skGender  = fat_user.skGender
inner join gold.dim_profession  as dim_profession on dim_profession.skProfession  = fat_user.skProfession

group by 1,2,3,4,5,6

-- COMMAND ----------

-- DBTITLE 1,Usuários por localidade
select 
      --endereço
       dim_address.city	
      ,dim_address.state	
      
      --metricas
      ,sum(fat_user.nrUsers) as nrUsers

from gold.fat_user              as fat_user
inner join gold.dim_address     as dim_address    on dim_address.skAddress = fat_user.skAddress

group by 1,2 --,3,4,5

-- COMMAND ----------

-- DBTITLE 1,Usuários por gênero
select 
      --sexo
       dim_gender.genderUser
      
      --metricas
      ,sum(fat_user.nrUsers) as nrUsers

from gold.fat_user              as fat_user
inner join gold.dim_gender      as dim_gender     on dim_gender.skGender  = fat_user.skGender

group by 1

-- COMMAND ----------

-- DBTITLE 1,Usuários por range de idade
select 
     
      --faixa idade	
       dim_age.rangeAge
      --,dim_age.codeAge
      
      --metricas
      ,sum(fat_user.nrUsers) as nrUsers

from gold.fat_user              as fat_user
inner join gold.dim_age         as dim_age        on dim_age.skAge = fat_user.skAge

group by 1
order by 1

-- COMMAND ----------

-- DBTITLE 1,Usuários área de atuação
select 
      --profissão
      dim_profession.companyDepartment
      
      --metricas
      ,sum(fat_user.nrUsers) as nrUsers

from gold.fat_user              as fat_user
inner join gold.dim_profession  as dim_profession on dim_profession.skProfession  = fat_user.skProfession

group by 1
