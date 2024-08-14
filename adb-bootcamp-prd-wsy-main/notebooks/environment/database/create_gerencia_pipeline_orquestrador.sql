-- Databricks notebook source
-- MAGIC %md # Pipeline Orquestrador
-- MAGIC * Este notebook tem como objetivo manipular a tabela de orquestração do pipeline

-- COMMAND ----------

-- MAGIC %md ### Create

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gerencia;
DROP TABLE IF EXISTS gerencia.pipeline_orquestrador;
CREATE TABLE gerencia.pipeline_orquestrador (

    etapa string NOT NULL,
    chave_entidade string NOT NULL,
    
    descricao string,
    dataset string NOT NULL,
    parametro string NOT NULL,
    flg_ativo boolean NOT NULL,dt_atualizacao timestamp NOT NULL 
    
)
USING DELTA

TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')

LOCATION '/mnt/gerencia/pipeline/pipeline_orquestrador'

-- COMMAND ----------

-- MAGIC %md ### Insert

-- COMMAND ----------

-- DBTITLE 1,schema cliente
INSERT INTO gerencia.pipeline_orquestrador
(
     
       etapa,
       chave_entidade,
       descricao,
       dataset,
       parametro,
       flg_ativo,
       dt_atualizacao
)
VALUES 
(      
      
       'RB',
       'sql_server.core_financeiro.login',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.login',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "cliente",
            "tabela_origem": "login",
            "select_origem": "select id_login,nm_cliente,id_localizacao,id_sexo from cliente.login"
        }',
       'true',
        now()
),
(      
      
       'RB',
       'sql_server.core_financeiro.localizacao',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.localizacao',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "cliente",
            "tabela_origem": "localizacao",
            "select_origem": "select id_localizacao, nm_estado, sg_estado, nr_ddd from cliente.localizacao"
        }',
       'true',
        now()
),
(      
      
       'RB',
       'sql_server.core_financeiro.sexo',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.sexo',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "cliente",
            "tabela_origem": "sexo",
            "select_origem": "select id_sexo, ds_sexo from cliente.sexo"
        }',
       'true',
        now()
)

-- COMMAND ----------

-- DBTITLE 1,schema transacao
INSERT INTO gerencia.pipeline_orquestrador
(
     
       etapa,
       chave_entidade,
       descricao,
       dataset,
       parametro,
       flg_ativo,
       dt_atualizacao
)
VALUES 
(      
      
       'RB',
       'sql_server.core_financeiro.status_conta',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.status_conta',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "transacao",
            "tabela_origem": "status_conta",
            "select_origem": "select id_status_conta, ds_status_conta from transacao.status_conta"
        }',
       'true',
        now()
),
(      
      
       'RB',
       'sql_server.core_financeiro.conta',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.conta',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "transacao",
            "tabela_origem": "conta",
            "select_origem": "select id_conta, cd_conta, id_login, id_status_conta from transacao.conta"
        }',
       'true',
        now()
),
(      
      
       'RB',
       'sql_server.core_financeiro.natureza_transacao',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.natureza_transacao',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "transacao",
            "tabela_origem": "natureza_transacao",
            "select_origem": "select id_natureza, ds_natureza_operacao from transacao.natureza_transacao"
        }',
       'true',
        now()
),
(      
      
       'RB',
       'sql_server.core_financeiro.motivo_transacao',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.motivo_transacao',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "transacao",
            "tabela_origem": "motivo_transacao",
            "select_origem": "select id_motivo, ds_transacao_motivo, id_natureza from transacao.motivo_transacao"
        }',
       'true',
        now()
),
(      
      
       'RB',
       'sql_server.core_financeiro.transacao',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.transacao',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "transacao",
            "tabela_origem": "transacao",
            "select_origem": "select id_transacao, dt_transacao, vl_transacao, id_motivo ,id_conta from transacao.transacao"
        }',
       'true',
        now()
)

-- COMMAND ----------

INSERT INTO gerencia.pipeline_orquestrador
(
     
       etapa,
       chave_entidade,
       descricao,
       dataset,
       parametro,
       flg_ativo,
       dt_atualizacao
)
VALUES 
(      
      
       'RB',
       'sql_server.core_financeiro.telefone',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.telefone',
       'ds_sqlserver_corefinanceiro',
       '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "cliente",
            "tabela_origem": "telefone",
            "select_origem": "select id_telefone, nr_ddd, nr_telefone from cliente.telefone"
        }',
       'true',
        now()
)

-- COMMAND ----------

update gerencia.pipeline_orquestrador
set parametro = '{
            "container_destino": "raw",
            "diretorio_destino": "sqlserver/core_financeiro",
            "esquema_origem": "cliente",
            "tabela_origem": "telefone",
            "select_origem": "select id_telefone, nr_ddd, nr_telefone,id_login from cliente.telefone"
        }'
where chave_entidade = 'sql_server.core_financeiro.telefone'


-- COMMAND ----------

INSERT INTO gerencia.pipeline_orquestrador
(
     
       etapa,
       chave_entidade,
       descricao,
       dataset,
       parametro,
       flg_ativo,
       dt_atualizacao
)
VALUES 
(      
      
       'SV',
       'cliente_conta.perfil_financeiro.transacoes',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.transacoes',
       'ds_cliente_conta',
       '{
            "notebook": "transacoes",
        }',
       'true',
        now()
),
(      
      
       'SV',
       'cliente_conta.perfil_financeiro.perfil_cliente',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.perfil_cliente',
       'ds_cliente_conta',
       '{
            "notebook": "perfil_cliente",
        }',
       'true',
        now()
),
(      
      
       'SV',
       'cliente_conta.perfil_financeiro.conta',
       'Este Job tem como objetivo carregar a entidade: sqlserver.core_financeiro.conta',
       'ds_cliente_conta',
       '{
            "notebook":"conta",
        }',
       'true',
        now()
)

-- COMMAND ----------

select * from gerencia.pipeline_orquestrador

-- COMMAND ----------


update gerencia.pipeline_orquestrador
set flg_ativo = 'false'
where chave_entidade = 'cliente_conta.perfil_financeiro.conta'
