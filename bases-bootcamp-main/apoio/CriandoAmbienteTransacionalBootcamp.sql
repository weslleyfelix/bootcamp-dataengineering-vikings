USE [sqldb-bootcamp-teste];
GO

CREATE SCHEMA cliente;
GO

CREATE SCHEMA transacao;
GO

--Localização
CREATE TABLE cliente.localizacao(

    id_localizacao INT PRIMARY KEY IDENTITY,
    nm_estado VARCHAR(50) NOT NULL,	
    sg_estado VARCHAR(10),
    nr_ddd INT

)

INSERT INTO cliente.localizacao(nm_estado,sg_estado,nr_ddd)
VALUES ('Minas Gerais' , 'MG', 32)
      ,('Piauí' , 'PI', 89)
      ,('Alagoas' , 'AL', 82)
      ,('Rio de Janeiro' , 'RJ', 21)
      ,('São Paulo' , 'SP', 11)
      ,('Santa Catarina' , 'SC', 47)

--Sexo
CREATE TABLE cliente.sexo(

    id_sexo INT PRIMARY KEY IDENTITY,
    ds_sexo VARCHAR(50) NOT NULL
)

INSERT INTO cliente.sexo(ds_sexo)
VALUES ('masculino')
      ,('feminino' )
 

--Login
CREATE TABLE cliente.login(

    id_login INT PRIMARY KEY IDENTITY,
    nm_cliente VARCHAR(50) NOT NULL,
    id_localizacao INT FOREIGN KEY REFERENCES cliente.localizacao (id_localizacao),
    id_sexo INT FOREIGN KEY REFERENCES cliente.sexo (id_sexo)
)


INSERT INTO cliente.login(nm_cliente,id_localizacao,id_sexo)
VALUES ('weslley' , 1, 1)
      ,('odemir' , 2, 1)
      ,('lucas' , 3, 1)
      ,('erika' , 4, 2)
      ,('eliane' , 5, 2)
      ,('roberto' , 6, 1)


--telefone
CREATE TABLE cliente.telefone(

    id_telefone INT PRIMARY KEY IDENTITY,
    nr_ddd INT,
    nr_telefone INT,
    id_login INT FOREIGN KEY REFERENCES cliente.login(id_login)
)

INSERT INTO cliente.telefone(nr_ddd,nr_telefone,id_login)
VALUES  (32, 993540000,	1)
       ,(32, 993540001,	1)
       ,(89, 993540002,	2)
       ,(82, 993540003,	3)
       ,(21, 993540004,	4)
       ,(21, 993540005,	4)
       ,(11, 993540006,	5)
       ,(47, 993540007,	6)


--Status
CREATE TABLE transacao.status_conta(

    id_status_conta INT PRIMARY KEY IDENTITY,
	ds_status_conta VARCHAR(50) NOT NULL
)

INSERT INTO transacao.status_conta(ds_status_conta)
VALUES ('ativa')
      ,('encerrada')
      ,('em encerramento')


--Conta
CREATE TABLE transacao.conta(

    id_conta INT PRIMARY KEY IDENTITY,
	cd_conta INT,	
    id_login INT FOREIGN KEY REFERENCES cliente.login (id_login),	
    id_status_conta INT FOREIGN KEY REFERENCES transacao.status_conta (id_status_conta)
)

INSERT INTO transacao.conta(cd_conta,id_login,id_status_conta)
VALUES (1121 , 1, 1)
      ,(1122 , 2, 2)
      ,(1123 , 3, 3)
      ,(1124 , 4, 1)
      ,(1125 , 5, 1)
      ,(1126 , 6, 1)


--Natureza da Transacao
CREATE TABLE transacao.natureza_transacao(

    id_natureza INT PRIMARY KEY IDENTITY,
	ds_natureza_operacao VARCHAR(50) NOT NULL

)

INSERT INTO transacao.natureza_transacao(ds_natureza_operacao)
VALUES ('Débito')
      ,('Crédito')


--Motivo da Transacao
CREATE TABLE transacao.motivo_transacao(

    id_motivo INT PRIMARY KEY IDENTITY,
	ds_transacao_motivo VARCHAR(50) NOT NULL,	
    id_natureza INT FOREIGN KEY REFERENCES transacao.natureza_transacao(id_natureza) 

)

INSERT INTO transacao.motivo_transacao(ds_transacao_motivo,id_natureza)
VALUES ('Pagamento' , 1)
      ,('PIX Enviado' , 1)
      ,('Transferência Recebida' , 2)
      ,('Pix Recebido' , 2)
      ,('Compra no débito' , 1)



--Tabela de transações financeiras
CREATE TABLE transacao.transacao(

    id_transacao INT PRIMARY KEY IDENTITY,
	dt_transacao DATE,	
    vl_transacao DECIMAL,
    id_motivo INT FOREIGN KEY REFERENCES transacao.motivo_transacao(id_motivo),
    id_conta INT FOREIGN KEY REFERENCES transacao.conta(id_conta) 

)

INSERT INTO transacao.transacao(dt_transacao,vl_transacao,id_motivo,id_conta)
VALUES  ('2023-02-14', 40.000   ,1 ,1)
       ,('2022-05-06', 15.000   ,2 ,2)
       ,('2021-08-25', 0.0400   ,3 ,3)
       ,('2022-04-25', 20.000	,4 ,4)
       ,('2023-01-05', 36.983   ,5 ,5)
       ,('2022-09-13', 1.500	,3 ,6)
       ,('2023-04-28', 1.000	,4 ,1)
       ,('2022-09-06', 16.346	,1 ,1)
       ,('2022-04-26', 4.745	,2 ,3)
       ,('2022-12-28', 1.500	,3 ,4)
       ,('2023-02-14', 2.100	,4 ,5)
       ,('2022-11-21', 2.000	,5 ,5)
       ,('2023-02-22', 8.990	,3 ,6)
       ,('2022-10-06', 10.000	,5 ,6)
       ,('2023-02-01', 1.068	,5 ,6)
       ,('2022-12-20', 1.200	,5 ,2)
       ,('2023-01-20', 1.400	,5 ,2)
       ,('2022-05-05', 5.000	,2 ,1)
       ,('2023-03-27', 0.0100	,2 ,5)
       ,('2022-12-23', 100.000  ,2 ,6)


-- Tabelas
select * from cliente.login;
select * from cliente.localizacao;
select * from cliente.telefone;
select * from cliente.sexo;
select * from transacao.status_conta;
select * from transacao.conta;
select * from transacao.natureza_transacao;
select * from transacao.motivo_transacao;
select * from transacao.transacao;

-- base para mailing
select log.id_login,
       log.nm_cliente,
       tel.id_telefone,
       tel.nr_ddd,
       tel.nr_telefone

from cliente.login as log
inner join cliente.telefone as tel on tel.id_login = log.id_login
;

--Perfil do Cliente
select log.id_login,
       log.nm_cliente,
       sex.id_sexo,
       sex.ds_sexo,
       loc.id_localizacao,
       loc.nm_estado,
       loc.sg_estado

from cliente.login as log
inner join cliente.localizacao as loc on loc.id_localizacao = log.id_localizacao
inner join cliente.sexo as sex on sex.id_sexo = log.id_sexo
;


-- cliente e conta
select log.id_login,
       log.nm_cliente,
       con.id_conta,
       con.cd_conta,
       sta.id_status_conta,
       sta.ds_status_conta

from cliente.login as log
inner join transacao.conta as con on con.id_login = log.id_login
inner join transacao.status_conta as sta on sta.id_status_conta = con.id_status_conta
;


-- Transacao
select tra.id_transacao,	
       tra.dt_transacao,	
       tra.vl_transacao,
       mot.id_motivo,
       mot.ds_transacao_motivo,
       nat.id_natureza,
       nat.ds_natureza_operacao

from transacao.transacao as tra
inner join transacao.motivo_transacao as mot on mot.id_motivo = tra.id_motivo
inner join transacao.natureza_transacao as nat on nat.id_natureza = mot.id_natureza

;
