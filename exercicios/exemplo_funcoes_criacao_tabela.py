# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("""CREATE DATABASE IF NOT EXISTS beca""")


def get_ddl_clientes(): 
    # retornar comando DDL para criacao da tabela
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS hive_metastore.beca.clientes (
        id_cliente INTEGER COMMENT "Idade do cliente",
        nome_cliente STRING COMMENT "Nome do Cliente",
        idade_cliente INTEGER COMMENT "Idade do cliente",
        SEDE STRING COMMENT "Sede da loja"
    ) 
    USING parquet 
    LOCATION 'dbfs:/tmp/beca/clientes';"""




spark.sql(get_ddl_clientes())

def inset_table_clientes():
    return f"""
    INSERT INTO hive_metastore.beca.clientes
    VALUES
        (1, 'Jaqueline', 25, 'São Paulo'),
        (2, 'Ruth', 30, 'Rio de Janeiro'),
        (3, 'Sergio', 22, 'Curitiba'),
        (4, 'Cleusa', 28, 'São Paulo'),
        (5, 'Gregorio', 35, 'Rio de Janeiro'),
        (6, 'Paula', 27, 'Curitiba'),
        (7, 'Vanessa', 32, 'São Paulo'),
        (8, 'Valde', 29, 'Rio de Janeiro'),
        (9, 'Matheus', 26, 'Curitiba'),
        (10, 'Valdirene', 31, 'São Paulo'),
        (11, 'Tyago', 24, 'São Paulo'),
        (12, 'Cibele', 31, 'Rio de Janeiro'),
        (13, 'Thais', 40, 'Curitiba'),
        (14, 'Diego', 52, 'São Paulo'),
        (15, 'Barbara', 35, 'Rio de Janeiro'),
        (16, 'Janaina', 18, 'Curitiba'),
        (17, 'Wilson', 21, 'São Paulo'),
        (18, 'Cleber', 29, 'Rio de Janeiro'),
        (9, 'Matheus', 26, 'Curitiba'),
        (19, 'Camila', 33, 'São Paulo'),
        (20, 'David', 31, 'São Paulo'),
        (21, 'Clara', 24, 'São Paulo'),
        (22, 'Enzo', 31, 'Rio de Janeiro'),
        (23, 'Victor', 40, 'Curitiba'),
        (3, 'Sergio', 52, 'Curitiba'),
        (5, 'Gregorio', 35, 'Rio de Janeiro'),
        (24, 'Janaina', 18, 'Curitiba'),
        (17, 'Wilson', 21, 'São Paulo'),
        (7, 'Vanessa', 29, 'São Paulo'),
        (9, 'Matheus', 26, 'Curitiba');"""

spark.sql(inset_table_clientes())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.beca.vendas
# MAGIC

# COMMAND ----------

def get_ddl_vendas(): 
# retornar comando DDL para criacao da tabela
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS hive_metastore.beca.vendas (
    id_venda INTEGER COMMENT "ID da venda",
    id_cliente INTEGER COMMENT "id do cliente",
    id_produto INTEGER COMMENT "id do produto",
    quantidade INTEGER COMMENT "Quantidade vendida",
    data_da_venda STRING COMMENT "Data da ultima venda"
) 
USING parquet 
LOCATION 'dbfs:/tmp/beca/vendas';"""

spark.sql(get_ddl_vendas())


def inset_table_vendas():
    return f"""
    INSERT INTO hive_metastore.beca.vendas
    VALUES     
        (4, 4, 104, 1,'2023-02-15'),
        (5, 5, 105, 2,'2023-05-03'),
        (6, 6, 110, 1,'2023-02-21'),
        (7, 7, 107, 3,'2023-01-15'),
        (8, 8, 108, 2,'2023-11-10'),
        (9, 9, 113, 1,'2023-07-11'),
        (10, 10, 101, 4,'2023-09-05'),
        (11, 11, 106, 2,'2023-07-22'),
        (12, 12, 102, 1,'2023-04-21'),
        (13, 13, 112, 3,'2023-01-21'),
        (14, 14, 109, 2,'2023-11-10'),
        (15, 15, 111, 1,'2023-05-03'),
        (16, 16, 115, 3,'2023-04-21'),
        (17, 17, 117, 2,'2023-06-05'),
        (18, 18, 118, 1,'2023-06-18'),
        (19, 19, 120, 4,'2023-03-10'),
        (20, 20, 103, 2,'2023-09-05'),
        (21, 21, 114, 1,'2023-07-22'),
        (22, 22, 119, 3,'2023-04-21'),
        (23, 23, 116, 2,'2023-01-21'),
        (24, 9, 111, 2,'2023-01-11'),
        (25, 5, 108, 1,'2023-05-03'),
        (26, 16, 113, 1,'2023-04-21'),
        (27, 17, 101, 3,'2023-06-05'),
        (28, 7, 108, 4,'2023-06-18'),
        (29, 9, 104, 5,'2023-01-11'),
        (30, 10, 102, 3,'2023-11-10');"""


spark.sql(inset_table_vendas())

# COMMAND ----------

def get_ddl_produtos(): 
    # retornar comando DDL para criacao da tabela
    return f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS hive_metastore.beca.produtos (
        id_produto INTEGER COMMENT "ID do produto",
        nome_produto STRING COMMENT "Nome do produto",
        preco_produto FLOAT COMMENT "Preço do produto"
    ) 
    USING parquet 
    LOCATION 'dbfs:/tmp/beca/produtos';"""




spark.sql(get_ddl_produtos())

def inset_table_produtos():
    return f"""
    INSERT INTO hive_metastore.beca.produtos
    VALUES
        (101, "Produto1", 250.0),
        (102, "Produto2", 130.0),
        (103, "Produto3", 25.0),
        (104, "Produto4", 80.0),
        (105, "Produto5", 20.0),
        (106, "Produto6", 35.0),
        (107, "Produto7", 45.0),
        (108, "Produto8", 28.5),
        (109, "Produto9", 32.0),
        (110, "Produto10", 18.0),
        (111, "Produto11", 53.0),
        (112, "Produto12", 130.0),
        (113, "Produto13", 125.0),
        (114, "Produto14", 42.0),
        (115, "Produto15", 22.0),
        (116, "Produto16", 35.0),
        (117, "Produto17", 45.0),
        (118, "Produto18", 28.0),
        (119, "Produto19", 32.0),
        (120, "Produto20", 38.0);"""

spark.sql(inset_table_produtos())

