# Databricks notebook source



# COMMAND ----------


# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum,month,when,col,avg

# Inicializando a sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Definindo o esquema para o DataFrame de Clientes
schema_clientes = StructType([
    StructField("id_cliente", IntegerType(), True),
    StructField("nome_cliente", StringType(), True),
    StructField("idade_cliente", IntegerType(), True),
    StructField("SEDE", StringType(), True),
])


# Criando o DataFrame de Clientes
clientes_data = [(1, "Jaqueline", 25, "São Paulo"),
                 (2, "Ruth", 30, "Rio de Janeiro"),
                 (3, "Sergio", 22, "Curitiba"),
                 (4, "Cleusa", 28, "São Paulo"),
                 (5, "Gregorio", 35, "Rio de Janeiro"),
                 (6, "Paula", 27, "Curitiba"),
                 (7, "Vanessa", 32, "São Paulo"),
                 (8, "Valde", 29, "Rio de Janeiro"),
                 (9, "Matheus", 26, "Curitiba"),
                 (10, "Valdirene", 31, "São Paulo"),
                 (11, "Tyago", 24, "São Paulo"),
                 (12, "Cibele", 31, "Rio de Janeiro"),
                 (13, "Thais", 40, "Curitiba"),
                 (14, "Diego", 52, "São Paulo"),
                 (15, "Barbara", 35, "Rio de Janeiro"),
                 (16, "Janaina", 18, "Curitiba"),
                 (17, "Wilson", 21, "São Paulo"),
                 (18, "Cleber", 29, "Rio de Janeiro"),
                 (9, "Matheus", 26, "Curitiba"),
                 (19, "Camila", 33, "São Paulo"),
                 (20, "David", 31, "São Paulo"),
                 (21, "Clara", 24, "São Paulo"),
                 (22, "Enzo", 31, "Rio de Janeiro"),
                 (23, "Victor", 40, "Curitiba"),
                 (3, "Sergio", 52, "Curitiba"),
                 (5, "Gregorio", 35, "Rio de Janeiro"),
                 (24, "Janaina", 18, "Curitiba"),
                 (17, "Wilson", 21, "São Paulo"),
                 (7, "Vanessa", 29, "São Paulo"),
                 (9, "Matheus", 26, "Curitiba")]

clientes_df = spark.createDataFrame(clientes_data, schema=schema_clientes)
clientes_df.display()

# Definindo o esquema para o DataFrame de Produtos
schema_produtos = StructType([
    StructField("id_produto", IntegerType(), True),
    StructField("nome_produto", StringType(), True),
    StructField("preco_produto", DoubleType(), True)
])

# Criando o DataFrame de Produtos
produtos_data = [(101, "Produto1", 250.0),
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
                 (120, "Produto20", 38.0)]

produtos_df = spark.createDataFrame(produtos_data, schema=schema_produtos)

# Exibindo o DataFrame de Produtos
display(produtos_df)

# Definindo o esquema para o DataFrame de Vendas
schema_vendas = StructType([
    StructField("id_venda", IntegerType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("id_produto", IntegerType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("data_da_venda", StringType(), True)
])

# Criando o DataFrame de Vendas
vendas_data=([
    (1, 1, 101, 2, '2023-02-15'),
    (2, 2, 111, 1, '2023-02-15'),
    (3, 3, 120, 4, '2023-02-15'),
    (4, 4, 104, 1, '2023-02-15'),
    (5, 5, 105, 2, '2023-05-03'),
    (6, 6, 110, 1, '2023-02-21'),
    (7, 7, 107, 3, '2023-01-15'),
    (8, 8, 108, 2, '2023-11-10'),
    (9, 9, 113, 1, '2023-07-11'),
    (10, 10, 101, 4, '2023-09-05'),
    (11, 11, 106, 2, '2023-07-22'),
    (12, 12, 102, 1, '2023-04-21'),
    (13, 13, 112, 3, '2023-01-21'),
    (14, 14, 109, 2, '2023-11-10'),
    (15, 15, 111, 1, '2023-05-03'),
    (16, 16, 115, 3, '2023-04-21'),
    (17, 17, 117, 2, '2023-06-05'),
    (18, 18, 118, 1, '2023-06-18'),
    (19, 19, 120, 4, '2023-03-10'),
    (20, 20, 103, 2, '2023-09-05'),
    (21, 21, 114, 1, '2023-07-22'),
    (22, 22, 119, 3, '2023-04-21'),
    (23, 23, 116, 2, '2023-01-21'),
    (24, 9, 111, 2, '2023-01-11'),
    (25, 5, 108, 1, '2023-05-03'),
    (26, 16, 113, 1, '2023-04-21'),
    (27, 17, 101, 3, '2023-06-05'),
    (28, 7, 108, 4, '2023-06-18'),
    (29, 9, 104, 5, '2023-01-11'),
    (30, 10, 102, 3, '2023-11-10')
])

# Criando DataFrame de Vendas com os dados fictícios
vendas_df = spark.createDataFrame(vendas_data, schema=schema_vendas)


# Exibindo o DataFrame de Vendas
vendas_df.display()



# COMMAND ----------



# a) Selecionando colunas "nome_cliente" e "idade_cliente"  e crie uma nova coluna "idade_categorizada"
clientes_resultado = clientes_df.select("nome_cliente", "idade_cliente") \
    .withColumn("idade_categorizada",when(col("idade_cliente") < 21, "Jovem").otherwise("Adulto"))

# c) Renomeando a coluna "idade_cliente" para "idade"
clientes_resultado = clientes_resultado.withColumnRenamed("idade_cliente", "idade")

# d) Agregando para calcular a média de idade por categoria
clientes_resultado = clientes_resultado.groupBy("idade_categorizada") \
    .agg(avg("idade").alias("media_idade"))

# e) Filtrando apenas os clientes da categoria "Jovem"
clientes_resultado = clientes_resultado.filter(col("idade_categorizada") == "Jovem")

# Exibindo o resultado final
clientes_resultado.display()


# COMMAND ----------



# Unindo os DataFrames clientes_df, produtos_df e vendas_df
vendas_completas_df = vendas_df.join(clientes_df, "id_cliente", "inner").join(produtos_df, "id_produto", "inner")

# Adicionando uma coluna com o total gasto em cada venda
vendas_completas_df = vendas_completas_df.withColumn("total_gasto", vendas_completas_df["quantidade"] * vendas_completas_df["preco_produto"])

# Agregando o total gasto por cliente
total_gasto_por_cliente = vendas_completas_df.groupBy("id_cliente", "nome_cliente").agg(sum("total_gasto").alias("total_gasto_cliente"))

# Ordenando o DataFrame pelo total gasto em ordem decrescente
total_gasto_por_cliente = total_gasto_por_cliente.orderBy("total_gasto_cliente", ascending=False)

# Exibindo o DataFrame resultante
display(total_gasto_por_cliente)



# COMMAND ----------

# Agregando o total vendido por produto
total_vendido_por_produto = vendas_df.groupBy("id_produto").agg(sum("quantidade").alias("total_vendido_por_produto"))

# Unindo o DataFrame total_vendido_por_produto com o DataFrame de produtos_df para obter informações sobre os produtos
produtos_mais_vendidos_df = total_vendido_por_produto.join(produtos_df, "id_produto", "inner")

# Ordenando o DataFrame pelo total vendido em ordem decrescente
produtos_mais_vendidos_df = produtos_mais_vendidos_df.orderBy("total_vendido_por_produto", ascending=False)

# Exibindo o DataFrame resultante
display(produtos_mais_vendidos_df)

#display(clientes_df)


# COMMAND ----------



# Definindo o ID do cliente desejado para o filtro
id_cliente_desejado = 5

# Filtrando o DataFrame de vendas para exibir apenas as vendas do cliente desejado
vendas_do_cliente_df = vendas_completas_df.filter(vendas_completas_df["id_cliente"] == id_cliente_desejado)

# Exibindo o DataFrame resultante
display(vendas_do_cliente_df)

# COMMAND ----------

from pyspark.sql import functions as F

# a) Converta a coluna "data_da_venda" para o formato de data.
vendas_df = vendas_df.withColumn("data_da_venda", F.to_date("data_da_venda", "yyyy-MM-dd"))

# b) Realize um join entre vendas_df e produtos_df usando a coluna "id_produto".
#    Isso adicionará as colunas "preco_produto" e "nome_produto" a um novo DataFrame chamado vendas_com_produto_df.
vendas_com_produto_df = vendas_df.join(produtos_df, on="id_produto", how="inner")

# c) Agrupe as vendas por mês e d) calcule o valor total vendido em cada mês.
vendas_por_mes = vendas_com_produto_df.withColumn("valor_total", col("quantidade") * col("preco_produto")) \
    .groupBy(F.year("data_da_venda").alias("ano"), F.month("data_da_venda").alias("mes")) \
    .agg(F.sum("valor_total").alias("valor_total_mes")) \
    .orderBy("ano", "mes")

# Exibindo os resultados
vendas_por_mes.display()


# COMMAND ----------

from pyspark.sql import functions as F

# a) Converta a coluna "data_da_venda" para o formato de data.
vendas_df = vendas_df.withColumn("data_da_venda", F.to_date("data_da_venda", "yyyy-MM-dd"))

# b) Realize um join entre vendas_df e produtos_df usando a coluna "id_produto".
#    Isso adicionará as colunas "preco_produto" e "nome_produto" a um novo DataFrame chamado vendas_com_produto_df.
vendas_com_produto_df = vendas_df.join(produtos_df, on="id_produto", how="inner")

# c) Calcule a coluna "valor_total" antes de realizar a agregação.
vendas_com_produto_df = vendas_com_produto_df.withColumn("valor_total", col("quantidade") * col("preco_produto"))

# d) Agregue o DataFrame de vendas_com_produto_df para calcular o valor total vendido em cada sede.
#    Neste exemplo, vamos supor que a sede esteja na coluna "SEDE" do DataFrame clientes_df.
vendas_por_sede = vendas_com_produto_df.join(clientes_df, on="id_cliente", how="inner") \
    .groupBy("SEDE") \
    .agg(F.sum("valor_total").alias("valor_total_por_sede")) \
    .orderBy("valor_total_por_sede", ascending=False)

# Exibindo os resultados
vendas_por_sede.display()


# COMMAND ----------

from pyspark.sql.functions import col

# a) Identifique os clientes que não fizeram compras.
clientes_sem_compras = clientes_df.join(vendas_df, on="id_cliente", how="left_anti")

# Exibindo os resultados
clientes_sem_compras.display()


