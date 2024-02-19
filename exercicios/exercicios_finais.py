from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, to_date, date_format
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window
# Crie uma Spark session
spark = SparkSession.builder.getOrCreate()

#Leando dataset do databricks
caminho_arquivo_csv = 'dbfs:/databricks-datasets/online_retail/data-001/data.csv'
df = spark.read.csv(caminho_arquivo_csv, header=True, inferSchema=True)



# COMMAND ----------

#Lista de exercicios:
#Qual a contagem de Registros do dataframe?

#Renomeie os nomes das colunas 
#DE InvoiceNo PARA Numero da Fatura
#DE StockCode PARA Código de estoque
#DE Description PARA Descrição
#DE Quantity PARA  Quantidade
#DE InvoiceDate PARA Data da fatura
#DE UnitPrice PARA Preço Unitário
#DE CustomerID PARA Identificação do Cliente
#DE CountryPARA  Pais

#Altere o formato da coluna Data da fatura de 'MM/d/yy H:mm' para YYYMMDD
#Qual a quantidade total vendidas?
#Qual o Preço médio unitário?
#Conte o número de faturas por país
#Encontre as 5 faturas mais caras
#Calcule o total faturado por cada cliente
#Encontre e exiba as faturas com quantidade negativa
#Conte quantos clientes unicos existem por cada pais

# COMMAND ----------

#Respostas
####################################################
# Qual a contagem de Registros do dataframe
total_registros = df.count()
print("Número total de registros:", total_registros)
######################################################

# Renomear colunas
df = df.withColumnRenamed("InvoiceNo", "Numero da Fatura") \
        .withColumnRenamed("StockCode", "Código de estoque") \
        .withColumnRenamed("Description", "Descrição") \
        .withColumnRenamed("Quantity", "Quantidade") \
        .withColumnRenamed("InvoiceDate", "Data da fatura") \
        .withColumnRenamed("UnitPrice", "Preço Unitário") \
        .withColumnRenamed("CustomerID", "Identificação do Cliente") \
        .withColumnRenamed("Country", "Pais")

display(df)

##################################################################################

# Altere o formato da coluna
df = df.withColumn("Data da Fatura", to_date(df["Data da Fatura"], 'MM/d/yy H:mm'))

# Formatando a data para "YYYYMMDD"
df = df.withColumn("DataFormatada", date_format("Data da Fatura", "yyyyMMdd"))
display(df)

########################################################################################

#Quantidade total vendida:
quantidade_total_vendida = df.agg({"Quantidade": "sum"}).collect()[0][0]
print("Quantidade total vendida:", quantidade_total_vendida)

################################################################

#Preço médio unitário
preco_medio_unitario = df.agg({"Preço Unitário": "avg"}).collect()[0][0]
print("Preço médio unitário:", preco_medio_unitario)

################################################################

#Conte o número de faturas por país
faturas_por_pais = df.groupBy("Pais").count()
display(faturas_por_pais)

#######################################################################

#Encontre as 5 faturas mais caras
faturas_mais_caras = df.orderBy("Preço Unitário", ascending=False).limit(5)
display(faturas_mais_caras)

########################################################################

#Calcule o total faturado por cada cliente
total_faturado_por_cliente = df.groupBy("Identificação do Cliente").agg({"Preço Unitário": "sum"})
display(total_faturado_por_cliente)

#################################################################################################

#Encontre e exiba as faturas com quantidade negativa
faturas_quantidade_negativa = df.filter(df["Quantidade"] < 0)
display(faturas_quantidade_negativa)

#######################################################################################################
#Conte quantos clientes unicos existem por cada pais
clientes_por_pais = df.select("Pais", "Identificação do Cliente").distinct()

# Contar clientes únicos por país
clientes_por_pais_count = clientes_por_pais.groupBy("Pais").count()

# Renomear a coluna de contagem
clientes_por_pais_count = clientes_por_pais_count.withColumnRenamed("count", "Clientes Únicos por País")
display(clientes_por_pais_count)

####################################################################################################################

