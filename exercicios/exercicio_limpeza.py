# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType
from pyspark.sql.functions import when, to_date,current_date,regexp_replace, col


# Crie uma sessão Spark
spark = SparkSession.builder.appName("ExercicioLimpezaDados").getOrCreate()

# Defina a estrutura do DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("nome", StringType(), True),
    StructField("idade", StringType(), True),
    StructField("salario", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("email", StringType(), True),
    StructField("data_contratacao", StringType(), True),
    StructField("status", StringType(), True),
    StructField("avaliacao", StringType(), True)
])

# Crie um DataFrame com dados de qualidade questionável xD
data = ([
    (1, "João", "25", "$5000", "RH", "São Paulo", "joao@email.com", "2022-01-15", "ativo", "Bom"),
    (2, "Maria", "30", "$6000", "Financeiro", "Rio de Janeiro", "maria@email.com", "2021-11-20", "inativo", "Regular"),
    (3, "José", "XYZ", "$7000", "TI", "Belo Horizonte", "jose@email.com", "2021-09-10", "ativo", "Ruim"),
    (4, "Ana", "28", "$8000", "Vendas", "Porto Alegre", "ana@email.com", "20220228", "ativo", "Ótimo"),
    (5, "Carlos", "32", "$9000", "Marketing", "Brasília", "carlos@email.com", "2021-08-05", "inativo", "Regular"),
    (6, "Fernanda", "27", "NaN", "RH", "Recife", "fernanda@email.com", "2022-03-12", "ativo", "Bom"),
    (7, "Mariana", "22", "$5500", "Vendas", "XYZ", "mariana@email.com", "20211130", "ativo", "Bom"),
    (8, "Ricardo", "35", "$6200", "Financeiro", "XYZ", "ricardo@email.com", "2021-06-25", "inativo", None),
    (9, "Camila", "XYZ", "$7200", "TI", "XYZ", None, "2022-02-10", "ativo", "Regular"),
    (10, "Pedro", "29", "$8500", "Vendas", "XYZ", "pedro@email.com", None, "inativo", "Ótimo"),
    (11, "Sofia", "31", "$9200", "Marketing", "XYZ", "sofia@email.com", "2022-04-05", "ativo", "Bom"),
    (12, "Lucas", "26", "NaN", "RH", "XYZ", "lucas@email.com", "2021-09-05", "ativo", "Ruim"),
    (13, "Isabela", "33", "$7700", "Vendas", "XYZ", "isabela@email.com", "2021-12-20", "inativo", "Ótimo"),
    (14, "Felipe", "28", "$8800", "Financeiro", "XYZ", "felipe@email.com", "20210815", None, "Regular"),
    (15, "Amanda", "36", "$9500", "TI", "XYZ", "amanda@email.com", "2021-10-01", "ativo", "Bom"),
    (16, "Gustavo", "24", "$5300", None, "Brasília", "gustavo@email.com", "2021-07-01", "ativo", "Ruim"),
    (17, "Patrícia", "XYZ", "$7000", "Vendas", "XYZ", "patricia@email.com", "2021-07-01", "ativo", "Ruim"),
    (18, "Henrique", "30", "$8300", "Marketing", "XYZ", "henrique@email.com", "2021/07/01", "inativo", None),
    (19, "Vanessa", "29", "$8000", "RH", "XYZ", "vanessa@email.com", "2021-07-01", "inativo", "Ótimo"),
    (20, "Eduardo", "XYZ", "$7200", "Financeiro", "XYZ", "eduardo@email.com", "2021-07-01", "ativo", "Ruim"),
    (21, "Laura", "25", "$5800", "RH", "São Paulo", "laura@email.com", "2022-01-15", "ativo", "Bom"),
    (22, "Rodrigo", "34", "$7200", "Financeiro", "Rio de Janeiro", "rodrigo@email.com", "2021-11-20", "inativo", None),
    (23, "Fernando", "XYZ", "$6500", "TI", "Belo Horizonte", "fernando@email.com", "2021-09-10", "ativo", "Ruim"),
    (24, "Carolina", "27", "$6700", "Vendas", "Porto Alegre", "carolina@email.com", None, "inativo", "Ótimo"),
    (25, "Rafael", "32", "$8000", "Marketing", "Brasília", "rafael@email.com", "2021-08-05", "inativo", "Regular"),
    (26, "Mariano", "28", "$7100", "RH", "Recife", "mariano@email.com", "2022-03-12", "ativo", "Bom"),
    (27, "Tatiane", "30", "$7600", "Financeiro", "Fortaleza", "tatiane@email.com", "2021-12-10", "inativo", "Ruim"),
    (28, "Daniel", "31", "$8300", "TI", "Salvador", "daniel@email.com", "2021-10-25", "ativo", "Regular"),
    (29, "Priscila", "29", "$7900", "Vendas", "Manaus", "priscila@email.com", "2022-01-05", "inativo", "Ótimo"),
    (30, "Vinícius", "35", "$8800", "Marketing", "Curitiba", "vinicius@email.com", "2021-07-15", "ativo", "Bom"),
    (31, "Renata", "28", "$7600", "Vendas", "XYZ", "renata@email.com", "2021-11-30", "ativo", "Bom"),
    (32, "Anderson", "33", "$8200", "TI", "Campinas", "anderson@email.com", "2021-06-25", "inativo", None),
    (33, "Juliana", "XYZ", "$6900", "Marketing", "XYZ", None, "2022-02-10", "ativo", "Regular"),
    (34, "Luciana", "26", "$7000", "RH", "XYZ", "luciana@email.com", None, "inativo", "Ótimo"),
    (35, "Gabriel", "29", "$7800", None, "Brasília", "gabriel@email.com", "2022-04-05", "ativo", "Bom"),
    (36, "Renato", "31", "$8400", "Vendas", "XYZ", "renato@email.com", "2021-09-05", "ativo", "Ruim"),
    (37, None, "30", "$8000", "TI", "XYZ", "email_invalido", "2021-12-20", "inativo", "Regular"),
    (38, "Beatriz", "34", "$9000", "Marketing", "XYZ", "beatriz@email.com", "2021-08-15", None, "Ótimo"),
    (39, "Thiago", "32", "$8700", "RH", "XYZ", "thiago@email.com", "2021-10-01", "ativo", "Bom"),
    (40, "Vanessa", "35", "$9200", "Vendas", "XYZ", "vanessa@email.com", "2021-07-01", "ativo", "Ruim")
])

# Crie o DataFrame com os dados e a estrutura
df = spark.createDataFrame(data, schema=schema)

# Exiba o DataFrame
df.display()



# Tratar datas para o formato "YYYY-MM-DD"
df = df.withColumn("data_contratacao",
    when(df["data_contratacao"].rlike(r'^\d{8}$'), to_date(df["data_contratacao"], 'yyyyMMdd'))
    .when(df["data_contratacao"].rlike(r'^\d{4}/\d{2}/\d{2}$'), to_date(df["data_contratacao"], 'yyyy/MM/dd'))
    .otherwise(to_date(df["data_contratacao"], 'yyyy-MM-dd'))
)

display(df)


# Remover símbolos de dólar
df = df.withColumn("salario", regexp_replace("salario", "\\$", ""))

# Converter coluna "salario" para tipo numérico
df = df.withColumn("salario", col("salario").cast(DoubleType()))

display(df)


# Converter coluna "idade" para tipo numérico
df = df.withColumn("idade", col("idade").cast(IntegerType()))

# Lidar com valores "XYZ"
df = df.withColumn("idade", when(col("idade") == "XYZ", None).otherwise(col("idade")))
display(df)




# Preencher valores ausentes na coluna "data_contratacao" com a data atual
df = df.withColumn("data_contratacao", when(df["data_contratacao"].isNull(), current_date()).otherwise(df["data_contratacao"]))
display(df)

