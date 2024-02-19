from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum, mean as mean, count as count, max, desc

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from faker import Faker

#IMPORTANTE DAR !pip install faker


fake = Faker()
# Inicialize a Spark Session
spark = SparkSession.builder.appName("exemplo-vendas-oop").getOrCreate()

# Classe para representar um Cliente
class Cliente:
    def __init__(self, cliente_id, nome, endereco):
        self.cliente_id = cliente_id
        self.nome = nome
        self.endereco = endereco

# Classe para representar um Produto
class Produto:
    def __init__(self, produto_id, nome, preco):
        self.produto_id = produto_id
        self.nome = nome
        self.preco = preco

# Classe para representar uma Transação de Venda
class Transacao:
    def __init__(self, cliente, produto, quantidade):
        self.cliente = cliente
        self.produto = produto
        self.quantidade = quantidade
        self.valor_total = produto.preco * quantidade

cliente1 = Cliente(1, fake.name(), fake.address())
cliente2 = Cliente(2, fake.name(), fake.address())
cliente3 = Cliente(3, fake.name(), fake.address())
cliente4 = Cliente(4, fake.name(), fake.address())
cliente5 = Cliente(5, fake.name(), fake.address())
cliente6 = Cliente(6, fake.name(), fake.address())

# Criar instâncias de Produto
produto1 = Produto(101, "ProdutoX", 50.0)
produto2 = Produto(102, "ProdutoY", 75.0)
produto3 = Produto(103, "ProdutoZ", 60.0)
produto4 = Produto(104, "ProdutoW", 90.0)
produto5 = Produto(105, "ProdutoV", 80.0)
produto6 = Produto(106, "ProdutoU", 120.0)

# Criar instâncias de Transacao
transacao1 = Transacao(cliente1, produto1, 2)
transacao2 = Transacao(cliente2, produto2, 3)
transacao3 = Transacao(cliente1, produto2, 1)
transacao4 = Transacao(cliente3, produto3, 4)
transacao5 = Transacao(cliente4, produto4, 2)
transacao6 = Transacao(cliente3, produto4, 3)
transacao7 = Transacao(cliente5, produto5, 5)
transacao8 = Transacao(cliente6, produto6, 2)
transacao9 = Transacao(cliente5, produto6, 4)

# Criar DataFrame de transações
data = [
    (transacao1.cliente.cliente_id, transacao1.produto.produto_id, transacao1.quantidade, transacao1.valor_total),
    (transacao2.cliente.cliente_id, transacao2.produto.produto_id, transacao2.quantidade, transacao2.valor_total),
    (transacao3.cliente.cliente_id, transacao3.produto.produto_id, transacao3.quantidade, transacao3.valor_total),
    (transacao4.cliente.cliente_id, transacao4.produto.produto_id, transacao4.quantidade, transacao4.valor_total),
    (transacao5.cliente.cliente_id, transacao5.produto.produto_id, transacao5.quantidade, transacao5.valor_total),
    (transacao6.cliente.cliente_id, transacao6.produto.produto_id, transacao6.quantidade, transacao6.valor_total),
    (transacao7.cliente.cliente_id, transacao7.produto.produto_id, transacao7.quantidade, transacao7.valor_total),
    (transacao8.cliente.cliente_id, transacao8.produto.produto_id, transacao8.quantidade, transacao8.valor_total),
    (transacao9.cliente.cliente_id, transacao9.produto.produto_id, transacao9.quantidade, transacao9.valor_total),
]

columns = ["Cliente_ID", "Produto_ID", "Quantidade", "Valor_Total"]
df_transacoes = spark.createDataFrame(data, columns)

display(df_transacoes)

# Análise 1: Total de vendas por cliente
total_vendas_por_cliente = df_transacoes.groupBy("Cliente_ID").agg({"Valor_Total": "sum"}).withColumnRenamed("sum(Valor_Total)", "Total_Vendas")
display(total_vendas_por_cliente)

# Análise 2: Número de transações por cliente
transacoes_por_cliente = df_transacoes.groupBy("Cliente_ID").agg({"Cliente_ID": "count"}).withColumnRenamed("count(Cliente_ID)", "Numero_Transacoes")
display(transacoes_por_cliente)

# Análise 3: Média de vendas por cliente
media_vendas_por_cliente = df_transacoes.groupBy("Cliente_ID").agg({"Valor_Total": "mean"}).withColumnRenamed("avg(Valor_Total)", "Media_Vendas")
display(media_vendas_por_cliente)

# Análise 4: Valor total de vendas por produto
total_vendas_por_produto = df_transacoes.groupBy("Produto_ID").agg({"Valor_Total": "sum"}).withColumnRenamed("sum(Valor_Total)", "Total_Vendas")
display(total_vendas_por_produto)

# Análise 5: Cliente que mais gastou
cliente_mais_gastou = df_transacoes.groupBy("Cliente_ID", "Produto_ID").agg({"Valor_Total": "sum"}).groupBy("Cliente_ID").agg({"sum(Valor_Total)": "max"}).withColumnRenamed("max(sum(Valor_Total))", "Maior_Gasto")
display(cliente_mais_gastou)

# Análise 6: Produtos mais vendidos em quantidade
produtos_mais_vendidos = df_transacoes.groupBy("Produto_ID").agg({"Quantidade": "sum"}).orderBy(desc("sum(Quantidade)")).withColumnRenamed("sum(Quantidade)", "Total_Vendido")
display(produtos_mais_vendidos)