DB_HOST = "localhost"
DB_NAME = "Amazon-metadata"
DB_USER = "admin"
DB_PASS = "icomp123"
     
import psycopg2
import pandas as pd

conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

cursor = conn.cursor()  

#Todas as consultas requeridas estão listadas a baixo

#a)consuta a listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação
cursor.execute("""  
""")

#b)Dado um produto, listar os produtos similares com maiores vendas do que ele
cursor.execute("""
""")

#c)Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
cursor.execute("""
""")

#d)Listar os 10 produtos líderes de venda em cada grupo de produtos
cursor.execute("""
""")

#e)Listar os 10 produtos com a maior média de avaliações úteis positivas por produto
cursor.execute("""
""")

#f)Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto
cursor.execute("""
""")

#g)Listar os 10 clientes que mais fizeram comentários por grupo de produto
cursor.execute("""
""")


cursor.close()

conn.close()
