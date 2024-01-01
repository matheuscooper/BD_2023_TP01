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
(SELECT titulo, rating, nohelpful
FROM produtos, review
WHERE produtos.asin=review.asin
ORDER BY nohelpful DESC, rating DESC
LIMIT 5)
               
UNION ALL
               
(SELECT titulo, rating, nohelpful
FROM produtos, review
WHERE produtos.asin=review.asin
ORDER BY nohelpful DESC, rating ASC
LIMIT 5);
""")

#b)Dado um produto, listar os produtos similares com maiores vendas do que ele
cursor.execute("""
SELECT p.titulo, asin, 
FROM Produtos p, Similarp s
WHERE p.asin=s.asin AND p.salesrank < (
               SELECT salesrank
               FROM Produtos
               WHERE Produtos.asin='0486220125'
);
""")


#c)Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
cursor.execute("""
SELECT data, AVG(rating)
FROM Review
GROUP BY data
ORDER BY data;
""")

#d)Listar os 10 produtos líderes de venda em cada grupo de produtos
cursor.execute("""
WITH ProdutosOrdenados AS (
    SELECT
        id,
        grupo,
        titulo,
        salesrank,
        asin,
        ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY salesrank) AS ranking_vendas
    FROM
        Produtos
)
SELECT
    grupo,
    titulo
FROM
    ProdutosOrdenados
WHERE
    ranking_vendas <= 10;
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
