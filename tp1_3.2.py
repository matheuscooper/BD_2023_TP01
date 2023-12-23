DB_HOST = "localhost"
DB_NAME = "Amazon-metadata"
DB_USER = "admin"
DB_PASS = "icomp123"

import re
from datetime import datetime
import psycopg2
import numpy as np
def connect_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        return conn

       
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None
   

def create_tables(conn):
    try:
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS Produtos(
            id INT UNIQUE,
            grupo VARCHAR(30),
            titulo VARCHAR(1000),
            salesrank BIGINT,
            asin VARCHAR(10) NOT NULL,
            PRIMARY KEY(asin)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Review(
            asin VARCHAR(10) NOT NULL,
            data DATE,
            customer VARCHAR(100),
            rating INT,
            novotes INT,
            nohelpful INT,
            FOREIGN KEY(asin) REFERENCES Produtos(asin)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Categorias(
            catid VARCHAR(20),
            categoria VARCHAR(200),
            PRIMARY KEY (catid),
            UNIQUE (categoria)           
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Prodcat(
            asin VARCHAR(10),
            catid VARCHAR(20),
            PRIMARY KEY (asin, catid),
            FOREIGN KEY (asin) REFERENCES Produtos (asin),
            FOREIGN KEY (catid) REFERENCES Categorias (catid)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS SimilarP(
            asin VARCHAR(10),
            similarasin VARCHAR(10),
            PRIMARY KEY (asin, similarasin),
            FOREIGN KEY (asin) REFERENCES Produtos (asin),
            FOREIGN KEY (similarasin) REFERENCES Produtos (asin)
        )""")

        conn.commit()
        print("Tabelas criadas com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro criando tabelas:", error)


def populate_tables(conn, data_file_path):
    try:
        cursor = conn.cursor()

        with open(data_file_path, 'r', encoding='utf-8') as file:
            current_product = None

            for line in file:
                line = line.strip()

                if line.startswith("Id:"):
                    if current_product:
                        # Inserir dados na tabela 'Produtos'
                        cursor.execute("""
                            INSERT INTO Produtos (id, grupo, titulo, salesrank, asin)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (current_product['Id'], current_product.get('group', ''), current_product.get('title', ''), current_product.get('salesrank', 0), current_product.get('ASIN', '')))

                    current_product = {'Id': int(re.search(r'\d+', line).group())}

                elif line.startswith("ASIN:"):
                    current_product['ASIN'] = line.split(":")[1].strip()

                elif line.startswith("  title:"):
                    current_product['title'] = line.split(":")[1].strip()

                elif line.startswith("  group:"):
                    current_product['group'] = line.split(":")[1].strip()

                elif line.startswith("  salesrank:"):
                    current_product['salesrank'] = int(re.search(r'\d+', line).group())

                elif line.startswith("    "):
                    # Linhas com dados de revisão
                    review_data = line.split()
                    date_str = review_data[0]
                    date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    customer = review_data[2]
                    rating = int(review_data[4])
                    votes = int(review_data[6])
                    helpful = int(review_data[8])

                    # Inserir dados na tabela 'Review'
                    cursor.execute("""
                        INSERT INTO Review (asin, data, customer, rating, novotes, nohelpful)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (current_product['ASIN'], date, customer, rating, votes, helpful))

            # Inserir o último produto
            if current_product:
                cursor.execute("""
                    INSERT INTO Produtos (id, grupo, titulo, salesrank, asin)
                    VALUES (%s, %s, %s, %s, %s)
                """, (current_product['Id'], current_product.get('group', ''), current_product.get('title', ''), current_product.get('salesrank', 0), current_product.get('ASIN', '')))

        conn.commit()
        print("Dados inseridos com sucesso.")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro inserindo dados:", error)

def read_data_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.readlines()
        return data
    except FileNotFoundError:
        print("Arquivo não encontrado.")
        return None
    except Exception as e:
        print("Ocorreu um erro durante a leitura do arquivo:", e)
        return None

##Função para extrair dados  da tabela Produtos 
def extrair_produtos(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Produtos")
        produtos = cursor.fetchall()
        return produtos
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erros para a tabela Produtos", error)
        return []


##Funcao para extrair dados da tabela Review

def extrair_review(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Review")
        categorias = cursor.fetchall()
        return categorias
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error para a tabela Review", error) 
        return [] 


# Função para extrair dados da tabela 'Categorias'
def extrair_categorias(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Categorias")
        categorias = cursor.fetchall()
        return categorias
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error para a tabela Categorias", error)
        return []        

##Funcao para extrair dados da tabela Prodcat

def extrair_prodcat(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM Prodcat")
        prodcat = cursor.fetchall()
        return prodcat 
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error para a tabela Prodcat", error) 
        return []

##Funcao para extrair dados da tabela Prodcat

def extrair_similarp(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM SimilarP")
        similiarp = cursor.fetchall()
        return similiarp 
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error para a tabela SimilarP", error) 
        return []            
        
    
if __name__ == '__main__':

    data_file_path = "amazon-meta.txt" 

    data = read_data_file(data_file_path)

    if data is not None:
        print("Conteúdo do arquivo lido com sucesso:")


    conn = connect_db()


    if(conn):
        create_tables(conn)
        data_file_path = "amazon-meta.txt"
        populate_tables(conn, data_file_path)
        conn.close()
        print("PostgreSQL connection is closed")

    if(conn):
        produtos = extrair_produtos(conn)
        reviews = extrair_review(conn)
        categorias = extrair_categorias(conn)
        prodcat = extrair_prodcat(conn)
        similiarp = extrair_similarp(conn)

    