DB_HOST = "localhost"
DB_NAME = "amazon"
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
            grupo VARCHAR(100),
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
            nohelpful INT
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Categorias(
            asin VARCHAR(10),
            categoria VARCHAR(200),
            PRIMARY KEY (asin, categoria),
            FOREIGN KEY (asin) REFERENCES Produtos (asin)        
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
            current_block = ""
            started = False  # Indica se já começou a leitura dos blocos
            for line in file:
                line = line.strip()
                # Verifica se a linha começa com "Id:"
                if line.startswith("Id:"):
                    # Se já começou, extrai o ASIN e a lista de ASINs similares
                    if started:
                        get_produto(cursor, current_block)
                        get_reviews(cursor, current_block)
                        get_categoria(cursor, current_block)
                    # Começa um novo bloco
                    current_block = line
                    started = True
                else:
                    # Adiciona a linha ao bloco atual
                    current_block += "\n" + line
            # Imprime o último bloco (se houver algum)
            if started:
                get_produto(cursor, current_block)
                get_reviews(cursor, current_block)
                get_categoria(cursor, current_block)
        conn.commit()
        print("Dados inseridos com sucesso.")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro inserindo dados:", error)

def get_produto(cursor, block):
    match_asin = re.search(r'ASIN: (\w+)', block)
    asin = match_asin.group(1) if match_asin else None

    match_id = re.search(r'Id:\s*(\d+)', block)
    id_value = int(match_id.group(1)) if match_id else 0

    match_grupo = re.search(r'group: (.+)', block)
    grupo = match_grupo.group(1) if match_grupo else None

    match_titulo = re.search(r'title: (.+)', block)
    titulo = match_titulo.group(1) if match_titulo else None

    match_salesrank = re.search(r'salesrank: (\d+)', block)
    salesrank = int(match_salesrank.group(1)) if match_salesrank else None

    if not is_discontinued(block):
        # Insira os dados na tabela de produtos apenas se não estiver descontinuado

        '''print("##############")
        print("ID:", id_value)
        print("ASIN:", asin)
        print("Grupo:", grupo)
        print("Título:", titulo)
        print("Salesrank:", salesrank)'''

        cursor.execute("""
            INSERT INTO Produtos (id, grupo, titulo, salesrank, asin)
            VALUES (%s, %s, %s, %s, %s)
        """, (id_value, grupo, titulo, salesrank, asin))

def is_discontinued(block):
    return "discontinued product" in block.lower()




def get_reviews(cursor, block):
    match_asin = re.search(r'ASIN: (\w+)', block)
    asin = match_asin.group(1) if match_asin else None

    lines = block.splitlines()

    collecting_reviews = False
    for line in lines:
        if line.startswith("reviews: total"):
            collecting_reviews = True
            continue  # Pula esta linha, pois já sabemos o total de avaliações
        if collecting_reviews:
            line = line.split()
            date = line[0]
            customer = line[2]
            rating = line[4]
            votes = line[6]
            helpful = line[8]

            '''print("Date:", date)
            print("Customer:", customer)
            print("Rating:", rating)
            print("Votes:", votes)
            print("Helpful:", helpful)
            print("----------")'''

            cursor.execute("""
                INSERT INTO Review (asin, data, customer, rating, novotes, nohelpful)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (asin, date, customer, rating, votes, helpful))



def get_categoria(cursor, block):
    match_asin = re.search(r'ASIN: (\w+)', block)
    asin = match_asin.group(1) if match_asin else None
    lines = block.splitlines()

    collecting_categoria = False
    collecting_review = False

    for line in lines:
        if line.startswith("categories:"):
            collecting_categoria = True
            print("ASIN:", asin)
            continue  # Pula esta linha, pois já sabemos o total de avaliações
        if line.startswith("reviews: total"):
            collecting_review = True
            break
            continue  # Pula esta linha, pois já sabemos o total de avaliações
        if collecting_categoria and not collecting_review:
            print("categoria", line)

            cursor.execute("""
                INSERT INTO Categorias (asin, categoria)
                VALUES (%s, %s)
            """, (asin, line))


if __name__ == '__main__':
    conn = connect_db()

    if conn:
        # Criar tabelas e popular
        create_tables(conn)
        data_file_path = "amazon2.txt"
        populate_tables(conn, data_file_path)
        
        conn.close()
        print("PostgreSQL connection is closed")
    