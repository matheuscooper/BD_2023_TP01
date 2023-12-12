DB_HOST = "localhost"
DB_NAME = "Amazon-metadata"
DB_USER = "admin"
DB_PASS = "icomp123"

import psycopg2

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
            id INT,
            grupo VARCHAR(30),
            titulo VARCHAR(1000),
            salesrank BIGINT,
            asin VARCHAR(10) NOT NULL,
            PRIMARY KEY(asin)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Review(
            asin VARCHAR(10) NOT NULL,
            data DATE,
            customer VARCHAR(20),
            rating INT,
            novotes INT,
            nohelpful INT,
            FOREIGN KEY(asin) REFERENCES Produtos(asin)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Categorias(
            catid VARCHAR(20),
            categoria VARCHAR(100)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS Prodcat(
            asin VARCHAR(10),
            catid VARCHAR(20),
            FOREIGN KEY(asin) REFERENCES Produtos(asin)
        )""")

        cursor.execute("""CREATE TABLE IF NOT EXISTS SimilarP(
            asin VARCHAR(10),
            similarasin VARCHAR(10),
            FOREIGN KEY(asin) REFERENCES Produtos(asin)
        )""")

        conn.commit()
        print("Tabelas criadas com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Erro criando tabelas:", error)

    
if __name__ == '__main__':
    conn = connect_db()


    if(conn):
        create_tables(conn)
        conn.close()
        print("PostgreSQL connection is closed")
        

     