import psycopg2
def presdb():
    try:
        conn = psycopg2.connect(host="localhost",dbname="amazon",user="tb01", password="123")
        cur = conn.cursor()
        ##cur.execute()
#process the result set (cursor)
       
    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while connecting to PostgreSQL", error)
    #closing database connection.
    if(conn):
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
        

if __name__ == '__main__':
    presdb()      