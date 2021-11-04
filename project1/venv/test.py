import psycopg2
import pandas as pd

DB_USER = "postgres"
DB_PASSWD = "12345"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "my_db"

#connection to db

conn = psycopg2.connect(user = DB_USER, password = DB_PASSWD, host =DB_HOST, port = DB_PORT, database = DB_NAME )
cursor = conn.cursor()

d= cursor.execute(""" SELECT * FROM info """)
d.fetchAll()
print(d)
ppf = pd.DataFrame(d)
print(ppf)