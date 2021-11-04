import pandas as pd
from pandas_profiling import ProfileReport
import datapane as dp
import altair as alt
import csv
import psycopg2
import matplotlib.pyplot as plt




DB_USER = "postgres"
DB_PASSWD = "12345"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "my_db"

#connection to db

conn = psycopg2.connect(user = DB_USER, password = DB_PASSWD, host =DB_HOST, port = DB_PORT, database = DB_NAME )
cursor = conn.cursor()

#drop table if exists
def drop_tables():
    cursor.execute("DROP TABLE data CASCADE ")
    cursor.execute("DROP TABLE IF EXISTS info CASCADE ")
    cursor.execute("DROP TABLE processed CASCADE")
    cursor.execute("DROP VIEW IF EXISTS v_notprocessed CASCADE")
    cursor.execute("DROP VIEW IF EXISTS v_processed CASCADE")
    cursor.execute("DROP VIEW IF EXISTS vone CASCADE")
    cursor.execute("DROP VIEW IF EXISTS vtwo CASCADE")

drop_tables()



#CREATE info table
def create_info_table():
    sql = '''CREATE TABLE info(
       region text,
       exchange text,
       index text,
       currency text
    )'''
    cursor.execute(sql)

def create_data_table():
    sql = '''CREATE TABLE data(
       KEY INT PRIMARY KEY NOT NULL,
       INDEX Text NOT NULL,
       DATE DATE NULL ,
       OPEN Float NULL,
       HIGH Float NULL,
       LOW Float NULL,
       CLOSE Float NULL,
       ADJCLOSE Float NULL,
       VOLUME Float NULL
    )'''
    cursor.execute(sql)

def create_processed_table():
    sql = ''' CREATE TABLE processed (
        index text,
        date date,
        OPEN Float NULL,
        HIGH Float NULL,
        LOW Float NULL,
        CLOSE Float NULL,
        ADJCLOSE Float NULL,
        VOLUME Float NULL,
        CLOSEUSD Float
        )
        '''
    cursor.execute(sql)


create_info_table()
create_data_table()
create_processed_table()
print("Tables created successfully........")

''' 
with open('archive/indexProcessed.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cursor.execute(
        "INSERT INTO processed  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        row
    )
'''

def processed_to_sql():
    with open("archive/indexProcessed.csv", "r", encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        list2 = []
        next(reader)
        for each in reader:
            list2.append(each)
    for each in list2:
        cursor.execute("INSERT INTO processed VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", each)
    conn.commit()


def info_to_sql():
    infoFile = pd.read_csv(r'archive/indexInfo.csv')
    with open('archive/indexInfo.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row.
        for row in reader:
            cursor.execute(
                "INSERT INTO info  VALUES (%s,%s,%s,%s)",
                row
            )
    conn.commit()

""" 
with open('archive/indexData.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        if(row[3]== ''):
            print(row)
            row[3] == None
            row[4] == None
            row[5] == None
            row[6] == None
            row[7] == None
            row[8] == None
            print(row)
        cursor.execute(
        "INSERT INTO data  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        row
    )
conn.commit()
"""

""" 
with open ('archive/indexData.csv', 'r') as f:
    reader = csv.reader(f)
    columns = next(reader)
    query = 'insert into data({0}) values({1})'
    query = query.format(','.join(columns), ','.join('?' * len(columns)))
    for data in reader:
        cursor.execute(query, data)
    cursor.commit()
"""

def data_to_sql():
    dataFile = pd.read_csv(r'archive/indexData.csv')
    df = pd.DataFrame(dataFile)
    df = df.dropna(how='all')
    # (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    # (?,?,?,?,?,?,?,?,?)
    for row in df.itertuples():
        cursor.execute(
            "  INSERT INTO data (key, index, date, open, high, low, close, adjclose, volume) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ",
            (row.key,
             row.index,
             row.date,
             row.open,
             row.high,
             row.low,
             row.close,
             row.adjclose,
             row.volume)
            )
    conn.commit()

def data_to_sql2():
    with open("archive/indexData.csv", "r", encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        list = []
        next(reader)
        for each in reader:
            list.append(each)

    for each in list:
        if (each[3] == ''):
            for i in each:
                if (i == ''):
                    each[each.index(i)] = None
        cursor.execute("INSERT INTO data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", each)

    conn.commit()

""" 
def data_to_sql():
    with open("archive/indexData.csv", "r", encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        list = []
        next(reader)
        for each in reader:
            list.append(each)

    for each in list:
        if (each[3] == ''):
            for i in each:
                if (i == ''):
                    each[each.index(i)] = None
        cursor.execute("INSERT INTO data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", each)

    conn.commit()
"""
info_to_sql()
print("table info populated")
processed_to_sql()
print("table processed populated")
data_to_sql()
print("table data populated")


def views():
    cursor.execute(''' CREATE VIEW V_NOTPROCESSED as 
SELECT DISTINCT d.key, d.date, d.index, i.region 
FROM data d
LEFT JOIN processed p ON (d.date = p.date) AND (d.index = p.index)
JOIN info i on (d.index = i.index)
	WHERE p.date IS NULL''')

    cursor.execute(''' CREATE VIEW V_PROCESSED as
 SELECT EXTRACT(year FROM p.date) AS year,
    TO_CHAR(TO_DATE(EXTRACT(MONTH FROM p.Date)::text, 'MM'), 'Month') AS month,
    max(p.open) AS max,
    min(p.low) AS low,
    i.region,
    i.exchange,
    i.currency
   FROM processed p
     JOIN info i ON i.index = p.index
  GROUP BY i.region, (EXTRACT(year FROM p.date)), (EXTRACT(month FROM p.date)), i.exchange, i.currency;''')


    cursor.execute(''' CREATE VIEW vone AS  SELECT v.month,
    v.region,
    max(v.max) AS max
   FROM v_processed v
  WHERE v.region = 'Germany'::text
  GROUP BY v.month, v.region;''')

    cursor.execute(''' CREATE VIEW vtwo AS
SELECT v.month,
    v.region,
     min(v.low) AS low
   FROM v_processed v
  WHERE v.region = 'Germany'::text
  GROUP BY v.month, v.region''')

views()

def report_one():

    cursor.execute(''' SELECT * FROM vone  ''')
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    df.columns = ['month', 'region', 'max']
    df.plot(x='month', y='max', kind='line')
    plt.savefig("mygraph.png")
    plt.close("all")

def report_two():
    cursor.execute(''' SELECT * FROM vtwo  ''')
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    df.columns = ['month', 'region', 'min']
    df.plot(x='month', y='min', kind='line')
    plt.savefig("mygraph2.png")
    plt.close("all")

report_one()
report_two()

#print(w)
#result2.describe(include='all')
#print(result2)
#profile = ProfileReport(result2)
#profile.to_notebook_iframe()

#profile.to_notebook_iframe()
#profile.to_file("Analysis2.html")
#for r in result:
#    print(r)
conn.commit()

# (key, index, date, open, high, low, close, adjclose, volume)
cursor.close()
conn.close()