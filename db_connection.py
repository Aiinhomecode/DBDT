import sqlalchemy
import pandas as pd
import urllib

url = sqlalchemy.engine.URL.create(
    "mssql+pyodbc",
    username="DCTDeveloper",
    password="developer@123",
    host="122.163.121.176",
    port='3050',
    database="DBDT",
    query={"driver": "ODBC Driver 17 for SQL Server"},
)


conn = sqlalchemy.create_engine(url)

df = pd.read_sql('select 1', con= conn)


# params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+'HP-SERVER\MSSQLSERVER02'+';DATABASE='+'DBDT'+';UID='+'DCTDeveloper'+';PWD='+ "developer@123")
# conn = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

