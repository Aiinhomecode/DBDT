import sqlalchemy
import pandas as pd
import urllib

url = sqlalchemy.engine.URL.create(
    "mssql+pyodbc",
    username="DCTDeveloper",
    password="developer@123",
    host="HP-SERVER\MSSQLSERVER02",
    database="DBDT",
    query={"driver": "ODBC Driver 17 for SQL Server"},
)


conn = sqlalchemy.create_engine(url)

df = pd.read_sql('select 1', con= conn)