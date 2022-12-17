import sqlalchemy
import pandas as pd

def mysql_con(host, port, user, password, db):
    try:
        url = sqlalchemy.engine.URL.create(
                "mysql",
                username=user,
                password=password,
                host=host,
                port=port,
                database=db,
                # query={"driver": "ODBC Driver 17 for SQL Server"},
            )
        print(url)
        conn = sqlalchemy.create_engine(url)
        result = pd.read_sql('select 1', con=conn)
        return {"status":True, "connection":conn}
    except Exception as e:
        return {"status":False, "err":str(e)}

def sqlserver_con(host, port, user, password, db):
    try:
        url = sqlalchemy.engine.URL.create(
                "mssql+pyodbc",
                username=user,
                password=password,
                host=host,
                port=port,
                database=db,
                query={"driver": "ODBC Driver 17 for SQL Server"},
            )
        # print(url)
        conn = sqlalchemy.create_engine(url)
        result = pd.read_sql('select 1', con=conn)
        return {"status":True, "connection":conn}
    except Exception as e:
        return {"status":False, "err":str(e)}