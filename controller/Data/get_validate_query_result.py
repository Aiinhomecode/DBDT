from flask import request, jsonify
import pandas as pd
import json

def get_validate_query_result():
    try:
        if request.method == 'POST':
            if 'target_id'and'query'and'mode' in request.json:
                target_id = request.json['target_id']
                query = request.json['query']
                mode = request.json['mode']

                target_conn = get_conn_of_target(target_id)
                if target_conn["status"]:
                    if mode.lower() == 'test':
                        df = (pd.read_sql(query, con= target_conn["connection"], chunksize=10))
                        df_count = pd.read_sql_query(f'select count(1) as cnt from ({query}) as tbl', con=target_conn["connection"])
                        
                        for chunk in df:
                            df = pd.DataFrame(chunk)
                            break
                    else:
                        df = (pd.read_sql(query, con= target_conn["connection"]))
                        df_count = pd.read_sql_query(f'select count(1) as cnt from ({query}) as tbl', con=target_conn["connection"])

                    df_dict = df.to_json(orient='records', date_format='iso')
                    result_dict = {
                        "status":"success",
                        "data":json.loads(df_dict),
                        "records_count": int(df_count['cnt'][0]),
                        "columns":list(df.columns)
                    }
            else:
                result_dict = {
                        "status":"failed",
                        "err":"Bodypart missing",
                    }
        else:
            result_dict = {
                            "status":"failed",
                            "err":"Method not allowed",
                        }
    except Exception as e:
        result_dict = {
                        "status":"failed",
                        "err":str(e.args).split('"')[1],
                    }
    finally:
        return jsonify(**result_dict)

from .make_db_connection import mysql_con, sqlserver_con
from db_connection import conn
import sqlalchemy
def get_conn_of_target(target_id):
    target, id = target_id.split('_')
    if target.lower() == 'sqlserver':
        host, port, user, password, db = get_credentials('SQLSERVER_TARGET', id)
        sqlserver_conn = sqlserver_con(host,port,user,password,db)
        return sqlserver_conn
    elif target.lower() == 'mysql':
        host, port, user, password, db = get_credentials('MYSQL_TARGET', id)
        mysql_conn = mysql_con(host,port,user,password,db)
        return mysql_conn

def get_credentials(table_name, id):
    df = pd.read_sql(f"select * from [{table_name}] where [id]={id}", con=conn)
    host = df['HOST'][0]
    port = df['PORT'][0]
    user = df['USER'][0]
    password = df['PASSWORD'][0]
    db = df['DATABASE'][0]
    return host, port, user, password, db