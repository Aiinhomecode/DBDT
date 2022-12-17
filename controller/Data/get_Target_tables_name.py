from flask import request, jsonify
import pandas as pd
from db_connection import conn
from .make_db_connection import mysql_con, sqlserver_con

def get_Target_tables_name():
    if request.method == 'POST':
        try:
            if request.json['target_id']:
                target = (request.json['target_id']).split('_')[0]
                id = (request.json['target_id']).split('_')[-1]

                if target.lower() == 'mysql':
                    df = pd.read_sql(f"select * from MYSQL_TARGET where [ID]={id}", con= conn)
                    host = df['HOST'][0]
                    port = df['PORT'][0]
                    user = df['USER'][0]
                    password = df['PASSWORD'][0]
                    db = df['DATABASE'][0]

                    mysql_conn = mysql_con(host, port, user, password, db)
                    if mysql_conn['status']:
                        new_con = mysql_conn['connection']
                        df_tables = pd.read_sql("show tables", con = new_con)
                        df_tables.columns = ['table_name']
                        table_dict = df_tables.to_dict(orient='records')
                        out_data = {
                            "status":"Success",
                            "data": table_dict,
                        }
                elif target.lower() == 'sqlserver':
                    df = pd.read_sql(f"select * from SQLSERVER_TARGET where [ID]={id}", con= conn)
                    host = df['HOST'][0]
                    port = df['PORT'][0]
                    user = df['USER'][0]
                    password = df['PASSWORD'][0]
                    db = df['DATABASE'][0]

                    mysql_conn = sqlserver_con(host, port, user, password, db)
                    if mysql_conn['status']:
                        new_con = mysql_conn['connection']
                        df_tables = pd.read_sql("select [TABLE_NAME] from INFORMATION_SCHEMA.TABLES where [TABLE_TYPE]='BASE TABLE'", con = new_con)
                        df_tables.columns = ['table_name']
                        table_dict = df_tables.to_dict(orient='records')
                        out_data = {
                            "status":"Success",
                            "data": table_dict,
                        }

                return jsonify(**out_data)
        except Exception as e:
            return jsonify({"status":"Failed","arr":"Something went wrong", "m": str(e)})

    return jsonify({"status":"Failed","msg":"Method not allowed"})