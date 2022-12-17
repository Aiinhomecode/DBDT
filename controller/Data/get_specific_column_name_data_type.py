from flask import request, jsonify
import pandas as pd
from db_connection import conn
from .make_db_connection import mysql_con, sqlserver_con

def get_Target_table_specific_columns_name_data_type():
    if request.method == 'POST':
        try:
            if request.json['target_id'] and  request.json['table_name'] and request.json['column_name']:
                target = (request.json['target_id']).split('_')[0]
                id = (request.json['target_id']).split('_')[-1]
                table_name = request.json['table_name']
                column_name = request.json['column_name']

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
                        result = column_name.strip("[]")
                        resp=list([f'{result}'])
                        for val in resp:
                            sql_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+table_name+"' AND COLUMN_NAME in ("+val+");"
                            df_tables = pd.read_sql(sql_query, con = new_con)
                            table_dict = df_tables.to_dict(orient='records')
                            out_data = {
                                "status":"Success",
                                "data": table_dict
                            }
                elif target.lower() == 'sqlserver':
                    df = pd.read_sql(f"select * from SQLSERVER_TARGET where [ID]={id}", con= conn)
                    host = df['HOST'][0]
                    port = df['PORT'][0]
                    user = df['USER'][0]
                    password = df['PASSWORD'][0]
                    db = df['DATABASE'][0]
                    sqlserver_conn = sqlserver_con(host, port, user, password, db)
                    if sqlserver_conn['status']:
                        new_con = sqlserver_conn['connection']
                        result = column_name.strip("[]")
                        resp=list([f'{result}'])
                        for val in resp:
                            sql_query = "SELECT [COLUMN_NAME], [DATA_TYPE] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+table_name+"' AND COLUMN_NAME in ("+val+")"                            
                            df_tables = pd.read_sql(sql_query, con = new_con)
                            table_dict = df_tables.to_dict(orient='records')
                            out_data = {
                                "status":"Success",
                                "data": table_dict
                            } 
            return jsonify(**out_data)
        except Exception as e:
            return jsonify({"status":"Failed","arr":"Something went wrong", "m": str(e)})

    return jsonify({"status":"Failed","msg":"Method not allowed"})