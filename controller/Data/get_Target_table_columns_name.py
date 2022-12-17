from flask import request, jsonify
import pandas as pd
from db_connection import conn
from .make_db_connection import mysql_con, sqlserver_con

def get_Target_table_columns_name():
    if request.method == 'POST':
        try:
            if request.json['target_id'] and  request.json['table_name']:
                target = (request.json['target_id']).split('_')[0]
                id = (request.json['target_id']).split('_')[-1]
                table_name = request.json['table_name']

                if target.lower() == 'mysql':
                    df = pd.read_sql(f"select * from MYSQL_TARGET where [ID]={id}", con= conn)
                    host = df['HOST'][0]
                    port = df['PORT'][0]
                    user = df['USER'][0]
                    password = df['PASSWORD'][0]
                    db = df['DATABASE'][0]
                    # out_data = {}
                    mysql_conn = mysql_con(host, port, user, password, db)
                    if mysql_conn['status']:
                        new_con = mysql_conn['connection']
                        sql_query = "SELECT [COLUMN_NAME], [DATA_TYPE] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+table_name+"';"
                        df_tables = pd.read_sql(sql_query, con = new_con)
                        #df_tables.columns = ['column_name']
                        table_dict = df_tables.to_dict(orient='records')
                        out_data = {
                            "status":"Success",
                            "data": table_dict
                        }
                elif target.lower() == 'sqlserver':
                    df = pd.read_sql(f"select * from SQLSERVER_TARGET where [ID]={id}", con= conn)
                    # print(df)
                    host = df['HOST'][0]
                    port = df['PORT'][0]
                    user = df['USER'][0]
                    password = df['PASSWORD'][0]
                    db = df['DATABASE'][0]
                    # print(host,port,user,password,db)
                    out_data = {}
                    sqlserver_conn = sqlserver_con(host, port, user, password, db)
                    # df = pd.read_sql(table_name,conn)
                    #     # df_tables.columns = ['table_name']
                    # resp=list(df.columns)
                    # out_data = {
                    #         "status":"Success",
                    #         "data": resp
                    #     } 
                    if sqlserver_conn['status']:
                        new_con = sqlserver_conn['connection']
                        sql_query =   f"SELECT [COLUMN_NAME], [DATA_TYPE] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '"+table_name+"'"
                        # print (sql_query)
                        df_tables = pd.read_sql(sql_query, con = new_con)
                        # print (df_tables)
                        #df_tables.columns = ['column_name']
                        table_dict = df_tables.to_dict(orient='records')
    
                        out_data = {
                            "status":"Success",
                            "data": table_dict
                        } 
            return jsonify(**out_data)
        except Exception as e:
            return jsonify({"status":"Failed","arr":"Something went wrong", "m": str(e)})

    return jsonify({"status":"Failed","msg":"Method not allowed"})