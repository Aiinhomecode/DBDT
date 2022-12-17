from flask import request, jsonify
import pandas as pd
from db_connection import conn
from .make_db_connection import mysql_con, sqlserver_con

def get_visualisation_aggregate_colums():
    if request.method == 'POST':
        try:
            if request.json['target_id'] and request.json["column_name_1"] and request.json["column_name_2"] and request.json["table_name"] and request.json["aggregate_function"]:
                target = (request.json['target_id']).split('_')[0]
                id = (request.json['target_id']).split('_')[-1]
                column_name_1 = request.json["column_name_1"]
                column_name_2 = request.json["column_name_2"]
                table_name = request.json["table_name"]
                aggregate_function = request.json["aggregate_function"]
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
                        user_query=f"select [{column_name_1}],{aggregate_function}([{column_name_2}]) AS Total_Quantity from [{table_name}] group by [{column_name_1}]"
                        df = pd.read_sql(user_query, con= new_con)
                        df_final=df.values.tolist()
                        df_final.insert(0,[column_name_1,column_name_2])
                        # print(df_final)
                        final_data = {
                            "status":"Success",
                            "data1": df_final
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
                        user_query=f"select [{column_name_1}],{aggregate_function}([{column_name_2}]) AS Total_Quantity from [{table_name}] group by [{column_name_1}]"
                        df = pd.read_sql(user_query, con= new_con)
                        df_final=df.values.tolist()
                        df_final.insert(0,[column_name_1,column_name_2])
                        # print(df_final)
                        final_data = {
                            "status":"Success",
                            "data1": df_final
                        }      
            return jsonify(**final_data)
        except Exception as e:
                response={'response': "Failed","err": str(e)}
                return jsonify(response)
