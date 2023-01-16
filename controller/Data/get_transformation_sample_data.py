from flask import request, jsonify
import pandas as pd

def get_transformation_sample_data():
    if request.method == 'POST':
        try: 
            if 'target_id'and'table_name' in request.json:
                target_id = request.json['target_id']
                table_name = request.json['table_name']
                # print(table_name)
                target_conn,host, port, user, password, db = get_conn_of_target(target_id)
                # print(host, port, user, password, db)
                object_name=table_name.split(".");
                sanitized_tablename = object_name[1].replace('[','');
                schema_without_tlbname = sanitized_tablename.replace(']','');
                if target_conn["status"]:
                    df = (pd.read_sql(f'select top(10) * from {table_name} ', con= target_conn["connection"], chunksize=10))
                    # print(df)
                    df_count = pd.read_sql_query(f'select count(1) as cnt from {table_name}', con= target_conn["connection"])
                    table_info = pd.read_sql_query(f"""SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT AS DEFAULT_VALUE,CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS where TABLE_CATALOG = '{db}'  AND TABLE_NAME='{schema_without_tlbname}'""", con= target_conn["connection"])     
                    table_info = table_info.to_json(orient='records', date_format='iso')
                    for chunk in df:
                        df = pd.DataFrame(chunk)
                        break
                    df.fillna('', inplace=True)
                    df_dict = df.to_dict(orient='records')
                    result_dict = {
                        "status":"success",
                        "data":df_dict,
                        "table_info":table_info,
                        "records_count": int(df_count['cnt'][0]),
                        "columns":list(df.columns)
                    }
                return jsonify(**result_dict)
            else:
                return jsonify({"status":"Failed","err":"Bodypart missing"})
        except Exception as e:
            print(str(e))
            return jsonify({"status":"Failed","err":"Something went worng"})
    return jsonify({"status":"Failed","err":"Method not allowed"})


from .make_db_connection import mysql_con, sqlserver_con
from db_connection import conn
import sqlalchemy
def get_conn_of_target(target_id):
    target, id = target_id.split('_')
    if target.lower() == 'sqlserver':
        host, port, user, password, db = get_credentials('SQLSERVER_TARGET', id)
        sqlserver_conn = sqlserver_con(host,port,user,password,db)
        return sqlserver_conn ,host, port, user, password, db
    elif target.lower() == 'mysql':
        host, port, user, password, db = get_credentials('MYSQL_TARGET', id)
        mysql_conn = mysql_con(host,port,user,password,db)
        return mysql_conn, host, port, user, password, db
def get_credentials(table_name, id):
    df = pd.read_sql(f"select * from [{table_name}] where [id]={id}", con=conn)
    host = df['HOST'][0]
    port = df['PORT'][0]
    user = df['USER'][0]
    password = df['PASSWORD'][0]
    db = df['DATABASE'][0]
    return host, port, user, password, db