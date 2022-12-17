from flask import request, jsonify
from db_connection import conn
import pandas as pd
from datetime import datetime as dt
import cx_Oracle

def test_save_oracle_source():
    if request.method == 'POST':
        if request.json["host"] and  request.json["port"] and request.json["user"] and  request.json["password"] \
             and  request.json["service"] and  request.json["table_name"] and  request.json["source_name"] and request.json["user_id"]:
            try:
                host=request.json["host"]
                port= int(request.json["port"])
                user= request.json["user"]
                password= request.json["password"]
                service = request.json["service"]
                table_name = request.json["table_name"]
                source_name = request.json["source_name"]
                user_id = request.json["user_id"]

                rtn = pd.read_sql(f"SELECT 1 FROM [ORACLE_DB_SOURCE] WHERE [SOURCE_NAME]='{source_name}'", con= conn)
                if len(rtn.index)>0:
                    return jsonify({"status":"Failed", "err":"Source already exists"})

                CONN_INFO = {
                    'host': host,
                    'port': port,
                    'user': user,
                    'psw': password,
                    'service': service
                }
                # print (CONN_INFO)

                # CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO)
                # orcl_conn = cx_Oracle.connect(CONN_STR)

                data = [{
                    "USER_ID":user_id,
                    "SOURCE_NAME":source_name,
                    "HOST":host,
                    "PORT":port,
                    "USER":user,
                    "PASSWORD":password,
                    "SERVICE": service,
                    "TABLE_NAME":table_name,                  
                    "CREATED_AT": str(dt.now())[0:23]
                }]
                df = pd.DataFrame(data)
                
                df.to_sql("ORACLE_DB_SOURCE", con= conn, if_exists='append', index=False)

                return jsonify({"status":"ok","msg":"Connection established successfully and saved"})
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})