from flask import request, jsonify
import pandas as pd
from datetime import datetime as dt
from db_connection import conn

def save_db_source():
    try:
        source = request.json["source"]
        host = request.json["host"]
        port = request.json["port"]
        username = request.json["username"]
        password = request.json["password"]
        db = request.json["db"]
        source_name = request.json["source_name"]
        user_id = request.json["user_id"]

        data = [{
                    "USER_ID": user_id,
                    "HOST": host,
                    "PORT": port,
                    "USER": username,
                    "PASSWORD": password,
                    "DATABASE":db,
                    "CREATED_AT": str(dt.now())[0:23],
                    "SOURCE_NAME": source_name
                }]
        df = pd.DataFrame(data)
        if source == 'mysql':
            rtn = pd.read_sql(f"select 1 from MYSQL_TARGET where TARGET_NAME='{source_name}'", con=conn)
            if len(rtn.index)>0:
                return jsonify({"response":"Failed","err":"Target Name Already Exists",})
            df.to_sql("MYSQL_TARGET", con=conn, index=False, if_exists='append')
            return jsonify({"response":"Success","data":"Connection Successfully Saved",})
        elif source == 'sqlserver':
            rtn = pd.read_sql(f"select 1 from SQLSERVER_SOURCE where SOURCE_NAME='{source_name}' and USER_ID={user_id}", con=conn)
            if len(rtn.index)>0:
                response={"response":"Failed","err":"Target Name Already Exists",}
            df.to_sql("SQLSERVER_SOURCE", con=conn, index=False, if_exists='append')
            response={"response":"Success","data":"Connection Successfully Saved",}
    except Exception as e:
        response={"response":"Failed","err":str(e),}
    finally:
        return jsonify(**response)
