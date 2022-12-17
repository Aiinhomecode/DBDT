from flask import request, jsonify
from datetime import datetime as dt
import pandas as pd
from db_connection import conn

def save_target_details():
    if request.method == 'POST':
            if request.json["target"] and  request.json["host"] and request.json["port"] and request.json["username"] \
                and request.json["password"] and request.json["db"] and request.json["target_name"] and request.json["user_id"]:
                target = request.json["target"]
                host = request.json["host"]
                port = request.json["port"]
                username = request.json["username"]
                password = request.json["password"]
                db = request.json["db"]
                target_name = request.json["target_name"]
                user_id = request.json["user_id"]

                data = [{
                    "USER_ID": user_id,
                    "HOST": host,
                    "PORT": port,
                    "USER": username,
                    "PASSWORD": password,
                    "DATABASE":db,
                    "CREATED_AT": str(dt.now())[0:23],
                    "TARGET_NAME": target_name
                }]
                
                df = pd.DataFrame(data)
                if target == 'mysql':
                    rtn = pd.read_sql(f"select 1 from MYSQL_TARGET where TARGET_NAME='{target_name}'", con=conn)
                    if len(rtn.index)>0:
                        return jsonify({"response":"Failed","err":"Target Name Already Exists",})
                    df.to_sql("MYSQL_TARGET", con=conn, index=False, if_exists='append')
                    return jsonify({"response":"Success","data":"Connection Successfully Saved",})
                elif target == 'sqlserver':
                    rtn = pd.read_sql(f"select 1 from SQLSERVER_TARGET where TARGET_NAME='{target_name}'", con=conn)
                    if len(rtn.index)>0:
                        return jsonify({"response":"Failed","err":"Target Name Already Exists",})
                    df.to_sql("SQLSERVER_TARGET", con=conn, index=False, if_exists='append')
                    return jsonify({"response":"Success","data":"Connection Successfully Saved",})
                    

                return jsonify({"response":"Failed","data":"Connection Not Established",})
            return jsonify({"response":"Failed","message":"body part missing"})
    return jsonify({"response":"Failed","message":"Method not allow"})