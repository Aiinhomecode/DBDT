from flask import request, jsonify
from ftplib import FTP
from db_connection import conn
import pandas as pd
from datetime import datetime as dt
from pymongo import MongoClient

def test_save_mongo_db_source():
    if request.method == 'POST':
        if request.json["host"] and  request.json["port"] and request.json["user"] and  request.json["password"] \
             and  request.json["database"] and  request.json["source_name"] and request.json["user_id"]:
            try:
                host=request.json["host"]
                port= int(request.json["port"])
                user= request.json["user"]
                password= request.json["password"]
                database= request.json["database"]
                source_name = request.json["source_name"]
                user_id = request.json["user_id"]

                rtn = pd.read_sql(F"SELECT 1 FROM [MONGO_DB_SOURCE] WHERE SOURCE_NAME='{source_name}'", con=conn)
                if len(rtn.index)>0:
                    return jsonify({"status":"Failed", "err":"Source already exists"})

                # client = MongoClient(f'{host}',port, username=''+user+'', password=''+password+'')
                # mydb = client[database]
                # mydatabase = mydb.list_collection_names()
                # if mydatabase is not None:
                #     return jsonify({"status":"ok","msg":"Connection established successfully"})
                # else:    
                #     return jsonify({"status":"Failed","msg":"Connection not established successfully"})

                data = [{
                    "USER_ID":user_id,
                    "SOURCE_NAME":source_name,
                    "HOST":host,
                    "PORT":port,
                    "USER":user,
                    "PASSWORD":password,
                    "DATABASE":database,
                    "CREATED_AT": str(dt.now())[0:23]
                }]
                df = pd.DataFrame(data)
                df.to_sql("MONGO_DB_SOURCE", con= conn, if_exists='append', index=False)

                return jsonify({"status":"ok","msg":"Connection established successfully and saved"})
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})