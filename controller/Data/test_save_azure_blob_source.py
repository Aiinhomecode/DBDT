from flask import request, jsonify
from ftplib import FTP
from db_connection import conn
import pandas as pd
from datetime import datetime as dt
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os.path
import pyodbc

def test_save_azure_blob_source():
    if request.method == 'POST':
        if  request.json["connection_string"] and  request.json["source_name"] and request.json["user_id"]:
            try:
                CONNECTION_STRING = request.json["connection_string"]
                source_name = request.json["source_name"]
                user_id = request.json["user_id"]



                rtn = pd.read_sql(F"SELECT 1 FROM [AZURE_BLOB_SOURCE] WHERE [SOURCE_NAME]='{source_name}'", con=conn)
                if len(rtn.index)>0:
                    return jsonify({"status":"Failed", "err":"Source already exists"})
                # blob_service = 
                # BlobServiceClient(account_name=STORAGEACCOUNTNAME, account_key=STORAGEACCOUNTKEY,account_url=ACCOUNT_URL)
                
                data = [{
                    "USER_ID":user_id,
                    "SOURCE_NAME":source_name,
                    "CONNECTION_STRING":CONNECTION_STRING,              
                    "CREATED_AT": str(dt.now())[0:23]
                }]
                df = pd.DataFrame(data)
                
                df.to_sql("AZURE_BLOB_SOURCE", con= conn, if_exists='append', index=False)

                return jsonify({"status":"ok","msg":"Connection established successfully and saved"})
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})