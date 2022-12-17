from flask import request, jsonify
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os.path
import pyodbc
import pandas as pd

def azure_blob_test_connection():
    if request.method == 'POST':
        if  request.json["connection_string"]:
            try:
                connection_string=request.json['connection_string']
                blob_svc = BlobServiceClient.from_connection_string(conn_str=connection_string)
                containers = blob_svc.list_containers()
                list_of_blobs = []
                for c in containers:
                    container_client = blob_svc.get_container_client(c)
                    blob_list = container_client.list_blobs()
                    # for blob in blob_list:
                    #     resp = list_of_blobs.append(c.name+'/'+blob.name)
                if blob_list is not None:
                    return jsonify({"status":"ok","msg":"Connection established successfully"})
                else:    
                    return jsonify({"status":"Failed","msg":"Connection not established successfully"})   
   
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})