from flask import request, jsonify
from numpy import dtype
from db_connection import conn
import pandas as pd
from ftplib import FTP
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from .make_db_connection import mysql_con, sqlserver_con
from dotenv import load_dotenv
import os

load_dotenv()

def get_source_filenames():
    try:
        if request.method == 'POST':
            if 'source_id' in request.json :
                SOURCE_ID = request.json['source_id']
                # SOURCE_NAME = request.json['source_name']
                source_type = SOURCE_ID.split('_')[0]
                id = SOURCE_ID.split('_')[-1]
                print(id)

                if source_type.lower() == 'ftp':
                    df = pd.read_sql(f"select * from [FTP_SOURCE] where [ID]={id}", con= conn)
                    
                    host = df["HOST"][0]
                    port = df["PORT"][0]
                    user = df["USER"][0]
                    password = df["PASSWORD"][0]
                    path = df["PATH"][0]
                    file_format = df["FILE_FORMAT"][0]
                    # print(host, type(host))
                    
                    ftp = FTP()
                    ftp.connect(host=host, port=int(port))
                    ftp.login(user=user,passwd=password)
                    ftp.cwd(path)

                    files_list = ftp.nlst()
                    result_list = []
                    for file in files_list:
                        if file.endswith(file_format.lower()) or file.endswith(file_format.upper()) or file.endswith(file_format.lower()[:3]) or file.endswith(file_format.upper()[:3]):
                            result_list.append(file)
                    result = {
                        "status":"success",
                        "data":result_list,
                    }
                elif source_type.lower() == 'sqlserver':
                    df = pd.read_sql(f"select * from SQLSERVER_SOURCE where [ID]={id}", con= conn)
                    host = df['HOST'][0]
                    port = df['PORT'][0]
                    user = df['USER'][0]
                    password = df['PASSWORD'][0]
                    db = df['DATABASE'][0]
                    out_data = {}
                    sqlserver_conn = sqlserver_con(host, port, user, password, db)
                    if sqlserver_conn['status']:
                        new_con = sqlserver_conn['connection']
                        sql_query =   f"select TABLE_NAME from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
                        df_tables = pd.read_sql(sql_query, con = new_con)

                        result = {
                            "status":"success",
                            "data": list(df_tables['TABLE_NAME'])
                        } 

                elif source_type.lower() == "local":
                    localsource_Path = os.path.join(os.environ.get('UPLOAD_LOCATION'), 'LOCALSOURCE_'+str(id))
                    result = {
                            "status":"success",
                            "data": os.listdir(localsource_Path)
                        }
                    
                elif source_type.lower() == 'googledrive':
                    return jsonify({"status":"Success","data":[]})

                elif source_type.lower() == 'azureblob': 
                    df = pd.read_sql(f"select * from [AZURE_BLOB_SOURCE] where [ID]={id}", con= conn)
                    
                    connection_string = df["CONNECTION_STRING"][0]
                    # print (connection_string)
                    blob_svc = BlobServiceClient.from_connection_string(conn_str=connection_string)
                    containers = blob_svc.list_containers()
                    list_of_blobs = []
                    for c in containers:
                        container_client = blob_svc.get_container_client(c)
                        blob_list = container_client.list_blobs()
                        for blob in blob_list:
                            list_of_blobs.append(c.name+'/'+blob.name)
                        result={
                            "status":"success",
                            "data":list_of_blobs
                            }  

                elif source_type.lower() == 'mongodb': 
                    df = pd.read_sql(f"select * from [MONGO_DB_SOURCE] where [ID]={id}", con= conn)
                    
                    host = df["HOST"][0]
                    port = df["PORT"][0]
                    user = df["USER"][0]
                    password = df["PASSWORD"][0]
                    database = df["DATABASE"][0]

                    client = MongoClient(f'{host}',port, username=''+user+'', password=''+password+'')
                    mydb = client[database]
                    mydatabase = mydb.list_collection_names()
                    list_of_collections= []
                    for collection in mydatabase:
                        list_of_collections.append(collection)
                    result={
                        "response":"sucessfull",
                        "data":list_of_collections
                        }                
            else:    
                result = {"status":"Failed","err":"Body part missing"}
        else:
            result = {"status":"Failed","err":"Method not allowed"}
    except Exception as e:
        result = {"status":"Failed","err": str(e)}
    finally:
        return jsonify(**result)