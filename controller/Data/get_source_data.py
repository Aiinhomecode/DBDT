from operator import index
from unittest import result
from flask import request, jsonify
from ftplib import FTP
import pandas as pd
from db_connection import conn
import io
import cx_Oracle
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os.path
import os
import pyodbc
from pymongo import MongoClient
import json as js
from .make_db_connection import mysql_con, sqlserver_con
import json
from dotenv import load_dotenv

load_dotenv()

def get_source_data():
    try:
        if request.method == "POST":
                json = request.json
                ID = json["source_id"]#FTP_1
                File_Name = json["file_name"]
                Source_Name,Source_ID = ID.split("_")
                
                if Source_Name.lower() == 'ftp':
                    df = pd.read_sql_query('''SELECT * FROM FTP_SOURCE WHERE ID= ?''', conn, params=([int(Source_ID)]))
                    HOST = df['HOST'].item()
                    PORT = df['PORT'].item()
                    USER = df['USER'].item()
                    PASSWORD = df['PASSWORD'].item()
                    PATH = df['PATH'].item()
                    file_format = df['FILE_FORMAT'].item()
                    ftp = FTP(timeout = 60)
                    ftp.connect(HOST, PORT)
                    ftp.login(USER, PASSWORD)
                    ftp.cwd(PATH)

                    download_file = io.BytesIO()
                    ftp.retrbinary("RETR {}".format(File_Name), download_file.write)
                    download_file.seek(0) 
                    if file_format.lower() == 'csv':
                        df = pd.read_csv(download_file, nrows=10) 
                        columns = list(df.columns.astype(str))
                        # df = df.iloc[:10].copy()
                        df.fillna('',inplace=True)
                        df = df.astype(str).copy()
                        dict_data = df.to_dict(orient='records')
                        out_resp = {
                            "status":"Success",
                            "data":dict_data,
                            "columns": columns
                        }
                        # print(out_resp)
                    elif file_format.lower() == 'xlsx':
                        df = pd.read_excel(download_file, nrows=10)
                        json_data = df.to_json(orient='records', date_format='iso')
                        out_resp = {
                            "status":"Success",
                            "data":js.loads(json_data),
                            "columns": list(df.columns.astype(str))
                        }
                        # print(out_resp)
                    
                    return jsonify(**out_resp)

                elif Source_Name.lower() == 'sqlserver':
                    return get_sqlserver_table_data(Source_ID, File_Name)

                elif Source_Name.lower() == 'local':
                    return get_LocalSource_FileData(Source_ID, File_Name)

                elif Source_Name.lower() == 'googledrive':
                    df = pd.read_sql_query('''SELECT * FROM GOOGLE_DRIVE_SOURCE WHERE ID= ?''', conn, params=(Source_ID))
                    out_resp = {
                        "status":"Success",
                        "data":[],
                    }
                    return jsonify(**out_resp)


                elif Source_Name.lower() == 'oracle':
                    df = pd.read_sql_query('''SELECT * FROM [ORACLE_DB_SOURCE] WHERE ID= ?''', conn, params=(Source_ID))
                    HOST = df['HOST'].item()
                    PORT = df['PORT'].item()
                    USER = df['USER'].item()
                    PASSWORD = df['PASSWORD'].item()
                    SERVICE = df['SERVICE'].item()
                    TABLE_NAME = df['TABLE_NAME'].item()
                    CONN_STR = f'{USER}/{PASSWORD}@{HOST}:{PORT}/{SERVICE}'#.format(**CONN_INFO)
                    orcl_conn = cx_Oracle.connect(CONN_STR)
                    cur = orcl_conn.cursor()
                    for row in cur.execute(f"SELECT * FROM '{TABLE_NAME}']"):
                        data_dict = list(row)
                    out_resp = {
                        "status":"Success",
                        "data":data_dict,
                    }
                    return jsonify(**out_resp) 

                elif Source_Name.lower() == 'azureblob':
                    df = pd.read_sql_query('''SELECT * FROM [AZURE_BLOB_SOURCE] WHERE ID= ?''', conn, params=(Source_ID))
                    connection_string = df['CONNECTION_STRING'].item()
                    contfilename= File_Name
                    CONTAINER_NAME=contfilename.split('/')[0]
                    BLOB_NAME=contfilename.split('/')[1]
                    blob_svc = BlobServiceClient.from_connection_string(conn_str=connection_string)
                    blobstring = blob_svc.get_blob_client(CONTAINER_NAME,BLOB_NAME,snapshot=None)
                    blob_data = blobstring.download_blob()
                    data = blob_data.readall()
                    str1 = data.decode('UTF-8')
                    rown=[]
                    for line in str1.splitlines():
                       rown.append(line.split(','))
                    h=rown.pop(0)
                    df = pd.DataFrame(rown,columns=h)
                    column=[]
                    for col in df.columns:
                        column.append(col)
                    dict_data = df.to_dict(orient='records')
                    out_resp={
                        "status":"Success",
                        "data":dict_data,
                        "columns":column
                    }
                    return jsonify(**out_resp)  

                elif Source_Name.lower() == 'mongodb':
                    df = pd.read_sql_query('''SELECT * FROM [MONGO_DB_SOURCE] WHERE ID= ?''', conn, params=(Source_ID))
                    host = df["HOST"][0]
                    port = df["PORT"][0]
                    user = df["USER"][0]
                    password = df["PASSWORD"][0]
                    database = df["DATABASE"][0]

                    client = MongoClient(f'{host}',port, username=''+user+'', password=''+password+'')
                    mydb = client[database]
                    collection_name = mydb[File_Name]
                    docs = []
                    for doc in collection_name.find():
                            doc.pop('_id')
                            docs.append(doc)
                    dict_data = docs
                    df = pd.DataFrame(dict_data)   
                    columns = list(df.columns)    
                    dict_data = df.to_dict(orient='records')
                    out_resp={
                        "status":"Success",
                        "data":dict_data,
                        "columns":columns
                    }
                    return jsonify(**out_resp)             
        return jsonify({'status': "Failed", 'message': 'Method not allowed'})
    except Exception as e:
            response={'status': "Failed", 'message': 'Connection Not Established successfully!', "err": str(e)}
            return jsonify(response)

def get_sqlserver_table_data(source_id, file_name):
    connValuesDF = pd.read_sql_query(f'select * from SQLSERVER_SOURCE where ID={source_id}', con=conn)
    host = connValuesDF['HOST'][0]
    port = connValuesDF['PORT'][0]
    user = connValuesDF['USER'][0]
    password = connValuesDF['PASSWORD'][0]
    db = connValuesDF['DATABASE'][0]
    sqlserver_conn = sqlserver_con(host, port, user, password, db)
    if sqlserver_conn['status']:
        new_con = sqlserver_conn['connection']
        dataChunk = pd.read_sql_table(file_name, con=new_con, chunksize=10)
        for chunk in dataChunk:
            dataDF = pd.DataFrame(chunk)
            break
        jsonData = dataDF.to_json(orient='records', date_format='iso')
        response = {
            "status":"Success",
            "data": json.loads(jsonData),
            "columns": list((dataDF.columns).astype(str))
        }
    else:
        response = {
            "status":"Failed",
            "message": "Connection Error",
        }
    return jsonify(**response)

def get_LocalSource_FileData(source_id, file_name):
    localSource_FilePath = os.path.join(os.environ.get('UPLOAD_LOCATION'), 'LOCALSOURCE_'+str(source_id), file_name)
    if file_name.endswith(('csv','CSV')):
        dataDF = pd.read_csv(localSource_FilePath, nrows=10)
    elif file_name.endswith(('xls','XLS','xlsx', 'XLSX')):
        dataDF = pd.read_excel(localSource_FilePath, nrows=10)
    jsonData = dataDF.to_json(orient='records', date_format='iso')
    response = {
        "status":"Success",
        "data": json.loads(jsonData),
        "columns": list((dataDF.columns).astype(str))
    }
    return jsonify(**response)