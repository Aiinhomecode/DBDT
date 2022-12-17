from json import load
import pandas as pd
from flask import request,jsonify
from pyodbc import Cursor
from db_connection import conn
from ftplib import FTP
import io
from .make_db_connection import mysql_con, sqlserver_con
# from sqlalchemy.types import NVARCHAR, VARCHAR, INT, INTEGER, DATE, DATETIME, TEXT, DECIMAL
from urllib.parse import unquote
from .data_type_parse import cor_datatype
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from db_connection import conn
import os
from dotenv import load_dotenv

load_dotenv()

def push_source_data_to_target():
    if request.method == 'POST':
        # return 'ok'
        try:
            if "source_id" in request.json and "file_name" in request.json and "target_id" in request.json and "table_name" in request.json and "user_id" in request.json:
                print('start')
                user_id=request.json["user_id"]
                source_id=request.json["source_id"]
                file_name=request.json["file_name"]
                target_id=request.json["target_id"]
                table_name=request.json["table_name"]
                schema = request.json['schema']
                use_existing = request.json['use_existing']
                first_row_header = request.json['first_row_header']
                Selected_Columns = request.json['selected_columns']
                id = source_id.split("_")[-1]
                source= source_id.split("_")[0]
                tgt_id = target_id.split("_")[-1]
                target = target_id.split("_")[0]
                # print(source)

                if source.lower() == 'ftp':
                    # print('reached')
                    query1="select * from FTP_SOURCE where id='"+id+"'"
                    df=pd.read_sql(query1,con=conn)
                    # print(df)

                    src_host = df['HOST'].item()
                    src_port = df['PORT'].item()
                    src_user = df['USER'].item()
                    src_password = df['PASSWORD'].item()
                    src_path = df['PATH'].item()
                    src_file_format = df['FILE_FORMAT'].item()
                    src_name = df['SOURCE_NAME'].item()

                    ftp = FTP(timeout = 60)
                    ftp.connect(src_host,src_port)
                    ftp.login(src_user,src_password)
                    ftp.cwd(src_path)

                    download_file = io.BytesIO()
                    ftp.retrbinary("RETR {}".format(file_name), download_file.write)
                    download_file.seek(0) 
                    if src_file_format.lower() == 'csv':
                        if use_existing == 'Yes' and first_row_header == 'No':
                            data_df = pd.read_csv(download_file, header=None)
                        else:
                            data_df = pd.read_csv(download_file)
                    if src_file_format.lower() in ['xls','xlsx']:
                        if use_existing == 'Yes' and first_row_header == 'No':
                            data_df = pd.read_excel(download_file, header=None)
                        else:
                            data_df = pd.read_excel(download_file)

                elif source.lower() == 'sqlserver':
                    data_df, src_name = getSqlserverSourceData(id, file_name)

                elif source.lower() == 'local':
                    data_df, src_name = getLocalSourceFileData(id, file_name, use_existing, first_row_header)

                elif source.lower() == 'googledrive':
                    data_df = driveFileData(file_name,use_existing, first_row_header)
                

                elif source.lower() == 'azureblob':
                    # print('reached')
                    query1="select * from [AZURE_BLOB_SOURCE] where id='"+id+"'"
                    df=pd.read_sql(query1,con=conn)
                    # print(df)
                    connection_string = df['CONNECTION_STRING'].item()
                    contfilename= file_name
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
                    if use_existing == 'Yes' and first_row_header == 'No':    
                        data_df = df
                    else:
                      data_df = df

                elif source.lower() == 'mongodb':
                    query1="select * from [MONGO_DB_SOURCE] where id='"+id+"'"
                    df=pd.read_sql(query1,con=conn)
                    host = df["HOST"][0]
                    port = df["PORT"][0]
                    user = df["USER"][0]
                    password = df["PASSWORD"][0]
                    database = df["DATABASE"][0]

                    client = MongoClient(f'{host}',port, username=''+user+'', password=''+password+'')
                    mydb = client[database]
                    collection_name = mydb[file_name]
                    docs = []
                    for doc in collection_name.find():
                            doc.pop('_id')
                            docs.append(doc)
                    dict_data = docs
                    df = pd.DataFrame(dict_data)   
                    columns = list(df.columns)    
                    dict_data = df.to_dict(orient='records')
                    if use_existing == 'Yes' and first_row_header == 'No':    
                        data_df = dict_data#.to_dict(orient='records')
                    else:
                      data_df = dict_data


                # Prepare the Datatype and rename the column
                rename_columns = {}
                data_type = {}
                for columns in schema:
                    rename_columns[columns['column_name']]=columns['rename_to']
                    size = columns['size']
                    if size != '' and size is not None:
                        # print('size')
                        data_type[columns['rename_to']]=cor_datatype(columns['data_type'], *size.split(','))
                    else:
                        data_type[columns['rename_to']]=cor_datatype(columns['data_type'])
                data_df.rename(columns=rename_columns, inplace=True)

                # Selected Columns Only
                data_df = data_df.reindex(labels=Selected_Columns, axis=1)

                if target.lower() =='mysql':
                    query1="select * from MYSQL_TARGET where id='"+tgt_id+"'"
                    df=pd.read_sql(query1,con=conn)
                    # print(df)

                    tgt_host = df['HOST'].item()
                    tgt_port = df['PORT'].item()
                    tgt_user = df['USER'].item()
                    tgt_password = df['PASSWORD'].item()
                    tgt_db = df['DB'].item()
                    rtn_conn = mysql_con(host=tgt_host, port=tgt_port,password=tgt_password, user=tgt_user, db=tgt_db)
                    if rtn_conn["status"]:
                        conn_mysql = rtn_conn["connection"]
                        # data_df.to_sql(table_name, con=conn_mysql, index=False, if_exists='append')
                        return jsonify({"status":"Success","msg":"Data successfully pushed to target"})
                    return jsonify({"status":"Failed","err":"Connection Failed"})

                elif target.lower() =='sqlserver':
                    query1="select * from SQLSERVER_TARGET where id='"+tgt_id+"'"
                    df=pd.read_sql(query1,con=conn)
                    # print(df)

                    tgt_host = df['HOST'].item()
                    tgt_port = df['PORT'].item()
                    tgt_user = df['USER'].item()
                    tgt_password = df['PASSWORD'].item()
                    tgt_db = df['DATABASE'].item()
                    tgt_name = df['TARGET_NAME'].item()
                    # print('ok')
                    rtn_conn = sqlserver_con(host=tgt_host, port=tgt_port,password=tgt_password, user=tgt_user, db=tgt_db)
                    if rtn_conn["status"]:
                        # rows_effected = 0
                        conn_sqlserver = rtn_conn["connection"]
                        # print(data_df)
                        if use_existing == 'Yes':
                            col_df = pd.read_sql(f"select * from {table_name} where 1<>1", con= conn_sqlserver)
                            exist_col = col_df.columns
                            data_df.columns = exist_col
                            data_df.to_sql(table_name, con=conn_sqlserver, index=False, if_exists='append', dtype= data_type)
                        else:
                            data_df.to_sql(table_name, con=conn_sqlserver, index=False, if_exists='fail', dtype= data_type)
                        rows_effected = len(data_df)
                        insert_log(user_id= user_id, source_name= src_name, file_name = file_name, destination_name= tgt_name, table_name= table_name, rows_effected= rows_effected)
                        return jsonify({"status":"Success","msg":"Data successfully pushed to target",'records_count':rows_effected})
                    return jsonify({"status":"Failed","err":"Connection Failed"})
        except Exception as e:
            return jsonify({"status":"Failed","err":str(e)})
    return jsonify({"status":"Failed1","err":"Method not allowed"})
                       
def driveFileData(id, existing, headers):
    dwn_url=f'https://drive.google.com/uc?id={id}'
    # print(dwn_url)
    if existing == 'Yes' and headers == 'No':
        df = pd.read_csv(dwn_url, header=None)
    else:
        df = pd.read_csv(dwn_url)
    return df

def insert_log(**params):
    try:
        query = f"""EXEC INSERT_LOG_DATA {params['user_id']},'{params['source_name']}','{params['file_name']}','{params['destination_name']}','{params['table_name']}',{params['rows_effected']}"""
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
    finally:
        connection.close()
   
def getSqlserverSourceData(source_id, tableName):
    connValuesDF = pd.read_sql_query(f'select * from SQLSERVER_SOURCE where ID={source_id}', con=conn)
    host = connValuesDF['HOST'][0]
    port = connValuesDF['PORT'][0]
    user = connValuesDF['USER'][0]
    password = connValuesDF['PASSWORD'][0]
    db = connValuesDF['DATABASE'][0]
    src_name = connValuesDF['SOURCE_NAME'][0]
    sqlserver_conn = sqlserver_con(host, port, user, password, db)
    if sqlserver_conn['status']:
        new_con = sqlserver_conn['connection']
        dataDF = pd.read_sql_table(tableName, con= new_con)
        return dataDF, src_name

def getLocalSourceFileData(source_id, fileName, use_existing, first_row_header):
    source_Name = pd.read_sql_query(f"SELECT [SOURCE_NAME] FROM [LOCAL_SOURCE] WHERE [ID]={source_id}", con= conn)['SOURCE_NAME'][0]
    localSource_FilePath = os.path.join(os.environ.get('UPLOAD_LOCATION'), 'LOCALSOURCE_'+str(source_id), fileName)
    header = None if (use_existing == 'Yes' and first_row_header == 'No') else 0

    if fileName.endswith(('csv','CSV')):
        dataDF = pd.read_csv(localSource_FilePath, header=header)
    elif fileName.endswith(('xls','XLS','xlsx', 'XLSX')):
        dataDF = pd.read_excel(localSource_FilePath, header=header)
    return dataDF, source_Name