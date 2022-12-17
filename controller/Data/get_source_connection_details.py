from flask import request, jsonify
from numpy import source
import pandas as pd
from db_connection import conn

def get_source_connection_details():
    if request.method == 'POST':
        try:
            if request.json['source_id']:
                source_id = request.json['source_id']
                source = source_id.split('_')[0]
                id = source_id.split('_')[-1]

                if source.lower().strip() == 'ftp':
                    df = pd.read_sql(f"SELECT CONCAT('FTP_',[ID]) AS [SOURCE_ID], [SOURCE_NAME], [HOST], [PORT], [USER], [PATH], [FILE_FORMAT] FROM [dbo].[FTP_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }
                elif source.lower().strip() == 'sqlserver':
                    df = pd.read_sql(f"SELECT CONCAT('FTP_',[ID]) AS [SOURCE_ID], [SOURCE_NAME], [HOST], [PORT], [USER], [DATABASE] FROM [dbo].[SQLSERVER_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }
                elif source.lower().strip() == 'local':
                    df = pd.read_sql(f"SELECT CONCAT('FTP_',[ID]) AS [SOURCE_ID], [SOURCE_NAME] FROM [dbo].[LOCAL_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }
                elif source.lower().strip() == 'googledrive':
                    df = pd.read_sql(f"SELECT CONCAT('GOOGLEDRIVE_',[ID]) AS [SOURCE_ID], [SOURCE_NAME], [DRIVE_TOKEN], [FOLDER_ID] FROM [dbo].[GOOGLE_DRIVE_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }
                elif source.lower().strip() == 'azureblob':
                    df = pd.read_sql(f"SELECT CONCAT('AZUREBLOB_',[ID]) AS [SOURCE_ID], [SOURCE_NAME], [CONTAINER_NAME], [BLOB_NAME], [ACCOUNT_URL], [CONNECTION_STRING] FROM [dbo].[AZURE_BLOB_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }
                elif source.lower().strip() == 'mongodb':
                    df = pd.read_sql(f"SELECT CONCAT('MONGODB_',[ID]) AS [SOURCE_ID], [SOURCE_NAME], [HOST], [PORT], [USER],[DATABASE] FROM [dbo].[MONGO_DB_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }            
                return jsonify(**out_data)
        except Exception as e:
            return jsonify({"status":"Failed","err":"Something went wrong", "msg": str(e)})
    return jsonify({"status":"Failed","err":"Method not allowed"})