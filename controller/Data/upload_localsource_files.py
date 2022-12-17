from ast import If
from flask import request, jsonify
from dotenv import load_dotenv
from os import environ, path, mkdir
from sqlalchemy import false
from werkzeug.utils import secure_filename
from datetime import datetime as dt
import pandas as pd
from db_connection import conn

load_dotenv()
upload_Location = environ.get('UPLOAD_LOCATION')

def upload_localsource_files():
    try:
        user_id = request.form["user_id"]
        source_name = request.form['source_name']
        use_existing = request.form['use_existing']
        files = request.files.getlist('files')
        upload_directory = environ.get('UPLOAD_LOCATION')
        
        if use_existing == 'No':
            source_ID = create_Source(user_id, source_name)
            base_Folder_Path = path.join(upload_directory, 'LOCALSOURCE_'+str(source_ID))
            mkdir(base_Folder_Path)
        else:
            base_Folder_Path = path.join(upload_directory, 'LOCALSOURCE_'+str(source_name))

        for file in files:
            file.save(path.join(base_Folder_Path, secure_filename(file.filename)))

        response = {
            "status":"Success",
            "msg":"Files Successfully Uploaded"
        }
    except Exception as e:
        response = {
            "status":"Failed",
            "msg": str(e)
        }
    finally:
        return jsonify(**response)

def create_Source(user_id, source_name):
    source_Exists = pd.read_sql_query(f"SELECT 1 FROM LOCAL_SOURCE WHERE [SOURCE_NAME]='{source_name}' AND USER_ID = {user_id}", con=conn)
    if len(source_Exists.index) > 0:
        raise Exception("Source Name Already Exists.")
    source = [{
                "USER_ID":user_id,
                "SOURCE_NAME":source_name,
                "CREATED_AT": str(dt.now())[0:23]
                }]
    sourceDF = pd.DataFrame(source)
    sourceDF.to_sql('LOCAL_SOURCE', con=conn, if_exists='append', index=False)
    sourceID = pd.read_sql_query(f"SELECT [ID] FROM LOCAL_SOURCE WHERE [SOURCE_NAME]='{source_name}' AND USER_ID = {user_id}", con=conn)['ID'][0]
    return sourceID