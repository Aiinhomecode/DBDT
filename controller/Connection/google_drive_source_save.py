from flask import request,jsonify
from datetime import datetime as dt
from db_connection import conn
import pandas as pd

def google_drive_source_save():
    if request.method == 'POST':
        if 'user_id' and 'id' and 'token' and "source_name" in request.json:
            user_id = request.json["user_id"]
            id = request.json['id']
            token = request.json['token']
            source_name = request.json['source_name']

            data = [{
                "USER_ID": user_id,
                "SOURCE_NAME": source_name,
                "DRIVE_TOKEN": token,
                "FOLDER_ID": id,
                "CREATED_AT": str(dt.now())[0:23]
            }]
            df = pd.DataFrame(data)
            df.to_sql('GOOGLE_DRIVE_SOURCE', con=conn, if_exists='append', index=False)
            return jsonify({"status":"success","msg":"successfully saved"})
        return jsonify({"status":"failed","err":"Bodypart missing"})
    return jsonify({"status":"failed","err":"Method not allowed"})