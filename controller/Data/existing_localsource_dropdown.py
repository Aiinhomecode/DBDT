from flask import request, jsonify
import pandas as pd
from db_connection import conn

def existing_localsource_dropdown():
    try:
        user_id = request.json['user_id']
        responseDF = pd.read_sql_query(f"SELECT [ID], [SOURCE_NAME] FROM LOCAL_SOURCE WHERE [ACTIVE_STATUS]='Y' AND [USER_ID]={user_id}", con = conn)
        response = {
            "status":"Success",
            "data": responseDF.to_dict(orient='records')
        }
    except Exception as e:
        response = {
            "status":"Failed",
            "msg": str(e),
        }
    finally:
        return jsonify(**response)