from unittest import result
from flask import request, jsonify
from db_connection import conn
import pandas as pd
import json

def get_log_details():
    try:
        user_id = request.json['user_id']
        result_df = pd.read_sql_query(F'EXEC [GET_LOG_DETAILS] {user_id}', con = conn)
        result_json = result_df.to_json(orient='records', date_format='iso')
        result = {
            'status':'Success',
            'data':json.loads(result_json)
        }
        
    except Exception as e:
        result = {
            'status':'Failed',
            'data':'',
            'error_msg': str(e)
        }
    finally:
        return jsonify(**result) 