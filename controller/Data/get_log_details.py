from unittest import result
from flask import request, jsonify
from db_connection import conn
import pandas as pd
import json

def get_log_details():
    try:
        result_df = pd.read_sql_query(F'EXEC [dbo].[GET_AUDIT_REPORT]', con = conn)
        result_json = result_df.to_json(orient='records', date_format='iso')
        load_data_df = pd.read_sql_query(F'EXEC [GET_LOG_DETAILS]',con = conn)
        load_data = load_data_df.to_json(orient='records',date_format='iso')
        result = {
            'status':'Success',
            'data':json.loads(result_json),
            'load_data':load_data
        }
        
    except Exception as e:
        result = {
            'status':'Failed',
            'data':'',
            'error_msg': str(e)
        }
    finally:
        return jsonify(**result) 