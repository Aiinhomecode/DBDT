from flask import request, jsonify
import json
import pandas as pd
from db_connection import conn

def get_process_execution_report():
    try:
        if request.method == "POST":
            process_master_id = request.json['process_master_id']

            result = pd.read_sql_query(F"EXEC GET_PROCESS_EXECUTION_REPORT {process_master_id}", con = conn)
            json_result = result.to_json(orient = 'records', date_format= 'iso')
            response = {
                "status": "Success",
                "Data": json.loads(json_result)
            }
        else:
            raise("Method not allowed.")
    except Exception as e:
        response = {
            'Status':'Failed',
            'msg': str(e)
        }
    finally:
        return jsonify(**response)