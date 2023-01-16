from urllib import response
from flask import request, jsonify
import pandas as pd
from pyodbc import Cursor
from db_connection import conn

def run_process():
    try:
        if request.method == 'POST':
            process_master_id = request.json['process_master_id']
            connection = conn.raw_connection()
            cursor = connection.cursor()
            cursor.execute(F"EXEC [RUN_PROCESSOR] {process_master_id}")
            result = cursor.fetchall()
            print(result)
            if result[0][0] == 'Success':
                response = {"Status":"Success"}
            else:
                response = {"Status":"Failed","msg":result[0][1]}
            
        else:
            raise Exception("Method not allowed.")
    except Exception as e:
        response = {"Status":"Failed", "msg":str(e)}
    finally:
        cursor.close()
        connection.commit()
        connection.close()
        return jsonify(response)