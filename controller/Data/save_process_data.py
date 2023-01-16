from unittest import result
from flask import request, jsonify
from db_connection import conn
import pandas as pd
import json
from pyodbc import Cursor

def insert_process_data():
    try:
        user_id = request.json['user_id']
        process_name = request.json['process_name']
        sourse_connection = request.json['sourse_connection']
        sourse_table = request.json['sourse_table']
        targate_name = request.json['targate_name']
        process_json = request.json['process_json']
        process_json = process_json.replace("'","''").replace('\\','')
        # print(user_id)
        # print(process_name)
        # print(sourse_connection )
        # print(sourse_table)
        # print(targate_name)
        print(process_json)
        


        query = f"""EXEC [dbo].[PROCESS_MASTER_DATA] {user_id},'{process_name}','{sourse_connection}','{sourse_table}','{targate_name}','{process_json}'"""
        # print(query)
        connection = conn.raw_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        # print(cursor.fetchall())
        result=cursor.fetchall()
        # print(result)
        connection.commit()
    
        # result_json2 = result_json.to_json(orient='records', date_format='iso')
        # print(cursor.fetchall())

        result = {
            'status':'Success',
            'data':result[0][0]
        }
        
    except Exception as e:
        result = {
            'status':'Failed',
            'data':'',
            'error_msg': str(e)
        }
    finally:
        return jsonify(**result) 