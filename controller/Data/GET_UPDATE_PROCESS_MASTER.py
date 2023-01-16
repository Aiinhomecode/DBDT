import sqlalchemy
import pandas as pd
import urllib
import pyodbc
from pyodbc import Cursor

from flask import request, jsonify
from numpy import source
from flask import Flask,jsonify,make_response
from flask_cors import CORS
from db_connection import conn
import json

def GET_UPDATE_PROCESS_MASTER():
    if request.method == 'GET' or request.method == 'POST':
        try:
            PROCESS_MASTER_ID = request.json['process_master_id']
            USER_ID = request.json['USER_ID']
            PROCESS_NAME =request.json['PROCESS_NAME']
            SOURSE_CONNECTION=request.json['SOURSE_CONNECTION']
            SOURSE_TABLE=request.json['SOURSE_TABLE']
            TERGATE_NAME=request.json['TERGATE_NAME']
            PROCESS_STATUS=request.json['PROCESS_STATUS']
            TRANSATION_JSON=request.json['query_json']
            TRANSATION_JSON = TRANSATION_JSON.replace("'","''").replace('\\','')
            #process_json = process_json.replace("'","''").replace('\\','')

            #print(PROC
            query=F"""EXEC [dbo].[UPDATE_PROCESS_MASTER] {PROCESS_MASTER_ID},'{USER_ID}','{PROCESS_NAME}','{SOURSE_CONNECTION}','{SOURSE_TABLE}','{TERGATE_NAME}','{PROCESS_STATUS}','{TRANSATION_JSON}'"""
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


        
