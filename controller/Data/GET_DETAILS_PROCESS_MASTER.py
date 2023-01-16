import sqlalchemy
import pandas as pd
import urllib

from flask import request, jsonify
from numpy import source
from flask import Flask,jsonify,make_response
from flask_cors import CORS
from db_connection import conn
import json

def GET_DETAILS_PROCESS_MASTER():
    if request.method == 'GET' or request.method == 'POST':
        try:
            PROCESS_MASTER_ID = request.json['process_master_id']
            #print(PROCESS_MASTER_ID)
            result_df = pd.read_sql_query(F'EXEC [dbo].[DETAILS_PRO_MASTER]  {PROCESS_MASTER_ID}', conn)
            result_json = result_df.to_json(orient='records', date_format='iso')
            #print(result_df)
            result = {'status':'Success','data':json.loads(result_json)}      
        except Exception as e:
            result={'response': "Failed", 'data': 'Data Not fetched successfully!', "err": str(e)}
        finally:
            return jsonify(**result)
    else:
        result={'response': "Failed", 'data': 'Bad request'}
        return jsonify(**result)
        



