from flask import request, jsonify
from json import loads
from flask import jsonify
from sqlalchemy import null
from db_connection import conn
import pandas as pd
import json

def GET_SERVER_NAME():

    if request.method == 'GET':
        try:
            resultOfServerName = pd.read_sql_query(F'[dbo].[GET_SERVER_NAME]', con=conn)
            resultJson= resultOfServerName.to_json(orient='records',date_format='iso')
            result ={'status':'success','data':json.loads(resultJson)}
        except Exception as e:
             result = {'status':'Failed','data':'Something is error ! Please try again ',"err":str(e)}
        finally:
            return jsonify(result)
    else:
        result={'status':'Failed','data':'Not found request'}
    

