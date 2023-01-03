import sqlalchemy
import pandas as pd
import urllib

from flask import request, jsonify
from numpy import source
from flask import Flask,jsonify,make_response
from flask_cors import CORS
from db_connection import conn

def details_GET_PROCESS_MASTER():

    if request.method == 'GET' or request.method == 'POST':
        try:
            user_query='exec [dbo].[GET_PROCESS_MASTER]'
            df = pd.read_sql(user_query, con= conn)
            #print(df)   
            dict_data = df.to_dict(orient='records')
            # print(dict_data)
            final_data = {
                    "status":"Success",
                    "data": dict_data,
                }
            return jsonify(**final_data) #kw args
        except Exception as e:
                response={'response': "Failed", 'data': 'Data Not fetched successfully!', "err": str(e)}
                return jsonify(response)


