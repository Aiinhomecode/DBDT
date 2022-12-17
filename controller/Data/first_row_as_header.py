import pandas as pd
import json
from flask import request,jsonify

def get_header_or_not():
        if request.method == 'GET':
            if request.json['existing_table'] == 'yes':
                if request.json['first_row_as_header'] == 'yes':
                    df = pd.read_csv('C:/Users/sandi/Downloads/STUDENT_SOURCE.csv')
                    return json.dumps(json.loads(df.to_json(orient="records")))
                else:
                    df = pd.read_csv('C:/Users/sandi/Downloads/STUDENT_SOURCE.csv',header=None)
                    return json.dumps(json.loads(df.to_json(orient="records")))
            else:
                return jsonify({"status":"failed","msg":"table is not exist"})
        return jsonify({"status":"failed","msg":"method not allowed"})
