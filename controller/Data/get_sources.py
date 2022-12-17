from flask import request, jsonify
import pandas as pd
from db_connection import conn

def get_sources():
    if request.method == 'POST':
        if request.json["user_id"]:
            user_id = int(request.json["user_id"])
            df = pd.read_sql(f"exec [GET_SOURCE_LIST] {user_id}", con= conn)
            dict_data = df.to_dict(orient='records')
            final_data = {
                "status":"Success",
                "data": dict_data,
            }
            return jsonify(**final_data)
    return jsonify({"status":"Failed","err":"Method not allowed"})
