from flask import request, jsonify
from db_connection import conn
import pandas as pd


def get_target_connection_details():
    try:
        target_id = request.json['target_id']
        target, id = target_id.split('_')
        if target.lower() == 'sqlserver':
            df = pd.read_sql_query(f"SELECT CONCAT('FTP_',[ID]) AS [TARGET_ID], [TARGET_NAME], [HOST], [PORT], [USER], [DATABASE] FROM [dbo].[SQLSERVER_TARGET] WHERE ID={id}", con= conn)
            response = {
                "status":"Success",
                "data": df.to_dict(orient='records')
            }
    except Exception as e:
        response = {
            "status":"Failed",
            "msg": str(e)
        }
    finally:
        return jsonify(**response)