from flask import request, jsonify
from numpy import source
import pandas as pd
from db_connection import conn

def get_drive_details():
    if request.method == 'POST':
        try:
            if request.json['source_id']:
                source_id = request.json['source_id']
                source = source_id.split('_')[0]
                id = source_id.split('_')[-1]

                if source.lower().strip() == 'googledrive':
                    df = pd.read_sql(f"SELECT CONCAT('GOOGLEDRIVE_',[ID]) AS [SOURCE_ID], [SOURCE_NAME], [DRIVE_TOKEN], [FOLDER_ID] FROM [dbo].[GOOGLE_DRIVE_SOURCE] WHERE ID={id}", con= conn)
                    df_dict = df.to_dict(orient='records')
                    out_data = {
                        "status":"Success",
                        "data": df_dict,
                    }
                    return jsonify(**out_data)
                else:
                    return jsonify({"status":"Failed","err":"Something went wrong"})
        except Exception as e:
            return jsonify({"status":"Failed","err":"Something went wrong", "m": str(e)})
    return jsonify({"status":"Failed","err":"Method not allowed"})