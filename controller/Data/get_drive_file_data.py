from flask import request, jsonify
import pandas as pd

def get_drive_file_data():
    if request.method == 'POST':
        try:
            if 'file_id' in request.json:
                file_id = request.json['file_id']
                dwn_url=f'https://drive.google.com/uc?id={file_id}'
                # print(dwn_url)
                df = pd.read_csv(dwn_url)[:10]
                df.fillna('', inplace=True)
                df_dict = df.to_dict(orient='records')
                result_dict = {
                    "status":"success",
                    "data":df_dict,
                    "columns":list(df.columns)
                }
                return jsonify(**result_dict)
            return jsonify({"status":"failed","err":"Bodypart missing"})
        except Exception as e:
            return jsonify({"status":"failed","err":"Somehing went wrong", "msg": str(e)})
    return jsonify({"status":"failed","err":"Method not allowed"})