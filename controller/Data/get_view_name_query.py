from flask import request, jsonify
import pandas as pd
from db_connection import conn

def get_view_query():
    if request.method == 'POST':
        try:
            if request.json["view_user_id"]:
                user_id = request.json["view_user_id"]
                df = pd.read_sql(f"select [VIEW_NAME],[QUERY] from [VIEW_QUERY] where [VIEW_USER_ID]='"+user_id+"'", con=conn)
                d_dict=df.to_dict(orient="records")
                final_data ={
                    "response":"success",
                    "data":d_dict
                    }     
            return jsonify(**final_data)
                # data = [{
                    # "VIEW_USER_ID" : user_id,
                    # "VIEW_NAME" : query_name,
                    # "QUERY" : user_query
                # }]
                # 
                # df = pd.DataFrame(data)
                # rtn = pd.read_sql(f"select 1 from [VIEW_QUERY] where [VIEW_NAME]='"+query_name+"' AND [VIEW_USER_ID]='"+user_id+"'", con=conn)
                # print (rtn)
                # if len(rtn.index)>0:
                        # return jsonify({"response":"Failed","err":"View Name Already Exists",})
                # df.to_sql("VIEW_QUERY", con=conn, index=False, if_exists='append')
                # return jsonify({"response":"Success","data":"Data Saved successfully!",})        
        except Exception as e:
                response={'response': "Failed", 'data': 'Data Not Saved successfully!', "err": str(e)}
                return jsonify(response)  
