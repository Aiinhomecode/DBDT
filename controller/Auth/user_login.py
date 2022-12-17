from operator import index
from db_connection import conn
from flask import request,jsonify
import pandas as pd
import json

def user_login() :
        if 'email' and 'password' not in request.form:
            return jsonify({'login status': 'Failed', 'message': 'failed', 'data': ' '})
        email = request.form['email']
        password = request.form['password']
        
        if email == '' or password == '':
            return jsonify({'login_status': 'Failed', 'message': 'email or password missing', 'data': ' '})

        if email != '' and password != '':
            df = pd.read_sql(f"""DECLARE @RSP NVARCHAR(100)
                EXEC [LOGIN_USER] '{email}','{password}',@RSP OUT
                SELECT @RSP as response""", con=conn)
            out_res = df["response"][0]
            print(out_res)
            # login_user = {'email': email}
            # login_pass = {'password': password}
            try:
                if out_res == 'SUCCESS':
                    data = pd.read_sql(f"select USER_ID, FIRST_NAME,LAST_NAME, EMAIL, CREATED_AT from USER_DETAILS where email='{email}' and password='{password}'", con=conn )
                    data = data.astype(str).copy()
                    result = {'login_status': 'success', 'message': 'Successfully logged in','data': data.to_dict(orient='records')}

                    return jsonify(**result)
                response={'login_status': 'Failed', 'message': 'email or password does not match'}
                return jsonify(response)
            except Exception as e:
                response={'login_status': 'Failed', 'message': 'email or password does not match'}
                return jsonify(response)
