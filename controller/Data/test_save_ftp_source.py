from flask import request, jsonify
from ftplib import FTP
from db_connection import conn
import pandas as pd
from datetime import datetime as dt

def test_save_ftp_source():
    if request.method == 'POST':
        if request.json["host"] and  request.json["port"] and request.json["user"] and  request.json["password"] \
             and  request.json["path"] and  request.json["source_name"] and request.json["file_format"] and request.json["user_id"]:
            try:
                host=request.json["host"]
                port= int(request.json["port"])
                user= request.json["user"]
                password= request.json["password"]
                path = request.json["path"]
                source_name = request.json["source_name"]
                file_format = request.json["file_format"]
                user_id = request.json["user_id"]

                # print(host, port, user, password, path)
                rtn = pd.read_sql(F"SELECT 1 FROM FTP_SOURCE WHERE SOURCE_NAME='{source_name}'", con=conn)
                if len(rtn.index)>0:
                    return jsonify({"status":"Failed", "err":"Source already exists"})

                ftp = FTP()
                ftp.connect(host=host, port=port)
                ftp.login(user=user, passwd=password)
                ftp.cwd(path)

                data = [{
                    "USER_ID":user_id,
                    "SOURCE_NAME":source_name,
                    "HOST":host,
                    "PORT":port,
                    "USER":user,
                    "PASSWORD":password,
                    "PATH":path,
                    "FILE_FORMAT": file_format,
                    "CREATED_AT": str(dt.now())[0:23]
                }]
                df = pd.DataFrame(data)
                # sqlQuery = f"""INSERT INTO FTP_SOURCE([USER_ID],[SOURCE_NAME],[HOST],[PORT],[USER],[PASSWORD],[PATH],[FILE_FORMAT],[CREATED_AT])
                #  VALUES({user_id},'{source_name}','{host}',{port},'{user}','{password}','{path}','{file_format}','{str(dt.now())[0:23]}')"""
                df.to_sql("FTP_SOURCE", con= conn, if_exists='append', index=False)

                return jsonify({"status":"ok","msg":"Connection established successfully and saved"})
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})