from flask import request, jsonify
import cx_Oracle

def oracle_db_test_connection():
    if request.method == 'POST':
        if request.json["host"] and  request.json["port"] and request.json["user"] and  request.json["password"] \
             and  request.json["service"]:
            try:
                host=request.json["host"]
                port= int(request.json["port"])
                user= request.json["user"]
                password= request.json["password"]
                service = request.json["service"]
                
                CONN_INFO = {
                    'host': host,
                    'port': port,
                    'user': user,
                    'psw': password,
                    'service': service
                }
                print (CONN_INFO)

                CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO)
                cx_Oracle.connect(CONN_STR)

                return jsonify({"status":"ok","msg":"Connection established successfully"})
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})