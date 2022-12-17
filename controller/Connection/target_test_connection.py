from .test_db_connection import mysql_con, sqlserver_con
from flask import request, jsonify

def target_test_connection():
    if request.method == 'POST':
            if request.json["target"] and  request.json["host"] and request.json["port"] and request.json["username"] and request.json["password"] and request.json["db"]:
                target = request.json["target"]
                host = request.json["host"]
                port = request.json["port"]
                username = request.json["username"]
                password = request.json["password"]
                db = request.json["db"]
                print(target, host,port,username,password,db, end='\n')
                if target == 'mysql':
                    con_rtn = mysql_con(host, port, username, password, db)
                    if con_rtn["status"]:
                        return jsonify({"response":"Success","data":"Successfully Connected",})
                    return jsonify({"response":"Failed","data":"Connection Not Established","err":con_rtn["err"]})
                else:
                    con_rtn = sqlserver_con(host, port, username, password, db)
                    if con_rtn["status"]:
                        return jsonify({"response":"Success","data":"Successfully Connected",})
                    return jsonify({"response":"Failed","data":"Connection Not Established","err":con_rtn["err"]})

                return jsonify({"response":"Failed","data":"Connection Not Established",})
            return jsonify({"response":"Failed","message":"body part missing"})
    return jsonify({"response":"Failed","message":"Method not allow"})