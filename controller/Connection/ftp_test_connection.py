from flask import request, jsonify
from ftplib import FTP

def ftp_test_connection():
    if request.method == 'POST':
        if request.json["host"] and  request.json["port"] and request.json["user"] and  request.json["password"] and  request.json["path"]:
            try:
                host=request.json["host"]
                port= int(request.json["port"])
                user= request.json["user"]
                password= request.json["password"]
                path = request.json["path"]

                # print(host, port, user, password, path)

                ftp = FTP()
                ftp.connect(host=host, port=port)
                ftp.login(user=user, passwd=password)
                ftp.cwd(path)

                return jsonify({"status":"ok","msg":"Connection established successfully"})
            except Exception as e:
                return jsonify({"status":"Failed", "err":str(e)})

        return jsonify({"status":"Failed", "err":"Body part missing"})
    return jsonify({"status":"Failed", "err":"Method Not Allowed"})