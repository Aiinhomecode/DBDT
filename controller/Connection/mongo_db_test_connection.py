from flask import request, jsonify
from pymongo import MongoClient

def mongo_db_test_connection():
	if request.method == 'POST':
            if request.json["host"] and  request.json["port"]:
                    try:
                        host=request.json["host"]
                        port= int(request.json["port"])
                        user= request.json["user"]
                        password= request.json["password"]
                        database = request.json["database"]
                        client = MongoClient(f'{host}',port, username=''+user+'', password=''+password+'')
                        mydb = client[database]
                        mydatabase = mydb.list_collection_names()
                        if mydatabase is not None:
                            return jsonify({"status":"ok","msg":"Connection established successfully"})
                        else:    
                            return jsonify({"status":"Failed","msg":"Connection not established successfully"})
                    except Exception as e:
                        return jsonify({"status":"Failed", "err":str(e)})