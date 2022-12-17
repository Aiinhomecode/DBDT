from flask import app
from werkzeug.exceptions import HTTPException
from app import app

@app.errorhandler(HTTPException)
def handle_httpException(e):
    return 'Failed'
