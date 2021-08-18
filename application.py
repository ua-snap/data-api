import asyncio
from aiohttp import ClientSession
from flask import Flask, render_template
from flask_cors import CORS
from routes import *

# Elastic Beanstalk wants `application` to be present.
application = app = Flask(__name__)
CORS(app)

app.register_blueprint(routes)


@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")
