import asyncio
from aiohttp import ClientSession
from flask import Flask, render_template
from routes import *

app = Flask(__name__)

app.register_blueprint(routes)


@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")
