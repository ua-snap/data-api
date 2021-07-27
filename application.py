import asyncio
from aiohttp import ClientSession
from flask import Flask, render_template, abort
from routes import *

app = Flask(__name__)

app.register_blueprint(routes)


@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")


@app.route("/🔥")
@app.route("/🔥/about")
def fire():
    """Render fire page"""
    return render_template("🔥.html")
