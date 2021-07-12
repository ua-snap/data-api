from flask import Flask
from flask import render_template

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/🔥")
@app.route("/🔥/about")
def fire():
    return render_template("🔥.html")


