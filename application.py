from datetime import datetime
from flask import Flask, render_template, send_from_directory
from flask_cors import CORS

from routes import *

# Elastic Beanstalk wants `application` to be present.
application = app = Flask(__name__)
CORS(app)

app.register_blueprint(routes)


@app.context_processor
def inject_date():
    """
    Inject date so it can be used in the footer easily.
    """
    year = datetime.now().year
    return dict(year=year)


@app.after_request
def add_cache_control(response):
    # Set cache control headers here
    response.cache_control.max_age=7776000
    response.cache_control.s_max_age=7776000
    return response


@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")


@app.route('/robots.txt')
def static_from_root():
    return send_from_directory(app.static_folder, request.path[1:])
