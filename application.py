from datetime import datetime
from flask import Flask, render_template
from flask_cors import CORS
from luts import update_needed

if update_needed:
    from routes.vectordata import update_data

    update_data()

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


@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")
