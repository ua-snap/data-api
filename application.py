from datetime import datetime
from flask import Flask, render_template, send_from_directory
from flask_cors import CORS
from config import SITE_OFFLINE

from routes import *

# Elastic Beanstalk wants `application` to be present.
application = app = Flask(__name__)
CORS(app)

app.register_blueprint(routes)


def get_service_categories():
    """
    This is the new location for the service_categories on the main page.
    This will function will be called on the default route and the list
    will be passed to the index.html template.
    """
    return [
        ("Climate Indicators", "/indicators"),
        ("Climate Protection from Spruce Beetles", "/beetles"),
        ("Degree Days", "/degree_days"),
        ("Digital Elevation Models (DEMs)", "/elevation"),
        ("Flammability and Vegetation Type (ALFRESCO)", "/alfresco"),
        ("Geology", "/geology"),
        ("Hydrology", "/hydrology"),
        ("Landfast Sea Ice", "/landfastice"),
        ("Permafrost", "/permafrost"),
        ("Physical and Administrative Boundary Polygons", "/boundary"),
        ("Physiography", "/physiography"),
        ("Sea Ice Concentration", "/seaice"),
        ("Snowfall Equivalent", "/snow"),
        ("Temperature and Precipitation", "/taspr"),
        ("Wet Days Per Year", "/wet_days_per_year"),
        ("Wildfire", "/fire"),
        ("Demographics", "/demographics"),
    ]


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
    response.cache_control.max_age = 7776000
    return response


@app.route("/")
def index():
    """Render index page"""
    # Sort the service categories by category name
    service_categories = sorted(get_service_categories(), key=lambda x: x[0])
    return render_template(
        "index.html", service_categories=service_categories, SITE_OFFLINE=SITE_OFFLINE
    )


@app.route("/robots.txt")
def static_from_root():
    return send_from_directory(app.static_folder, request.path[1:])
