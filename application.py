from datetime import datetime
from flask import Flask, render_template, send_from_directory
from flask_cors import CORS
from config import SITE_OFFLINE
from marshmallow import Schema, fields, validate
import re

from routes import routes, request

# Elastic Beanstalk wants `application` to be present.
application = app = Flask(__name__)
CORS(app)

app.register_blueprint(routes)


def get_service_categories():
    """
    This is the location for the service_categories on the main page.
    This will function will be called on the default route and the list
    will be passed to the index.html template.
    """
    return [
        ("CMIP6", "/cmip6"),
        ("Climate Indicators", "/indicators"),
        ("Climate Protection from Spruce Beetles", "/beetles"),
        ("Degree Days", "/degree_days"),
        ("Digital Elevation Models (DEMs)", "/elevation"),
        ("Flammability and Vegetation Type (ALFRESCO)", "/alfresco"),
        ("Hydrology", "/hydrology"),
        ("Landfast Sea Ice", "/landfastice"),
        ("Permafrost", "/permafrost"),
        # ("Physical and Administrative Boundary Polygons", "/boundary"),
        # ("Ecoregions", "/ecoregions"),
        ("Sea Ice Concentration", "/seaice"),
        ("Snowfall Equivalent", "/snow"),
        ("Temperature and Precipitation", "/taspr"),
        ("Wet Days Per Year", "/wet_days_per_year"),
        ("Wildfire", "/fire"),
        ("Demographics", "/demographics"),
    ]


def get_geospatial_categories():
    """
    This is the location for the geospatial_categories on the main page.
    This will function will be called on the default route and the list
    will be passed to the index.html template.
    """
    return [
        ("Communities, Places, and Areas of Interest", "/places"),
        ("GeoJSON Polygon Data", "/boundary"),
        # ("Ecoregions", "/ecoregions"),
    ]


@app.context_processor
def inject_date():
    """
    Inject date so it can be used in the footer easily.
    """
    year = datetime.now().year
    return dict(year=year)


@app.before_request
def validate_get_params():
    class QueryParamsSchema(Schema):
        format = fields.Str(validate=validate.OneOf(["csv"]), required=False)
        summarize = fields.Str(validate=validate.OneOf(["mmm"]), required=False)

        # Make sure "community" parameter is only uppercase letters and
        # numbers, and less than or equal to 10 characters long.
        community = fields.Str(
            validate=lambda str: bool(re.match(r"^[A-Z0-9]{0,10}$", str)),
            required=False,
        )

        # Make sure "vars" parameter is only letters and commas, and less than
        # or equal to 100 characters long.
        vars = fields.Str(
            validate=lambda str: bool(re.match(r"^[A-Za-z,]{0,100}$", str))
            and len(str) < 100,
            required=False,
        )

        # Make sure "tags" parameter is only letters and commas, and less than
        # or equal to 50 characters long.
        tags = fields.Str(
            validate=lambda str: bool(re.match(r"^[A-Za-z,]{0,50}$", str)),
            required=False,
        )

    schema = QueryParamsSchema()
    errors = schema.validate(request.args)
    if errors:
        return render_template("422/invalid_get_parameter.html"), 422


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
    geospatial_categories = sorted(get_geospatial_categories(), key=lambda x: x[0])
    return render_template(
        "index.html",
        service_categories=service_categories,
        geospatial_categories=geospatial_categories,
        SITE_OFFLINE=SITE_OFFLINE,
    )


@app.route("/robots.txt")
def static_from_root():
    return send_from_directory(app.static_folder, request.path[1:])
