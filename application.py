from datetime import datetime
import logging
import sys
from flask import Flask, render_template, send_from_directory
from flask_cors import CORS
from config import SITE_OFFLINE, geojson_names
from marshmallow import Schema, fields, validate, ValidationError
import re

from luts import (
    fire_weather_ops,
    all_cmip6_downscaled_models,
    all_cmip6_downscaled_scenarios,
)

from routes import routes, request

# Configure logging to emit to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

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
        ("CMIP6, Downscaled", "/cmip6_downscaled"),
        ("Climate Indicators", "/indicators"),
        ("Climate Indicators, Dynamic", "/dynamic_indicators"),
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
        ("Temperature Anomalies", "/temperature_anomalies"),
        ("Temperature and Precipitation", "/taspr"),
        ("Wet Days Per Year", "/wet_days_per_year"),
        ("Wildfire", "/fire"),
        ("WRF Dynamically Downscaled ERA5 Reanalysis", "/era5wrf"),
        ("Demographics", "/demographics"),
        ("CMIP6 Fire Weather Indices", "/fire_weather"),
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

        # Make sure "tags" parameter is only letters and commas, and less than
        # or equal to 50 characters long.
        tags = fields.Str(
            validate=lambda str: bool(re.match(r"^[A-Za-z,]{0,50}$", str)),
            required=False,
        )

        # Make sure "extent" parameter is one of the predefined extents
        extent = fields.Str(
            validate=validate.OneOf(geojson_names),
            required=False,
        )

        # Make sure "substring" parameter is less than or equal to 50 characters long, allow all UTF-8 characteres
        substring = fields.Str(
            validate=lambda str: len(str) <= 50,
            required=False,
        )

        def validate_vars(value):
            """
            Validate a comma-separated list of variable names.

            The regex is constructed such that it validates that the input string:
                - Is between 1 and 200 characters long.
                - Each variable name is alphanumeric, and may include underscores

            Args:
                value (str): raw `vars` query parameter to validate

            Returns:
                bool: True if validation succeeds.

            Raises: ValidationError: when `value` not a valid vars string
            """
            # 200 is arbitrary, but endpoints (e.g., era5wrf) have many vars
            climate_var_regex = re.compile(
                r"^(?=.{1,200}$)[A-Za-z0-9,_]+$"
            )
            if not climate_var_regex.match(value):
                raise ValidationError("Invalid var(s) provided.")
            return True

        vars = fields.Str(
            validate=validate_vars,
            required=False,
        )

        # Make sure "models" parameter contains only valid model names.
        def validate_models(value):
            items = value.split(",")
            if not all(item in all_cmip6_downscaled_models for item in items):
                raise ValidationError("Invalid model(s) provided.")
            return True

        models = fields.Str(
            validate=validate_models,
            required=False,
        )

        # Make sure "scenarios" parameter contains only valid scenario names.
        def validate_scenarios(value):
            items = value.split(",")
            if not all(item in all_cmip6_downscaled_scenarios for item in items):
                raise ValidationError("Invalid scenario(s) provided.")
            return True

        scenarios = fields.Str(
            validate=validate_scenarios,
            required=False,
        )

        # Make sure "op" parameter is one of the predefined fire weather operations
        op = fields.Str(
            validate=validate.OneOf(fire_weather_ops),
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
