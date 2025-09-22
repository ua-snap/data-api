import asyncio
import ast
import logging
from flask import Blueprint, render_template, request
import datetime

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    check_geotiffs,
    validate_year,
)
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv
from . import routes


logger = logging.getLogger(__name__)

cmip6_api = Blueprint("fire_weather_api", __name__)

# we have one geotiff to validate all fire weather variables
# so its not the same as coverage names
fire_weather_geotiff = "cmip6_all_fire_weather_variables"

fire_weather_coverage_ids = [
    "cmip6_bui",
    "cmip6_dmc",
    "cmip6_dc",
    "cmip6_ffmc",
    "cmip6_fwi",
    "cmip6_isi",
]

# get the basic metadata for all fire weather coverages


async def get_coverage_metadata(coverage_id):
    """Get the coverage metadata."""
    metadata = await describe_via_wcps(coverage_id)
    return metadata


var_coverage_metadata = {}

for coverage_id in fire_weather_coverage_ids:
    coverage_metadata = asyncio.run(get_coverage_metadata(coverage_id))
    # assumes only one variable per coverage!
    var_coverage_metadata[list(coverage_metadata["metadata"]["bands"].keys())[0]] = {
        "coverage_id": coverage_id,
        # Convert the string representation of model encoding dict to an actual dict
        "model_encoding": ast.literal_eval(
            coverage_metadata["metadata"]["axes"]["model"]["encoding"]
        ),
        "time_units": coverage_metadata["metadata"]["axes"]["time"]["units"],
        "time_min": int(coverage_metadata["metadata"]["axes"]["time"]["min_value"]),
        "time_max": int(coverage_metadata["metadata"]["axes"]["time"]["max_value"]),
    }


def validate_years_against_coverage_metadata(start_year, end_year, var):
    """
    Validate the start and end years against the coverage metadata.
    Args:
        start_year (int): start year
        end_year (int): end year
        var (str): variable to get coverage metadata for
    Returns:
        tuple: (start_time_value, end_time_value) in days since base date"""

    time_units, time_min, time_max = (
        var_coverage_metadata[var]["time_units"],
        var_coverage_metadata[var]["time_min"],
        var_coverage_metadata[var]["time_max"],
    )

    # Extract the base date from the time units string
    try:
        base_date_str = time_units.split("since")[1].strip()
        base_date = datetime.datetime.strptime(base_date_str, "%Y-%m-%d")
    except (IndexError, ValueError) as e:
        return 400

    def start_year_to_time_value(year):
        date = datetime.datetime(year, 4, 1)
        delta_days = (date - base_date).days
        return delta_days

    def end_year_to_time_value(year):
        date = datetime.datetime(year, 10, 1)
        delta_days = (date - base_date).days
        return delta_days

    try:
        start_year = int(start_year)
        start_time_value = start_year_to_time_value(start_year)
        if start_time_value < time_min or start_time_value > time_max:
            return 400
    except ValueError as e:
        return 400

    try:
        end_year = int(end_year)
        end_time_value = end_year_to_time_value(end_year)
        if end_time_value < time_min or end_time_value > time_max:
            return 400
    except ValueError as e:
        return 400

    return start_time_value, end_time_value


@routes.route("/fire_weather/")
def fire_weather_about():
    return render_template("/documentation/fire_weather.html")


@routes.route("/fire_weather/point/<lat>/<lon>")
@routes.route("/fire_weather/point/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_fire_weather_point_data(lat, lon, start_year=None, end_year=None):
    """
    Query the daily fire weather coverage

    Args:
        lat (float): latitude
        lon (float): longitude
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested daily fire weather data

    Notes:
        example request (all variables): http://localhost:5000/fire_weather/point/65.06/-146.16
        example request (select variables): http://localhost:5000/fire_weather/point/65.06/-146.16?vars=bui,fwi
        example request (all variables, select years): http://localhost:5000/fire_weather/point/65.06/-146.16/2000/2005
    """

    # Validate lat/lon values
    latlon_validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    geotiff_validation = check_geotiffs(
        float(lat), float(lon), coverages=[fire_weather_geotiff]
    )

    if latlon_validation != True:
        validation = latlon_validation
    elif geotiff_validation != True:
        validation = geotiff_validation
    else:
        validation = True

    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 404:
        return (
            render_template("404/no_data.html"),
            404,
        )

    # Validate any requested variables
    requested_vars = request.args.get("vars")
    if requested_vars:
        requested_vars = requested_vars.split(",")
        for var in requested_vars:
            if var not in var_coverage_metadata:
                return render_template("422/invalid_get_parameter.html"), 422
        # Filter the coverage IDs to only those requested
        coverage_ids = [
            var_coverage_metadata[var]["coverage_id"] for var in requested_vars
        ]
    else:
        # get all vars and their coverage ids
        requested_vars = [var for var in var_coverage_metadata]
        coverage_ids = fire_weather_coverage_ids

    # Validate the start and end years for each requested variable's coverage
    for var in requested_vars:
        if start_year is None and end_year is None:
            pass  # No validation needed if both are None
        elif start_year is None or end_year is None:
            return render_template("400/bad_request.html"), 400  # Both must be provided
        else:
            if validate_year(start_year, end_year) == True:
                times = validate_years_against_coverage_metadata(
                    start_year, end_year, var
                )
            if not isinstance(times, tuple):

                return render_template("400/bad_request.html"), 400

    return f"filter data for {requested_vars}, in coverages {coverage_ids}, by {times}, at point {lat} {lon}"  # TODO remove this line when ready
