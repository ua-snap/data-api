import asyncio
import ast
import logging
import xarray as xr
import cftime
import io
import requests
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

# TODO: for additional postprocessing or csv output, uncomment these imports and add code
# from postprocessing import postprocess, prune_nulls_with_max_intensity
# from csv_functions import create_csv

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


var_coverage_metadata = {}  # dict to hold coverage metadata for each variable
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


def start_year_to_time_value(year, base_date):
    date = datetime.datetime(year, 4, 1)
    delta_days = (date - base_date).days
    return delta_days


def end_year_to_time_value(year, base_date):
    date = datetime.datetime(year, 10, 1)
    delta_days = (date - base_date).days
    return delta_days


def time_value_to_year(time_value, base_date):
    date = base_date + datetime.timedelta(days=time_value)
    return date.year


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

    try:
        start_year = int(start_year)
        start_time_value = start_year_to_time_value(start_year, base_date)
        if start_time_value < time_min or start_time_value > time_max:
            return 400
    except ValueError as e:
        return 400

    try:
        end_year = int(end_year)
        end_time_value = end_year_to_time_value(end_year, base_date)
        if end_time_value < time_min or end_time_value > time_max:
            return 400
    except ValueError as e:
        return 400

    return start_time_value, end_time_value


def fetch_data(requested_vars, lat, lon, times):
    """
    Fetch the data for the requested variables at the given lat/lon and time range.
    Args:
        requested_vars (list): list of requested variables
        lat (float): latitude
        lon (float): longitude
        times (tuple): (start_time_value, end_time_value) in days since base date, or None
    Returns:
        dict: fetched data as xarray.Datasets, one per variable
    """

    data_dict = {}
    for var in requested_vars:
        coverage_id = var_coverage_metadata[var]["coverage_id"]
        time_slice = None
        if times:
            time_slice = ("time", f"{times[0]},{times[1]}")

        url = generate_wcs_query_url(
            generate_wcs_getcov_str(
                x=lon,
                y=lat,
                cov_id=coverage_id,
                var_coord=None,
                time_slice=time_slice,
                encoding="netcdf",
                projection="EPSG:4326",
            )
        )

        url += f"&RANGESUBSET={var}"

        with requests.get(url) as response:
            print("requesting data from:", url)
            # return 500 for any non-200 response
            if response.status_code != 200:
                return render_template("500/server_error.html"), 500
            ds = xr.open_dataset(io.BytesIO(response.content))

        data_dict[var] = ds

    return data_dict


def postprocess(data_dict, var_coverage_metadata, start_year, end_year):
    """
    Postprocess the data as needed.

    #TODO: Decide what kind of postprocessing we really want here.

    Demo:
    For each dataset in the dictionary, we will take a 3-day rolling average of values (smoothing).
    Then we summarize min, mean, and max of those rolling averages across the entire time range, for each model (including the baseline).
    Return the data as a dictionary with variable, model, time range, and min/mean/max values for each DOY from April 1 to October 31.

    Args:
        data_dict (dict): dict of xarray.Datasets, one per variable
        var_coverage_metadata (dict): metadata for each variable, which includes model encoding
        start_year (str): start year of the data
        end_year (str): end year of the data
    Returns:
        dict: postprocessed data
    """

    # highest level of the returned dict is year range
    if start_year is None:
        start_year = time_value_to_year(
            var_coverage_metadata[var]["time_min"],
            var_coverage_metadata[var]["time_units"].split("since")[1].strip(),
        )
        end_year = time_value_to_year(
            var_coverage_metadata[var]["time_max"],
            var_coverage_metadata[var]["time_units"].split("since")[1].strip(),
        )
        year_range_str = str(start_year + "-" + end_year)
    else:
        year_range_str = str(start_year + "-" + end_year)

    var_data_summary = {year_range_str: {}}

    # next levels are variable, model, and min/mean/max of 3-day rolling average per DOY
    for var in data_dict:
        var_data_summary[year_range_str][var] = {}

        ds = data_dict[var]
        # Apply a 3-day rolling average along the time dimension
        ds_rolled = ds.rolling(time=3, center=True).mean()

        # Group by day of year and model, and calculate min, mean, max
        ds_min_doy = ds.groupby(["time.dayofyear", "model"]).min()
        ds_mean_doy = ds.groupby(["time.dayofyear", "model"]).mean()
        ds_max_doy = ds.groupby(["time.dayofyear", "model"]).max()

        for model in ds_mean_doy["model"].values:
            model_name_str = var_coverage_metadata[var]["model_encoding"][int(model)]
            var_data_summary[year_range_str][var][model_name_str] = {}
            # for each DOY in the dataset extract min/mean/max values and create a dict of them under that DOY
            for doy in ds_mean_doy["dayofyear"].values:
                var_data_summary[year_range_str][var][model_name_str][int(doy)] = {
                    "min": float(
                        ds_min_doy.sel(dayofyear=doy, model=model).to_array().values[0]
                    ),
                    "mean": float(
                        ds_mean_doy.sel(dayofyear=doy, model=model).to_array().values[0]
                    ),
                    "max": float(
                        ds_max_doy.sel(dayofyear=doy, model=model).to_array().values[0]
                    ),
                }

    return var_data_summary


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
    else:
        requested_vars = [var for var in var_coverage_metadata]

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

    # fetch the data
    data = postprocess(
        fetch_data(requested_vars, float(lat), float(lon), times),
        var_coverage_metadata,
        start_year,
        end_year,
    )

    return data
