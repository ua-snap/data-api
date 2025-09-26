import asyncio
import ast
import logging
import xarray as xr
import cftime
import io
import numpy as np
from flask import Blueprint, render_template, request
import datetime

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    validate_year,
)

# TODO: for additional postprocessing or csv output, uncomment these imports and add code
# from postprocessing import postprocess, prune_nulls_with_max_intensity
# from csv_functions import create_csv

from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("dynamic_indicators_api", __name__)


#### SETUP AND METADATA ####

coverage_ids = [
    "cmip6_daily_pr_historical",
    "cmip6_daily_pr_projected",
]


# function to get the basic metadata for all coverages
async def get_coverage_metadata(coverage_id):
    """Get the coverage metadata."""
    metadata = await describe_via_wcps(coverage_id)
    return metadata


# build dict to hold coverage metadata for each variable / scenario
var_coverage_metadata = {}
for coverage_id in coverage_ids:
    coverage_metadata = asyncio.run(get_coverage_metadata(coverage_id))
    # assumes only one variable per coverage!
    var = list(coverage_metadata["metadata"]["bands"].keys())[0]

    if var not in var_coverage_metadata:
        var_coverage_metadata[var] = {"projected": {}, "historical": {}}
    else:
        pass
    if "projected" in coverage_id:
        era = "projected"
    if "historical" in coverage_id:
        era = "historical"

    var_coverage_metadata[var][era] = {
        "coverage_id": coverage_id,
        # Convert the string representation of model encoding dict to an actual dict
        "model_encoding": ast.literal_eval(
            coverage_metadata["metadata"]["axes"]["model"]["encoding"]
        ),
        "scenario_encoding": ast.literal_eval(
            coverage_metadata["metadata"]["axes"]["scenario"]["encoding"]
        ),
        "time_units": coverage_metadata["metadata"]["axes"]["time"]["units"],
        # "time_min": int(coverage_metadata["metadata"]["axes"]["time"]["min_value"]), # consider adding these attributes to the coverages for easier time validation
        # "time_max": int(coverage_metadata["metadata"]["axes"]["time"]["max_value"]), # consider adding these attributes to the coverages for easier time validation
    }


#### DATE CONVERSION FUNCTIONS ####


def date_to_cftime_value(year, month, day, base_date):
    """Convert a year, month, and day to a CF-compliant time value (days since the base date)."""
    date = datetime.datetime(year, month, day)
    delta_days = (date - base_date).days
    return delta_days


def cftime_value_to_year_month_day(time_value, base_date):
    """Convert a time value in days since the base date to a year, month, day tuple."""
    date = base_date + datetime.timedelta(days=time_value)
    return date.year, date.month, date.day


#### VALIDATION FUNCTIONS ####


#### DATA FETCHING FUNCTIONS ####


async def fetch_data_for_all_scenarios(var, lat, lon, times):
    """
    Fetch the data for the requested variables at the given lat/lon and time range.
    Data is fetched from more than 1 coverage due to file size constraints in the backend.

    Args:
        var (str): the requested variable
        lat (float): latitude
        lon (float): longitude
        times (tuple): (start_time_value, end_time_value) in days since base date, or None
    Returns:
        xarray.Dataset: fetched data as an xarray.Datasets, with historical and projected combined
    """
    tasks = []

    coverages = [
        var_coverage_metadata[var]["historical"]["coverage_id"],
        # var_coverage_metadata[var]["projected"]["coverage_id"], # need time min/max in metadata in order to safely enable this
    ]

    for coverage_id in coverages:
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

        tasks.append(fetch_data([url]))

    results = await asyncio.gather(*tasks)

    datasets = []
    for result in results:
        ds = xr.open_dataset(io.BytesIO(result))
        datasets.append(ds)

    # combine historical and projected datasets along the time dimension
    combined_ds = xr.concat(datasets, dim="time")

    return combined_ds


#### POSTPROCESSING FUNCTIONS ####
#
# - compute_max_nday(n)
#   - covers existing rx1day and rx5day indicators
#
# TODO: add additional indicators here, e.g.:
# - compute_days_above_threshold(t)
#   - covers r10mm
#
# - compute_consecutive_wetter_or_drier(t, type)
#   - covers cwd and cdd


def compute_max_nday(precip_data, n):
    """Compute the maximum n-day precipitation from a 1D array of daily precipitation data.

    Args:
        precip_data (np.ndarray): 1D array of daily precipitation values
        n (int): number of days over which to compute the maximum precipitation sum

    Returns:
        float: maximum n-day precipitation sum
    """
    max_nday = 0
    for i in range(len(precip_data) - (n - 1)):
        n_day_sum = sum(precip_data[i : i + n])
        if n_day_sum > max_nday:
            max_nday = n_day_sum
    return max_nday


def max_nday_precip(ds, n):
    """Compute the maximum n-day precipitation from daily precipitation data for all years in the dataset.

    Args:
        ds (xarray.Dataset): dataset containing daily precipitation data with a 'pr' variable
        n (int): number of days over which to compute the maximum precipitation sum

    Returns:
        dict: dictionary with model as keys and maximum n-day precipitation values for each year as values
    """

    # for each model, compute max n-day precip for each year
    results = {}
    for model in ds["model"].values:
        model_data = ds.sel(model=model)
        years = np.unique(model_data["time"].values)
        model_results = {}
        for year in years:
            year_data = model_data.sel(time=model_data["time"].dt.year == year)
            precip_data = year_data["pr"].values
            max_nday = compute_max_nday(precip_data, n)

            # convert datetime year to YYYY string for JSON compatibility (use str from time)
            year_str = year.strftime("%Y")
            model_results[year_str] = max_nday
        results[model] = model_results

    return results


#### FLASK ROUTES ####


@routes.route("/dynamic_indicators/")
def fire_weather_about():
    return render_template("/documentation/dynamic_indicators.html")


@routes.route("/dynamic_indicators/max_nday_precip/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_max_nday_precip_point_data(lat, lon, start_year=None, end_year=None):
    """Compute the maximum n-day precipitation indicator for all years in the specified range at the given lat/lon.

    Args:
        lat: latitude in decimal degrees
        lon: longitude in decimal degrees
        start_year: start year (e.g., 2000) or None
        end_year: end year (e.g., 2010) or None

    Use GET parameter n to specify number of days. Default is 5.
    Example: n=5 for max 5-day precipitation

    If start_year or end_year is None, fetches the full range of available data.

    Example request:
    http://127.0.0.1:5000/dynamic_indicators/max_nday_precip/65/-147/2000/2010?n=5

    Returns:
        dict: JSON response with the computed indicator values for each year and each model.

    """
    # TODO: validate inputs
    print(
        f"Received lat: {lat}, lon: {lon}, start_year: {start_year}, end_year: {end_year}"
    )
    print(f"Query parameter n: {request.args.get('n')}")

    var = "pr"

    # convert years to time values
    base_date_str = var_coverage_metadata[var]["historical"]["time_units"].split(
        "since "
    )[1]
    base_date = datetime.datetime.strptime(base_date_str, "%Y-%m-%d")
    start_time_value = date_to_cftime_value(int(start_year), 1, 1, base_date)
    end_time_value = date_to_cftime_value(int(end_year), 12, 31, base_date)
    times = (start_time_value, end_time_value)

    # fetch data
    ds = asyncio.run(fetch_data_for_all_scenarios(var, float(lat), float(lon), times))

    # compute max n-day precip
    n = int(request.args.get("n", 5))
    max_nday = max_nday_precip(ds, n)

    return max_nday
