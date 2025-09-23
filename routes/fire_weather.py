import asyncio
import ast
import logging
import xarray as xr
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
    check_geotiffs,
    validate_year,
)
from luts import summer_fire_danger_ratings_dict

# TODO: for additional postprocessing or csv output, uncomment these imports and add code
# from postprocessing import postprocess, prune_nulls_with_max_intensity
# from csv_functions import create_csv

from . import routes


logger = logging.getLogger(__name__)

cmip6_api = Blueprint("fire_weather_api", __name__)


#### SETUP AND METADATA ####


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


def dayofyear_to_mmdd(dayofyear):
    """Convert integer day of year (1-365) to a MM-DD string for a non-leap year."""
    days = float(dayofyear - 1)  # this cant be an int, so we convert to float
    date = datetime.datetime(2001, 1, 1) + datetime.timedelta(days)
    return date.strftime("%m-%d")


def set_dataset_doy_str(ds):
    """Convert the integer dayofyear coordinate to a MM-DD string for better readability."""
    dayofyear_str = [dayofyear_to_mmdd(doy) for doy in ds["dayofyear"].values]
    ds = ds.assign_coords(dayofyear=("dayofyear", dayofyear_str))
    return ds


def build_variable_year_range_str_from_start_and_end_year(var, start_year, end_year):
    """Build a year range string from start and end year."""
    if start_year is None:
        start_year, _start_month, _start_day = cftime_value_to_year_month_day(
            var_coverage_metadata[var]["time_min"],
            var_coverage_metadata[var]["time_units"].split("since")[1].strip(),
        )
        end_year, _end_month, _end_day = cftime_value_to_year_month_day(
            var_coverage_metadata[var]["time_max"],
            var_coverage_metadata[var]["time_units"].split("since")[1].strip(),
        )
        year_range_str = str(start_year + "-" + end_year)
    else:
        year_range_str = str(start_year + "-" + end_year)
    return year_range_str


#### VALIDATION FUNCTIONS ####


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
    except:
        return 400

    try:
        start_year = int(start_year)
        start_time_value = date_to_cftime_value(start_year, 4, 1, base_date)
        if start_time_value < time_min or start_time_value > time_max:
            return 400
    except:
        return 400

    try:
        end_year = int(end_year)
        end_time_value = date_to_cftime_value(end_year, 10, 1, base_date)
        if end_time_value < time_min or end_time_value > time_max:
            return 400
    except:
        return 400

    return start_time_value, end_time_value


#### DATA FETCHING FUNCTIONS ####


async def fetch_data_for_all_vars(requested_vars, lat, lon, times):
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

    fetched_data = {}
    tasks = []

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

        tasks.append(fetch_data([url]))

    results = await asyncio.gather(*tasks)

    for requested_var, result in zip(requested_vars, results):
        ds = xr.open_dataset(io.BytesIO(result))
        fetched_data[requested_var] = ds

    return fetched_data


#### POSTPROCESSING FUNCTIONS ####


def nday_rolling_avg(n, data_dict, var_coverage_metadata, start_year, end_year):
    """
    For each dataset in the dictionary, we will take an n-day rolling average of values (smoothing).
    Then we summarize min, mean, and max of those rolling averages across the entire time range, for each model (including the baseline).
    Return the data as a dictionary with variable, model, time range, and min/mean/max values for each DOY from April 1 to October 31.

    *** Note that we return NA if any NA are present for a given DOY/model combination.
    This matters for ERA5 model if date range includes both historical and projected (e.g. 2000-2030), because if we allow mean to ignore NA,
    we would be averaging the historical values only which could lead to misunderstandings.
    If the user wants to see the historical values only, they should specify a date range that is fully within the historical period (e.g. 1980-2021).

    Args:
        n (int): number of days for rolling average
        data_dict (dict): dict of xarray.Datasets, one per variable
        var_coverage_metadata (dict): metadata for each variable, which includes model encoding
        start_year (str): start year of the data
        end_year (str): end year of the data
    Returns:
        dict: postprocessed data
    """
    # highest level of the returned dict is year range
    # use first var in data_dict to determine year range if start_year and end_year are None
    var = list(data_dict.keys())[0]
    year_range_str = build_variable_year_range_str_from_start_and_end_year(
        var, start_year, end_year
    )
    var_nday_summary = {year_range_str: {}}

    # next levels are variable, model, and min/mean/max of 3-day rolling average per DOY
    for var in data_dict:
        var_nday_summary[year_range_str][var] = {}

        ds = data_dict[var]
        # Apply a n-day rolling average along the time dimension
        ds_rolled = ds.rolling(time=int(n), center=True).mean()

        # Group by day of year and model, and calculate min, mean, max
        # we want to return NA if any NA are present:
        # this matters for era5 model if date range includes both historical and projected (e.g. 2000-2030)
        ds_min_doy = ds_rolled.groupby(["time.dayofyear", "model"]).min(skipna=False)
        ds_mean_doy = ds_rolled.groupby(["time.dayofyear", "model"]).mean(skipna=False)
        ds_max_doy = ds_rolled.groupby(["time.dayofyear", "model"]).max(skipna=False)

        # Replace the integer DOY with dates in format MM-DD for better readability
        ds_min_doy = set_dataset_doy_str(ds_min_doy)
        ds_mean_doy = set_dataset_doy_str(ds_mean_doy)
        ds_max_doy = set_dataset_doy_str(ds_max_doy)

        # for each model in the dataset create a dict of DOYs under that model
        for model in ds_mean_doy["model"].values:
            # use model names from the coverage metadata
            model_name_str = var_coverage_metadata[var]["model_encoding"][int(model)]
            var_nday_summary[year_range_str][var][model_name_str] = {}
            # for each DOY in the dataset extract min/mean/max values and create a dict of them under that DOY
            for doy in ds_mean_doy["dayofyear"].values:
                var_nday_summary[year_range_str][var][model_name_str][doy] = {
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

    return var_nday_summary


def summer_fire_danger_rating_days(
    data_dict, var_coverage_metadata, start_year, end_year
):
    """For the months of June, July, and August, classify each day in the dataset based on the
    summer fire danger rating adjective classes for each fire weather variable. Count the days in each class
    per year, per model, and per variable. Get an averaege count of days in each class across all years in the dataset,
    rounded to the nearest integer.
    Return the data as a dictionary with variable, model, time range, and average counts for each class.

    ***NOTE: this function will not work for the isi variable, because some values are stored using scientific notation
    and Rasdaman is unable to encode these as netCDF (see this issue: https://github.com/ua-snap/rasdaman-ingest/pull/118#issuecomment-2992373485)

    Args:
        data_dict (dict): dict of xarray.Datasets, one per variable
        var_coverage_metadata (dict): metadata for each variable, which includes model encoding
        start_year (str): start year of the data
        end_year (str): end year of the data
    Returns:
        dict: postprocessed data

    """

    # highest level of the returned dict is year range
    # use first var in data_dict to determine year range if start_year and end_year are None
    first_var = list(data_dict.keys())[0]
    year_range_str = build_variable_year_range_str_from_start_and_end_year(
        first_var, start_year, end_year
    )
    var_summer_fire_summary = {year_range_str: {}}

    # get the total number of years from the year range string (to compute averages later)
    start_year_int = int(year_range_str.split("-")[0])
    end_year_int = int(year_range_str.split("-")[1])
    num_years = end_year_int - start_year_int + 1

    # next levels are variable, model, and the fire ranger rating average counts
    for var in data_dict:
        var_summer_fire_summary[year_range_str][var] = {}

        ds = data_dict[var]
        # drop any months that arent June, July, or August
        ds = ds.sel(time=ds["time"].dt.month.isin([6, 7, 8]))

        # For each variable, classify each daily value based on the summer fire danger rating classes in summer_fire_danger_ratings_dict
        var_classes = summer_fire_danger_ratings_dict[var].keys()

        for model in ds["model"].values:
            # use model names from the coverage metadata
            model_name_str = var_coverage_metadata[var]["model_encoding"][int(model)]
            var_summer_fire_summary[year_range_str][var][model_name_str] = {}

            for var_class in var_classes:
                # seed count at 0
                var_summer_fire_summary[year_range_str][var][model_name_str][
                    var_class
                ] = 0
                # get class bounds
                class_min, class_max = summer_fire_danger_ratings_dict[var][var_class]

                # filter by summer months and classify each value
                values = ds[var].sel(model=model).values
                # if there are more than 100 NA values, and the model is ERA5, return all NAs
                # this should catch the situation where the date range includes both historical and projected data
                if (
                    model_name_str == "era5"
                    and np.count_nonzero(np.isnan(values)) > 100
                ):
                    for var_class in var_classes:
                        var_summer_fire_summary[year_range_str][var][model_name_str][
                            var_class
                        ] = np.nan
                    break
                for value in values:
                    if class_min <= value < class_max:
                        var_summer_fire_summary[year_range_str][var][model_name_str][
                            var_class
                        ] += 1
            # compute average counts per year, rounded to nearest integer
            for var_class in var_classes:
                avg_count = (
                    var_summer_fire_summary[year_range_str][var][model_name_str][
                        var_class
                    ]
                    / num_years
                )
                var_summer_fire_summary[year_range_str][var][model_name_str][
                    var_class
                ] = (int(round(avg_count)) if not np.isnan(avg_count) else np.nan)

    return var_summer_fire_summary


#### FLASK ROUTES ####


@routes.route("/fire_weather/")
def fire_weather_about():
    return render_template("/documentation/fire_weather.html")


@routes.route("/fire_weather/point/<lat>/<lon>")
@routes.route("/fire_weather/point/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_fire_weather_point_data(lat, lon, start_year=None, end_year=None):
    """
    Query the daily fire weather coverage.
    GET parameters:
        vars: comma-separated list of variables to fetch (default: all variables)
            valid variables: bui, dmc, dc, ffmc, fwi, isi
        op: postprocessing operation to perform (required)
            valid operations: 3dayrollingavg, 5dayrollingavg, 7dayrollingavg
            only one operation can be performed at a time

    Args:
        lat (float): latitude
        lon (float): longitude
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested daily fire weather data

    Notes:
        example request (3 day rolling average for all variables, all years): http://localhost:5000/fire_weather/point/65.06/-146.16?op=3dayrollingavg
        example request (3 day rolling average for select variables, all years): http://localhost:5000/fire_weather/point/65.06/-146.16?vars=bui,fwi&op=3dayrollingavg
        example request (3 day rolling average for all variables, select years): http://localhost:5000/fire_weather/point/65.06/-146.16/2000/2030&op=3dayrollingavg
        example request (3 day rolling average for select variables, select years): http://localhost:5000/fire_weather/point/65.06/-146.16/2000/2030?vars=bui,fwi&op=3dayrollingavg

    """

    #### REQUEST VALIDATION ####

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

    # Validate postprocessing operation
    requested_ops = request.args.get("op")
    if requested_ops:
        requested_ops = requested_ops.split(",")
        if len(requested_ops) > 1:
            return (
                render_template("422/invalid_get_parameter.html"),
                422,
            )  # only one operation allowed
        for op in requested_ops:
            if op not in [
                "3dayrollingavg",
                "5dayrollingavg",
                "7dayrollingavg",
                "summer_fire_danger_rating_days",
            ]:
                return render_template("422/invalid_get_parameter.html"), 422
    else:
        requested_ops = ["3dayrollingavg"]  # default operation

    #### REQUEST DATA FETCHING ####

    # fetch the data
    fetched_data = asyncio.run(
        fetch_data_for_all_vars(requested_vars, float(lat), float(lon), times)
    )

    #### REQUEST POSTPROCESSING ####

    n = None
    if "3dayrollingavg" in requested_ops:
        n = 3
    elif "5dayrollingavg" in requested_ops:
        n = 5
    elif "7dayrollingavg" in requested_ops:
        n = 7

    if n is not None:
        processed_data = nday_rolling_avg(
            n,
            fetched_data,
            var_coverage_metadata,
            start_year,
            end_year,
        )
        return processed_data

    if "summer_fire_danger_rating_days" in requested_ops:
        processed_data = summer_fire_danger_rating_days(
            fetched_data,
            var_coverage_metadata,
            start_year,
            end_year,
        )
        return processed_data

    return render_template("400/bad_request.html"), 400  # an operation is required
