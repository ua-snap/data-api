import asyncio
import pandas as pd
from datetime import datetime
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps, fetch_wcs_point_data
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
    project_latlon,
    validate_xy_in_coverage_extent,
    generate_time_index_from_coverage_metadata,
)
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv
from . import routes


def format_era5_datetime(date_obj):
    """Convert datetime object to ISO format.

    CP note: good candidate to move to a utility module for time slicing and time validation

    Args:
        date_obj (datetime): Python datetime object

    Returns:
        str: ISO formatted datetime string
    """
    if date_obj is None:
        return None
    return f"{date_obj.strftime('%Y-%m-%d')}T00:00:00.000Z"


def create_era5_time_slice(start_date, end_date):
    """Create time_slice tuple for ERA5-WRF WCS requests.

    CP note: good candidate to move to a utility module for time slicing and time validation
    This is a small function and could be bundled but it helps me debug

    Args:
        start_date (datetime): Start date for temporal slice
        end_date (datetime): End date for temporal slice

    Returns:
        tuple: (time_axis, time_range_string) for WCS request, or None if no dates
    """
    if start_date is None or end_date is None:
        return None

    start_iso = format_era5_datetime(start_date)
    end_iso = format_era5_datetime(end_date)
    return ("time", f'"{start_iso}","{end_iso}"')


def get_era5_temporal_bounds(coverage_meta):
    """Extract temporal bounds from ERA5-WRF coverage metadata.

    Args:
        coverage_meta (dict): Coverage metadata from describe_via_wcps()

    Returns:
        tuple: (min_date, max_date) as datetime objects
    """
    try:
        # Find the time axis in coverage metadata
        # CP note: good candidate to move to a function in validate_request
        # much like the function that determines the spatial axes
        time_axis = next(
            axis
            for axis in coverage_meta["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"]
            == "time"  # ERA5-WRF uses "time" axis, could also be "ansi"
        )

        # Extract coordinate list and get bounds
        time_coordinates = time_axis["coordinate"]

        # Convert to datetime objects
        min_date = pd.to_datetime(time_coordinates[0])
        max_date = pd.to_datetime(time_coordinates[-1])

        # Remove timezone info to make naive datetimes
        min_date_naive = (
            min_date.tz_localize(None) if min_date.tz is not None else min_date
        )
        max_date_naive = (
            max_date.tz_localize(None) if max_date.tz is not None else max_date
        )

        return min_date_naive.to_pydatetime(), max_date_naive.to_pydatetime()

    except (KeyError, StopIteration, IndexError):
        raise ValueError(
            "Could not extract temporal bounds from ERA5-WRF coverage metadata"
        )


def validate_era5_date_range(start_date, end_date, coverage_meta):
    """Validate date range against actual ERA5-WRF coverage bounds.

    Args:
        start_date (datetime): User-provided start date
        end_date (datetime): User-provided end date
        coverage_meta (dict): ERA5-WRF coverage metadata

    Returns:
        True if valid, or Flask response tuple for errors
    """
    try:
        # Get actual temporal bounds from coverage
        min_date, max_date = get_era5_temporal_bounds(coverage_meta)

        # Validation checks
        if start_date > end_date:
            return (
                render_template(
                    "422/invalid_date_range.html",
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                ),
                422,
            )

        # Check bounds against actual coverage dates
        if start_date < min_date or end_date > max_date:
            return (
                render_template(
                    "422/invalid_year.html",
                    start_year=start_date.year,
                    end_year=end_date.year,
                    min_year=min_date.year,
                    max_year=max_date.year,
                ),
                422,
            )

        return True

    except ValueError as e:
        # in trouble if we can't parse metadata
        return render_template("500/server_error.html"), 500


def package_era5wrf_with_time(data_dict, coverage_meta, start_date=None, end_date=None):
    """Package ERA5-WRF data with time-first structure.

    Args:
        data_dict (dict): Variable names mapped to data arrays from fetch
        coverage_meta (dict): Coverage metadata containing time axis
        start_date (datetime, optional): Start date for filtering
        end_date (datetime, optional): End date for filtering

    Returns:
        dict: Time-first structured data {date: {variable: value}}
    """

    time_index = generate_time_index_from_coverage_metadata(coverage_meta)

    # Filter time index if date range provided
    if start_date and end_date:
        mask = (time_index.date >= start_date.date()) & (
            time_index.date <= end_date.date()
        )
        time_index = time_index[mask]

    # Package data with time keys
    packaged_data = {}
    for i, timestamp in enumerate(time_index):
        date_key = timestamp.strftime("%Y-%m-%d")
        packaged_data[date_key] = {}

        # Add each variable's value for this date
        for variable, values in data_dict.items():
            if i < len(values) and values[i] is not None:
                # round to 1 decimal good for current variable set, might need to change later
                packaged_data[date_key][variable] = round(values[i], 1)

    return packaged_data


era5wrf_api = Blueprint("era5wrf_api", __name__)

era5wrf_monthly_coverage_ids = {
    "t2_min": "era5_4km_daily_t2_min",
    "t2_mean": "era5_4km_daily_t2_mean",
    "t2_max": "era5_4km_daily_t2_max",
    "rh2_min": "era5_4km_daily_rh2_min",
    "rh2_mean": "era5_4km_daily_rh2_mean",
    "rh2_max": "era5_4km_daily_rh2_max",
    "wspd10_max": "era5_4km_daily_wspd10_max",
    "wspd10_mean": "era5_4km_daily_wspd10_mean",
    "wdir10_mean": "era5_4km_daily_wdir10_mean",
    # "seaice_max": "era5_4km_daily_seaice_max",
}

era5wrf_meta = {
    key: asyncio.run(describe_via_wcps(cov_id))
    for key, cov_id in era5wrf_monthly_coverage_ids.items()
}


@routes.route("/era5wrf/")
@routes.route("/era5wrf/abstract/")
@routes.route("/era5wrf/point/")
def era5wrf_about():
    return render_template("documentation/era5wrf.html")


async def fetch_era5_wrf_point_data(x, y, variables, start_date=None, end_date=None):
    """Fetch ERA5-WRF data for multiple variables asynchronously.

    Args:
        x (float): x-coordinate
        y (float): y-coordinate
        variables (list): List of variable names to fetch
        start_date (datetime, optional): Start date for temporal slice
        end_date (datetime, optional): End date for temporal slice

    Returns:
        dict: Variable names mapped to their data arrays
    """
    # Create time slice if dates are provided
    time_slice = create_era5_time_slice(start_date, end_date)

    tasks = []
    for var_name in variables:
        cov_id = era5wrf_monthly_coverage_ids[var_name]

        if time_slice:
            # Use direct WCS call with temporal slicing
            request_str = generate_wcs_getcov_str(x, y, cov_id, time_slice=time_slice)
            url = generate_wcs_query_url(request_str)
            tasks.append(fetch_data([url]))
        else:
            # Use existing helper function for full temporal range
            tasks.append(fetch_wcs_point_data(x, y, cov_id))

    results = await asyncio.gather(*tasks)
    return {var_name: data for var_name, data in zip(variables, results)}


@routes.route("/era5wrf/point/<lat>/<lon>")
def era5wrf_point(lat, lon):
    """ERA5-WRF point data endpoint.
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        JSON-like object of ERA5-WRF data
    """
    # extract query parameters for variables and start/end dates
    requested_vars = request.args.get("vars")
    requested_start_date = request.args.get("start_date")
    requested_end_date = request.args.get("end_date")
    # handle variable selection
    if requested_vars:
        variables = requested_vars.split(",")
        for var in variables:
            if var not in era5wrf_monthly_coverage_ids:
                # if user asks for a variable that doesn't exist, bad request
                return render_template("400/bad_request.html"), 400
    else:
        # if no variables are requested, use all variables
        variables = list(era5wrf_monthly_coverage_ids.keys())
    # handle start/end date selection
    if requested_start_date and requested_end_date:
        try:
            start_date = datetime.strptime(requested_start_date, "%Y-%m-%d")
            end_date = datetime.strptime(requested_end_date, "%Y-%m-%d")
        except ValueError:
            return render_template("400/bad_request.html"), 400

        # Validate dates against coverage metadata
        reference_meta = era5wrf_meta["t2_mean"]  # Use any coverage metadata
        validation_result = validate_era5_date_range(
            start_date, end_date, reference_meta
        )
        if validation_result != True:
            return validation_result  # Return error response
    else:
        start_date = None
        end_date = None

    # validate coordinates
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400

    # Construct bbox and validate coordinates are within coverage
    era5wrf_bbox = construct_latlon_bbox_from_coverage_bounds(
        era5wrf_meta["t2_mean"]  # any coverage will do
    )
    within_bounds = validate_latlon_in_bboxes(
        lat,
        lon,
        [era5wrf_bbox],
        ["era5_4km_daily_t2_mean"],  # using reference coverage
    )
    if within_bounds == 404:
        return render_template("404/no_data.html"), 404
    if within_bounds == 422:
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html",
                bboxes=[era5wrf_bbox],
            ),
            422,
        )
    # if we get to here, we are pretty sure we have a valid request

    x, y = project_latlon(lat, lon, 3338)

    # Validate projected coordinates are within coverage extent
    if not validate_xy_in_coverage_extent(
        x, y, era5wrf_meta["t2_mean"], east_tolerance=2000, north_tolerance=2000
    ):
        return render_template("500/server_error.html"), 500

    try:
        all_data = asyncio.run(
            fetch_era5_wrf_point_data(x, y, variables, start_date, end_date)
        )

        # Package data with time information
        reference_meta = era5wrf_meta[
            "t2_mean"
        ]  # Use any coverage metadata for time axis
        packaged_data = package_era5wrf_with_time(
            all_data, reference_meta, start_date, end_date
        )
        postprocessed = prune_nulls_with_max_intensity(
            postprocess(packaged_data, "era5wrf_4km")
        )

        if request.args.get("format") == "csv":
            return create_csv(postprocessed, "era5wrf_4km", lat=lat, lon=lon)

        return postprocessed

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
