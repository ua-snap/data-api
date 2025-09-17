import asyncio

from flask import Blueprint, render_template, request


from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str, generate_netcdf_wcs_getcov_str
from fetch_data import (
    fetch_data,
    describe_via_wcps,
    fetch_bbox_netcdf,
    get_poly,
)
from zonal_stats import interpolate_and_compute_zonal_stats
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
    project_latlon,
    validate_xy_in_coverage_extent,
    generate_time_index_from_coverage_metadata,
    validate_var_id,
)
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv
from . import routes

era5wrf_api = Blueprint("era5wrf_api", __name__)

era5wrf_coverage_ids = {
    "t2_min": "era5_4km_daily_t2_min_wcs",
    "t2_mean": "era5_4km_daily_t2_mean_wcs",
    "t2_max": "era5_4km_daily_t2_max_wcs",
    "rh2_min": "era5_4km_daily_rh2_min_wcs",
    "rh2_mean": "era5_4km_daily_rh2_mean_wcs",
    "rh2_max": "era5_4km_daily_rh2_max_wcs",
    "wspd10_max": "era5_4km_daily_wspd10_max_wcs",
    "wspd10_mean": "era5_4km_daily_wspd10_mean_wcs",
    "wdir10_mean": "era5_4km_daily_wdir10_mean_wcs",
    "seaice_max": "era5_4km_daily_seaice_max_wcs",
    "rainnc_sum": "era5_4km_daily_rainnc_sum_wcs",
}

era5wrf_meta = {
    key: asyncio.run(describe_via_wcps(cov_id))
    for key, cov_id in era5wrf_coverage_ids.items()
}


def package_era5wrf_point_data(data_dict, coverage_meta):
    """Package ERA5-WRF data with time-first structure.

    Args:
        data_dict (dict): Variable names mapped to data arrays from fetch
        coverage_meta (dict): Coverage metadata containing time axis

    Returns:
        dict: Time-first structured data {date: {variable: value}}
    """

    time_index = generate_time_index_from_coverage_metadata(coverage_meta)

    # package data with time keys at top level
    packaged_data = {}
    for i, timestamp in enumerate(time_index):
        date_key = timestamp.strftime("%Y-%m-%d")
        packaged_data[date_key] = {}

        # add each variable's value for this date
        for variable, values in data_dict.items():
            if i < len(values) and values[i] is not None:
                # round to 1 decimal good for current variable set, might need to change later
                packaged_data[date_key][variable] = round(values[i], 1)

    return packaged_data


@routes.route("/era5wrf/")
@routes.route("/era5wrf/abstract/")
@routes.route("/era5wrf/point/")
def era5wrf_about():
    return render_template("documentation/era5wrf.html")


async def fetch_era5_wrf_point_data(x, y, variables):
    """Fetch ERA5-WRF data for multiple variables asynchronously.

    Args:
        x (float): x-coordinate
        y (float): y-coordinate
        variables (list): List of variable names to fetch

    Returns:
        dict: Variable names mapped to their data arrays
    """
    tasks = []
    for var_name in variables:
        cov_id = era5wrf_coverage_ids[var_name]

        request_str = generate_wcs_getcov_str(x, y, cov_id)
        url = generate_wcs_query_url(request_str)
        tasks.append(fetch_data([url]))

    results = await asyncio.gather(*tasks)
    return {var_name: data for var_name, data in zip(variables, results)}


async def fetch_era5_wrf_area_data(polygon, variables):
    """Fetch ERA5-WRF bbox data for multiple variables.

    Args:
        polygon (GeoDataFrame): Polygon for which to compute zonal statistics
        variables (list): List of variable names to fetch

    Returns:
        dict: Variable names mapped to xarray datasets
    """
    bbox_bounds = polygon.total_bounds  # (xmin, ymin, xmax, ymax)

    tasks = []
    for var_name in variables:
        cov_id = era5wrf_coverage_ids[var_name]

        request_str = generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id)

        url = generate_wcs_query_url(request_str)
        tasks.append(fetch_bbox_netcdf([url]))

    datasets = await asyncio.gather(*tasks)
    return {var_name: ds for var_name, ds in zip(variables, datasets)}


def process_era5wrf_zonal_stats(polygon, datasets_dict, variables):
    """Process zonal statistics for ERA5-WRF datasets.

    Args:
        polygon (GeoDataFrame): Target polygon
        datasets_dict (dict): Variable names mapped to xarray datasets
        variables (list): List of variable names

    Returns:
        dict: Variable names mapped to their zonal statistics arrays (time series)
    """
    zonal_results = {}

    for var_name in variables:
        ds = datasets_dict[var_name]

        time_series_means = []

        # zonal stats computed per time slice
        for time_idx in range(ds.sizes["time"]):

            time_slice_ds = ds.isel(time=time_idx)
            zonal_stats_dict = interpolate_and_compute_zonal_stats(
                polygon,
                time_slice_ds,
                crs="EPSG:3338",
                var_name=var_name,
                x_dim="X",
                y_dim="Y",
            )

            time_series_means.append(zonal_stats_dict["mean"])

        zonal_results[var_name] = time_series_means

    return zonal_results


def package_era5wrf_area_data(zonal_results, coverage_meta, variables):
    """Package ERA5-WRF area data with time-first structure.

    Args:
        zonal_results (dict): Variable names mapped to zonal statistics
        coverage_meta (dict): Coverage metadata containing time axis
        variables (list): List of variable names

    Returns:
        dict: Time-first structured data {date: {variable: zonal_mean}}
    """
    time_index = generate_time_index_from_coverage_metadata(coverage_meta)

    # package data with time keys at top level, same as point query
    packaged_data = {}
    for i, timestamp in enumerate(time_index):
        date_key = timestamp.strftime("%Y-%m-%d")
        packaged_data[date_key] = {}

        # add each variable's zonal mean for this date
        for variable in variables:
            if (
                i < len(zonal_results[variable])
                and zonal_results[variable][i] is not None
            ):
                # round to 1 decimal for consistency with point queries
                packaged_data[date_key][variable] = round(zonal_results[variable][i], 1)

    return packaged_data


@routes.route("/era5wrf/point/<lat>/<lon>")
def era5wrf_point(lat, lon):
    """ERA5-WRF point data endpoint.
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        JSON-like object of ERA5-WRF data
    """
    # extract query parameters for variables
    requested_vars = request.args.get("vars")
    # handle variable selection
    if requested_vars:
        variables = requested_vars.split(",")
        for var in variables:
            if var not in era5wrf_coverage_ids:
                # if user asks for a variable that doesn't exist, bad request
                return render_template("400/bad_request.html"), 400
    else:
        # if no variables are requested, use all variables
        variables = list(era5wrf_coverage_ids.keys())

    # validate coordinates
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400

    # construct bbox and validate coordinates are within it
    era5wrf_bbox = construct_latlon_bbox_from_coverage_bounds(
        era5wrf_meta["t2_mean"]  # any coverage, they all have the same bounds
    )
    within_bounds = validate_latlon_in_bboxes(
        lat,
        lon,
        [era5wrf_bbox],
        ["era5_4km"],  # name of the geotiff we check for data vs. nodata extent
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

    # but check projected space also for edge case
    if not validate_xy_in_coverage_extent(
        x, y, era5wrf_meta["t2_mean"], east_tolerance=2000, north_tolerance=2000
    ):
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html", bboxes=[era5wrf_bbox]
            ),
            422,
        )

    try:
        all_data = asyncio.run(fetch_era5_wrf_point_data(x, y, variables))

        reference_meta = era5wrf_meta[
            "t2_mean"
        ]  # any coverage metadata for time axis, they are all the same
        packaged_data = package_era5wrf_point_data(all_data, reference_meta)
        postprocessed = prune_nulls_with_max_intensity(
            postprocess(packaged_data, "era5wrf_4km")
        )

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(
                postprocessed, "era5wrf_4km", place_id=place_id, lat=lat, lon=lon
            )

        return postprocessed

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/era5wrf/area/<place_id>")
def era5wrf_area(place_id):
    """ERA5-WRF area data endpoint.

    Args:
        place_id (str): ID for polygon from places endpoint

    Query Parameters:
        vars (str): Comma-separated variable names
        format (str): Output format ('csv' or default JSON)

    Returns:
        JSON-like object of ERA5-WRF area-aggregated data
    """
    poly_type = validate_var_id(place_id)

    if type(poly_type) is tuple:
        return poly_type

    try:
        polygon = get_poly(place_id, crs=3338)
    except:
        return render_template("422/invalid_area.html"), 422

    # extract and validate query parameters (mirror point query logic)
    requested_vars = request.args.get("vars")

    # handle variable selection, no vars means all variables
    if requested_vars:
        variables = requested_vars.split(",")
        for var in variables:
            if var not in era5wrf_coverage_ids:
                return render_template("400/bad_request.html"), 400
    else:
        variables = list(era5wrf_coverage_ids.keys())

    try:
        # fetch bbox datasets for requested variables
        datasets_dict = asyncio.run(fetch_era5_wrf_area_data(polygon, variables))
        zonal_results = process_era5wrf_zonal_stats(polygon, datasets_dict, variables)
        reference_meta = era5wrf_meta[
            "t2_mean"
        ]  # any coverage, they all have the same time spans
        packaged_data = package_era5wrf_area_data(
            zonal_results, reference_meta, variables
        )

        postprocessed = prune_nulls_with_max_intensity(
            postprocess(packaged_data, "era5wrf_4km")
        )

        if request.args.get("format") == "csv":
            return create_csv(postprocessed, "era5wrf_4km", place_id=place_id)

        return postprocessed

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
