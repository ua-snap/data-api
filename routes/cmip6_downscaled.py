import asyncio
import logging
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
    project_latlon,
    generate_time_index_from_coverage_metadata,
)
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv

from luts import cmip6_downscaled_options

from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("cmip6_api", __name__)


async def get_cmip6_metadata(cov_id):
    """Get the coverage metadata and encodings for CMIP6 monthly coverage"""
    metadata = await describe_via_wcps(cov_id)
    return metadata


async def fetch_cmip6_downscaled_point_data(
    cov_id, x, y, var_coord=None, time_slice=None
):
    """
    Make an async request for CMIP6 monthly data for a range of models, scenarios, and years at a specified point

    Args:
        lat (float): latitude
        lon (float): longitude
        var_coord (int): variable coordinate from dim_encoding, if specified
        time_slice (str): time slice for the data request, if specified

    Returns:
        list of data results from each of historical and future data at a specified point
    """

    # We must use EPSG:4326 for the CMIP6 monthly coverage to match the coverage projection
    wcs_str = generate_wcs_getcov_str(
        x,
        y,
        cov_id=cov_id,
        var_coord=var_coord,
        time_slice=time_slice,
    )

    # Generate the URL for the WCS query
    url = generate_wcs_query_url(wcs_str)

    # Fetch the data
    point_data_list = await fetch_data([url])

    return point_data_list


def package_cmip6_downscaled_data(
    metadata, point_data_list, var_id=None, start_year=None, end_year=None
):
    """
    Package the CMIP6 monthly values into human-readable JSON format

    Args:
        point_data_list (list): nested list of data from Rasdaman WCPS query
        var_id (str): variable name, if specified
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from dim_encodings global variable
    """
    di = dict()

    # Nest point_data_list one level deeper if var_id is specified.
    # This keeps the nesting level the same for all cases.
    if var_id != None:
        point_data_list = [point_data_list]

    time_series = generate_time_index_from_coverage_metadata(metadata)

    for var_coord, value in enumerate(point_data_list):
        time = time_series[var_coord].date().strftime("%Y-%m-%d")
        di[time] = dict()

        # replace NaN values (None) with -9999
        if value is None:
            value = -9999

        di[time] = round(float(value))

    return di


@routes.route("/cmip6_downscaled/point/<lat>/<lon>")
def cmip6_downscaled_point(lat, lon):
    if request.args.get("vars"):
        vars = request.args.get("vars").split(",")
        varname = vars
    else:
        vars = list(cmip6_downscaled_options.keys())

    if request.args.get("models"):
        models = request.args.get("models").split(",")
    else:
        models = list(cmip6_downscaled_options["pr"].keys())

    if request.args.get("scenarios"):
        scenarios = request.args.get("scenarios").split(",")
    else:
        scenarios = cmip6_downscaled_options["pr"][models[0]]

    results = {}
    for varname in vars:
        for model in models:
            for scenario in scenarios:
                result = run_fetch_cmip6_downscaled_point_data(
                    lat, lon, varname, model, scenario
                )
                # Since all coverages share the same BBOX and GeoTIFF mask, if
                # any coverage returns any of these error codes, return immediately.
                if isinstance(result, tuple) and result[1] in [400, 404, 422]:
                    return result
                else:
                    if model not in results:
                        results[model] = {}
                    if scenario not in results[model]:
                        results[model][scenario] = {}
                    if varname not in results[model][scenario]:
                        results[model][scenario][varname] = {}
                    results[model][scenario][varname] = result

    results = prune_nulls_with_max_intensity(postprocess(results, "cmip6_downscaled"))

    logger.debug(f"Results limited to {vars}")

    if request.args.get("format") == "csv":
        place_id = request.args.get("community")
        return create_csv(
            results,
            "cmip6_downscaled",
            place_id,
            lat,
            lon,
        )

    return results


def run_fetch_cmip6_downscaled_point_data(lat, lon, varname, model, scenario):
    try:
        if scenario in cmip6_downscaled_options[varname][model]:
            cov_id = f"cmip6_downscaled_{varname}_{model}_{scenario}_crstephenson"
        else:
            return render_template("400/bad_request.html"), 400
    except:
        return render_template("400/bad_request.html"), 400

    cov_id = cov_id.replace("-", "_")

    metadata = asyncio.run(get_cmip6_metadata(cov_id))

    # Validate the lat/lon values
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    cmip6_downscaled_bbox = construct_latlon_bbox_from_coverage_bounds(metadata)
    within_bounds = validate_latlon_in_bboxes(
        lat, lon, [cmip6_downscaled_bbox], [cov_id]
    )
    if within_bounds == 404:
        return (
            render_template("404/no_data.html"),
            404,
        )
    if within_bounds == 422:
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html",
                bboxes=[cmip6_downscaled_bbox],
            ),
            422,
        )

    # try:

    x, y = project_latlon(lat, lon, 3338)

    point_data_list = asyncio.run(fetch_cmip6_downscaled_point_data(cov_id, x, y))
    results = package_cmip6_downscaled_data(metadata, point_data_list)

    return results

    # except ValueError:
    #     return render_template("400/bad_request.html"), 400
    # except Exception as exc:
    #     if hasattr(exc, "status") and exc.status == 404:
    #         return render_template("404/no_data.html"), 404
    #     return render_template("500/server_error.html"), 500
