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

from luts import (
    cmip6_downscaled_options,
    all_cmip6_downscaled_vars,
    all_cmip6_downscaled_models,
    all_cmip6_downscaled_scenarios,
)

from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("cmip6_downscaled_api", __name__)


async def get_cmip6_metadata(cov_id):
    """Get the coverage metadata and encodings for CMIP6 downscaled daily coverage"""
    metadata = await describe_via_wcps(cov_id)
    return metadata


async def fetch_cmip6_downscaled_point_data(cov_id, x, y):
    """
    Make an async request for CMIP6 downscaled daily data for provided coverage at a specified point

    Args:
        cov_id (str): coverage ID
        x (float): x-coordinate
        y (float): y-coordinate

    Returns:
        list of data results at a specified point
    """

    wcs_str = generate_wcs_getcov_str(x, y, cov_id=cov_id)

    # Generate the URL for the WCS query
    url = generate_wcs_query_url(wcs_str)

    # Fetch the data
    point_data_list = await fetch_data([url])

    return point_data_list


def package_cmip6_downscaled_data(metadata, point_data_list):
    """
    Package CMIP6 downscaled daily values into human-readable JSON format

    Args:
        metadata (dict): coverage metadata
        point_data_list (list): list of data from Rasdaman WCPS query

    Returns:
        di (dict): time series dictionary of date/value pairs
    """
    di = dict()

    time_series = generate_time_index_from_coverage_metadata(metadata)

    for var_coord, value in enumerate(point_data_list):
        time = time_series[var_coord].date().strftime("%Y-%m-%d")

        if value is None:
            di[time] = None
        else:
            di[time] = round(float(value))

    return di


@routes.route("/cmip6_downscaled/")
def cmip6_downscaled_about():
    return render_template("/documentation/cmip6_downscaled.html")


@routes.route("/cmip6_downscaled/point/<lat>/<lon>")
def cmip6_downscaled_point(lat, lon):
    """
    Fetch CMIP6 downscaled daily data for at a specified point for each variable/model/scenario,
    then combine them all into a single nested dictionary.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        dict: time series data for the specified point for provided variables/models/scenarios

    Notes:
        example request (all variables): /cmip6_downscaled/point/61.5/-147
        example request (specific variable): /cmip6_downscaled/point/61.5/-147?vars=tasmax
        example request (specific model): /cmip6_downscaled/point/61.5/-147?models=6ModelAvg
        example request (specific scenario): /cmip6_downscaled/point/61.5/-147?scenarios=ssp585
    """

    # Split and assign optional HTTP GET parameters.
    if request.args.get("vars"):
        vars = request.args.get("vars").split(",")
        if not all(var in all_cmip6_downscaled_vars for var in vars):
            return render_template("400/bad_request.html"), 400
        logger.debug(f"Results limited to vars: {vars}")
    else:
        vars = all_cmip6_downscaled_vars

    if request.args.get("models"):
        models = request.args.get("models").split(",")
        if not all(model in all_cmip6_downscaled_models for model in models):
            return render_template("400/bad_request.html"), 400
        logger.debug(f"Results limited to models: {models}")
    else:
        models = all_cmip6_downscaled_models

    if request.args.get("scenarios"):
        scenarios = request.args.get("scenarios").split(",")
        if not all(
            scenario in all_cmip6_downscaled_scenarios for scenario in scenarios
        ):
            return render_template("400/bad_request.html"), 400
        logger.debug(f"Results limited to scenarios: {scenarios}")
    else:
        scenarios = all_cmip6_downscaled_scenarios

    try:
        results = fetch_all_requested_combos(lat, lon, vars, models, scenarios)
        if isinstance(results, tuple):
            return results
        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(
                results,
                "cmip6_downscaled",
                place_id,
                lat,
                lon,
                vars=vars,
            )
    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    return results


def fetch_all_requested_combos(lat, lon, vars, models, scenarios):
    """
    Query each variable/model/scenario coverage individually and combine them all into a nested dictionary.

    Args:
        lat (float): latitude
        lon (float): longitude
        vars (list): list of variable names
        models (list): list of model names
        scenarios (list): list of scenario names

    Returns:
        dict: combined time series data for the specified point
    """
    results = {}
    for varname in vars:
        for model in models:
            for scenario in scenarios:
                # Return immediately if an exception is encountered for any coverage.
                # All coverages share the same BBOX and structure, so an exception for one
                # can be assumed to be an exception for all.
                result = run_fetch_cmip6_downscaled_point_data(
                    lat, lon, varname, model, scenario
                )
                if isinstance(result, tuple) and result[1] in [400, 404, 422]:
                    return result
                else:
                    if model not in results:
                        results[model] = {}
                    if scenario not in results[model]:
                        results[model][scenario] = {}
                    for time, value in result.items():
                        if time not in results[model][scenario]:
                            results[model][scenario][time] = {}
                        results[model][scenario][time][varname] = value

    results = prune_nulls_with_max_intensity(postprocess(results, "cmip6_downscaled"))
    return results


def run_fetch_cmip6_downscaled_point_data(lat, lon, varname, model, scenario):
    """
    Fetch CMIP6 downscaled daily data for a single variable/model/scenario combo at a specified point.

    Args:
        lat (float): latitude
        lon (float): longitude
        varname (str): variable name
        model (str): model name
        scenario (str): scenario name

    Returns:
        dict: time series data for the specified point for a single variable/model/scenario combo
    """

    # If we have made it this far, the model and scenario are valid.
    # If they are not found for the variable, return empty results.
    if model not in cmip6_downscaled_options[varname]:
        return {}
    if scenario not in cmip6_downscaled_options[varname][model]:
        return {}

    # Validate the lat/lon values
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400

    cov_id = f"cmip6_downscaled_{varname}_{model}_{scenario}_wcs"
    cov_id = cov_id.replace("-", "_")
    metadata = asyncio.run(get_cmip6_metadata(cov_id))
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

    x, y = project_latlon(lat, lon, 3338)
    point_data_list = asyncio.run(fetch_cmip6_downscaled_point_data(cov_id, x, y))
    results = package_cmip6_downscaled_data(metadata, point_data_list)
    return results
