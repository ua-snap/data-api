"""Endpoints for climate indicators

These endpoint(s) query a coverage containing summarized versions of the indicators dataset created from the 12km NCAR dataset.
The thresholds and eras are preconfigured in the coverage. Calling this the "base" indicators for now (i.e., url suffix: /indicators/base).
"""

import json
import asyncio
import numpy as np
import itertools
from math import floor, isnan
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str, generate_netcdf_wcs_getcov_str
from fetch_data import (
    fetch_data,
    fetch_bbox_netcdf_list,
    get_poly,
    generate_nested_dict,
    describe_via_wcps,
    get_all_possible_dimension_combinations,
)
from zonal_stats import interpolate_and_compute_zonal_stats
from validate_request import (
    validate_latlon,
    latlon_is_numeric_and_in_geodetic_range,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
    project_latlon,
    validate_var_id,
    get_coverage_encodings,
    get_coverage_crs_str,
)
from postprocessing import (
    nullify_and_prune,
    postprocess,
    prune_nulls_with_max_intensity,
)
from csv_functions import create_csv
from . import routes
from config import WEST_BBOX, EAST_BBOX

indicators_api = Blueprint("indicators_api", __name__)

var_ep_lu = {
    "cmip5_indicators": {
        "cov_id_str": "ncar12km_indicators_era_summaries",
        "dim_encodings": None,  # populated below
        "bandnames": ["Gray"],
        "label": None,
        "crs": None,
    },
    "cmip6_indicators": {
        "cov_id_str": "cmip6_indicators",
        "dim_encodings": None,  # populated below
        "bandnames": ["dw", "ftc", "rx1day", "su"],
        "label": None,
        "crs": None,
    },
}


async def get_indicators_metadata():
    """Get the coverage metadata and encodings for NCAR 12km indicators coverage"""
    cmip5_metadata = await describe_via_wcps(
        var_ep_lu["cmip5_indicators"]["cov_id_str"]
    )
    cmip6_metadata = await describe_via_wcps(
        var_ep_lu["cmip6_indicators"]["cov_id_str"]
    )
    var_ep_lu["cmip5_indicators"]["dim_encodings"] = get_coverage_encodings(
        cmip5_metadata
    )
    var_ep_lu["cmip6_indicators"]["dim_encodings"] = get_coverage_encodings(
        cmip6_metadata
    )
    var_ep_lu["cmip5_indicators"]["crs"] = get_coverage_crs_str(cmip5_metadata)
    var_ep_lu["cmip6_indicators"]["crs"] = get_coverage_crs_str(cmip6_metadata)

    return var_ep_lu, cmip5_metadata, cmip6_metadata


var_ep_lu, cmip5_metadata, cmip6_metadata = asyncio.run(get_indicators_metadata())


# define eras used in cmip6 mmm summary operation
cmip6_eras = {
    "historical": {
        "start": 1950,
        "end": 2009,
    },
    "midcentury": {
        "start": 2040,
        "end": 2069,
    },
    "longterm": {
        "start": 2070,
        "end": 2099,
    },
}


async def fetch_indicators_point_data(lat, lon, cov_id_str, proj_str):
    """Make an async request for indicator data for a range of models, scenarios, and years at a specified point
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        list of data results from each of historical and future data at a specified point
    """

    wcs_str = generate_wcs_getcov_str(lon, lat, cov_id=cov_id_str, projection=proj_str)
    url = generate_wcs_query_url(wcs_str)
    point_data_list = await fetch_data([url])

    return point_data_list


async def fetch_indicators_bbox_data(bbox_bounds, cov_id_str):
    """Make the async request for the data at the specified point for a specific coverage

    Args:
        bbox_bounds (tuple): 4-tuple of x,y lower/upper bounds: (<xmin>,<ymin>,<xmax>,<ymax>)
        cov_id_str (str): shared portion of coverage_ids to query
    Returns:
        bbox_ds (xarray.DataSet): xarray dataset with the data for the bbox
    """
    # set up WCS request strings
    request_strs = []
    request_strs.append(generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id_str))
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    bbox_ds_list = await fetch_bbox_netcdf_list(urls)
    bbox_ds = bbox_ds_list[
        0
    ]  # there is only ever one dataset in the list for this endpoint
    return bbox_ds


def package_cmip5_point_data(rasdaman_response):
    # using the dimension names and dim_encodings, create the nested dict to hold results
    dim_encodings = var_ep_lu["cmip5_indicators"]["dim_encodings"]
    # we could get dimension names directly from the encodings, but they would be in the wrong order .... so we define explicitly here
    dimnames = [
        "indicator",
        "era",
        "model",
        "scenario",
        "stat",
    ]
    iter_coords = list(
        itertools.product(*[dim_encodings[dim].keys() for dim in dimnames])
    )
    dim_combos = get_all_possible_dimension_combinations(
        iter_coords, dimnames, dim_encodings
    )
    results = generate_nested_dict(dim_combos)

    # populate the results dict with the fetched data
    # using the coords to index into the rasdaman response
    for coords, dim_combo in zip(iter_coords, dim_combos):

        # check for impossible combinations of model and scenario, leaving these unpopulated (will be pruned)
        if dim_combo[1] == "historical" and dim_combo[2] != "Daymet":
            continue
        if dim_combo[1] != "historical" and dim_combo[2] == "Daymet":
            continue
        if dim_combo[2] == "Daymet" and dim_combo[3] != "historical":
            continue
        if dim_combo[2] != "Daymet" and dim_combo[3] == "historical":
            continue

        stat_value = rasdaman_response[coords[0]][coords[1]][coords[2]][coords[3]][
            coords[4]
        ]
        if isnan(stat_value):
            stat_value = -9999

        # round the values for certain indicators
        stat_value = (
            stat_value
            if (
                dim_combo[0] == "hd"
                or dim_combo[0] == "cd"
                or dim_combo[0] == "rx1day"
                or dim_combo[0] == "rx5day"
            )
            else floor(stat_value)
        )

        results[dim_combo[0]][dim_combo[1]][dim_combo[2]][dim_combo[3]][
            dim_combo[4]
        ] = stat_value

    results = nullify_and_prune(results, "ncar12km_indicators")
    results = prune_nulls_with_max_intensity(results)
    return results


def run_aggregate_var_polygon(poly_id, var_ep):
    """Get data summary (e.g. zonal mean) of single variable in polygon. Fetches data on
    the individual instances of the singular dimension combinations.

    Args:
        var_ep (str): variable endpoint (one of "cmip5_indicators" or "cmip6_indicators")
        poly_id (str or int): the unique `id` used to identify the Polygon
            for which to compute the zonal mean.
    Returns:
        aggr_results (dict): data representing zonal stats within the polygon.
    NOTE: "cmip6_indicators" is not yet implemented. That coverage uses "lat" and "lon" dimensions
            and has multiple band names...will require a different approach!
    """
    polygon = get_poly(poly_id)
    cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
    bandname = var_ep_lu[var_ep]["bandnames"][0]
    crs = var_ep_lu[var_ep]["crs"]

    ds = asyncio.run(fetch_indicators_bbox_data(polygon.total_bounds, cov_id_str))

    # get all combinations of non-XY dimensions in the dataset and their corresponding encodings
    # and create a dict to hold the results for each combo
    all_dims = ds[bandname].dims
    dimnames = [dim for dim in all_dims if dim not in ["X", "Y"]]
    dim_encodings = var_ep_lu[var_ep]["dim_encodings"]
    iter_coords = list(itertools.product(*[list(ds[dim].values) for dim in dimnames]))
    dim_combos = get_all_possible_dimension_combinations(
        iter_coords, dimnames, dim_encodings
    )
    aggr_results = generate_nested_dict(dim_combos)

    # fetch the dim combo from the dataset and calculate zonal stats, adding to the results dict
    for coords, dim_combo in zip(iter_coords, dim_combos):

        # check for impossible combinations of model and scenario, leaving these unpopulated (will be pruned)
        if dim_combo[1] == "historical" and dim_combo[2] != "Daymet":
            continue
        if dim_combo[1] != "historical" and dim_combo[2] == "Daymet":
            continue
        if dim_combo[2] == "Daymet" and dim_combo[3] != "historical":
            continue
        if dim_combo[2] != "Daymet" and dim_combo[3] == "historical":
            continue

        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        combo_ds = ds.sel(sel_di)
        combo_zonal_stats_dict = interpolate_and_compute_zonal_stats(
            polygon, combo_ds, crs
        )

        # determine the result based on the requested statistic (min, mean, max)
        # the string values for the `combo_zonal_stats_dict` will need to be updated to "min" or "max" for those summaries
        # if or when we implement a different kind method of extrema aggregation, see issue 560
        if dim_combo[4] == "min":
            result = floor(combo_zonal_stats_dict["mean"])
        elif dim_combo[4] == "mean":
            result = round(combo_zonal_stats_dict["mean"], 1)
        elif dim_combo[4] == "max":
            result = np.ceil(combo_zonal_stats_dict["mean"])

        # use the dim_combo to index into the results dict (indicator, era, model, scenario, stat)
        aggr_results[dim_combo[0]][dim_combo[1]][dim_combo[2]][dim_combo[3]][
            dim_combo[4]
        ] = result

    # remove null values from the results dict
    aggr_results = nullify_and_prune(aggr_results, "ncar12km_indicators")
    aggr_results = prune_nulls_with_max_intensity(aggr_results)

    return aggr_results


def remove_invalid_dim_combos(aggr_results):
    """Remove invalid dimension combinations from the results dict.
    Args:
        aggr_results (dict): the results dict containing the zonal stats for each dimension combination.
    Returns:
        aggr_results (dict): the results dict with invalid dimension combinations removed.
    """
    aggr_results_pruned = aggr_results.copy()

    for indicator in aggr_results:
        for era in aggr_results[indicator]:
            for model in aggr_results[indicator][era]:
                for scenario in aggr_results[indicator][era][model]:
                    if aggr_results[indicator][era][model][scenario] in [{}, None]:
                        del aggr_results_pruned[indicator][era][model][scenario]
                if aggr_results[indicator][era][model] in [{}, None]:
                    del aggr_results_pruned[indicator][era][model]

    return aggr_results_pruned


def package_cmip6_point_data(rasdaman_response):
    # using the dimension names and dim_encodings, create the nested dict to hold results
    dim_encodings = var_ep_lu["cmip6_indicators"]["dim_encodings"]
    # there is a year dimension, but its not represented in the encodings dict, so we need to add the year dimension manually
    dim_encodings["year"] = {int(i - 1950): str(i) for i in range(1950, 2101)}
    # we could dimension names directly from the encodings, but they would be in the wrong order
    # and also contain the band names ("indicator" key) which is not actually a dimension .... so we define explicitly here
    dimnames = [
        "scenario",
        "model",
        "year",
    ]
    iter_coords = list(
        itertools.product(*[dim_encodings[dim].keys() for dim in dimnames])
    )
    dim_combos = get_all_possible_dimension_combinations(
        iter_coords, dimnames, dim_encodings
    )

    results = generate_nested_dict(dim_combos)

    # populate the results dict with the fetched data
    # using the coords to index into the rasdaman response
    for coords, dim_combo in zip(iter_coords, dim_combos):
        indicator_values = rasdaman_response[coords[0]][coords[1]][coords[2]]

        # split the string of values into a list of strings
        indicator_values = indicator_values.split(" ")
        # then replace the items with floats, unless "nan" or "null" in which case we use -9999
        indicator_values = [
            float(value) if value not in ["nan", "null"] else -9999
            for value in indicator_values
        ]

        # if all values are -9999, remove this year from the results
        if all([value == -9999 for value in indicator_values]):
            results[dim_combo[0]][dim_combo[1]].pop(dim_combo[2])
            continue

        indicator_dict = dict()
        for indicator_name, indicator_value in zip(
            var_ep_lu["cmip6_indicators"]["bandnames"], indicator_values
        ):
            indicator_dict[indicator_name] = indicator_value

        results[dim_combo[0]][dim_combo[1]][dim_combo[2]] = indicator_dict

    return nullify_and_prune(results, "cmip6_indicators")


def summarize_cmip6_mmm(results):
    for era in cmip6_eras:
        for scenario in results:
            # remove impossible combinations of scenario and era
            if scenario == "historical" and era != "historical":
                continue
            if scenario != "historical" and era == "historical":
                continue
            for model in results[scenario]:

                # check for an empty dict (means the scenario is missing for this model), if so, skip to the next model
                if results[scenario][model] in [{}, None]:
                    continue

                # set up a dict to hold the values for this model/scenario/era
                model_scenario_era_values = dict()
                model_scenario_era_values[era] = dict()

                for year in results[scenario][model]:
                    # skip era results already saved in the dict
                    if year in cmip6_eras:
                        continue
                    if (
                        int(year) < cmip6_eras[era]["start"]
                        or int(year) > cmip6_eras[era]["end"]
                    ):  # remove unwanted years
                        continue
                    for indicator in results[scenario][model][year]:
                        # if the indicator is not in the dict, add it
                        if indicator not in model_scenario_era_values[era]:
                            model_scenario_era_values[era][indicator] = []
                        # if the value is not -9999, "nan", "null", or None, add it to the values list
                        if results[scenario][model][year][indicator] not in [
                            -9999,
                            "nan",
                            "null",
                            None,
                        ]:
                            model_scenario_era_values[era][indicator].append(
                                results[scenario][model][year][indicator]
                            )

                # compute stats for each indicator for this model/scenario/era
                for era in model_scenario_era_values:
                    results[scenario][model][era] = dict()
                    for indicator in model_scenario_era_values[era]:
                        # if list of values is not empty, compute min, mean, and max
                        if model_scenario_era_values[era][indicator]:
                            era_indicator_mmm = {
                                "min": round(
                                    np.nanmin(
                                        model_scenario_era_values[era][indicator]
                                    ),
                                    1,
                                ),
                                "mean": round(
                                    np.nanmean(
                                        model_scenario_era_values[era][indicator]
                                    ),
                                    1,
                                ),
                                "max": round(
                                    np.nanmax(
                                        model_scenario_era_values[era][indicator]
                                    ),
                                    1,
                                ),
                            }
                        # if list of values is empty, set min, mean, and max to -9999
                        else:
                            era_indicator_mmm = {
                                "min": -9999,
                                "mean": -9999,
                                "max": -9999,
                            }
                        # save results in the results dict
                        results[scenario][model][era][indicator] = era_indicator_mmm

    # prune the results dict to remove any empty dicts (i.e. models/scenarios/eras with no data)
    for scenario in results:
        for model in results[scenario]:
            if results[scenario][model]:
                results[scenario][model] = {
                    era: results[scenario][model][era]
                    for era in results[scenario][model]
                    if era in cmip6_eras.keys()
                }
    results = nullify_and_prune(results, "cmip6_indicators")
    results = prune_nulls_with_max_intensity(results)

    return results


@routes.route("/indicators/")
def about_indicators():
    return render_template("documentation/indicators.html")


@routes.route("/indicators/cmip6/point/<lat>/<lon>")
def run_fetch_cmip6_indicators_point_data(lat, lon):
    """
    Query the CMIP6 indicators coverage which contains indicators summarized over CMIP6 models, scenarios, and years

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested CMIP6 indicator data

    Notes:
        example request: http://localhost:5000/indicators/cmip6/point/65.06/-146.16
    """

    # Validate the lat/lon values
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    cmip6_bbox = construct_latlon_bbox_from_coverage_bounds(cmip6_metadata)
    within_bounds = validate_latlon_in_bboxes(
        lat, lon, [cmip6_bbox], [var_ep_lu["cmip6_indicators"]["cov_id_str"]]
    )
    if within_bounds == 422:
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html", bboxes=[cmip6_bbox]
            ),
            422,
        )
    try:
        rasdaman_response = asyncio.run(
            fetch_indicators_point_data(
                lat, lon, var_ep_lu["cmip6_indicators"]["cov_id_str"], "EPSG:4326"
            )
        )

        results = package_cmip6_point_data(rasdaman_response)

        if "summarize" in request.args and request.args.get("summarize") == "mmm":
            results = summarize_cmip6_mmm(results)

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(results, "cmip6_indicators", place_id, lat, lon)

        return results

    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route(
    "/indicators/base/point/<lat>/<lon>"
)  # original route, kept for backwards compatibility
@routes.route(
    "/indicators/cmip5/point/<lat>/<lon>/"
)  # new route, matches API documentation
def run_fetch_cmip5_indicators_point_data(lat, lon):
    """Query the NCAR 12km indicators_climatologies rasdaman coverage which contains indicators summarized over NCR time eras

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested data

    Notes:
        example request: http://localhost:5000/indicators/cmip5/point/65.06/-146.16
    """
    validation = validate_latlon(
        lat, lon, [var_ep_lu["cmip5_indicators"]["cov_id_str"]]
    )
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 404:
        return (
            render_template("404/no_data.html"),
            404,
        )
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    x, y = project_latlon(lat, lon, 3338)

    try:
        rasdaman_response = asyncio.run(
            fetch_indicators_point_data(
                y, x, var_ep_lu["cmip5_indicators"]["cov_id_str"], "EPSG:3338"
            )
        )

        results = package_cmip5_point_data(rasdaman_response)

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(results, "ncar12km_indicators", place_id, lat, lon)

        return results

    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404


@routes.route(
    "/indicators/base/area/<var_id>"
)  # original route, kept for backwards compatibility
@routes.route(
    "/indicators/cmip5/area/<var_id>/"
)  # new route, matches API documentation
def get_cmip5_indicators_area_data(var_id):
    """Area aggregation data endpoint. Fetch data within polygon area for specified variable and return JSON-like dict.

    Args:
        var_id (str): ID for given polygon from polygon endpoint.
    Returns:
        poly_pkg (dict): zonal mode of indicator summary results for AOI polygon

    """
    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
        aggr_results = run_aggregate_var_polygon(var_id, "cmip5_indicators")

    except:
        return render_template("422/invalid_area.html"), 422

    if aggr_results in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    aggr_results = postprocess(aggr_results, "ncar12km_indicators")

    if request.args.get("format") == "csv":
        return create_csv(aggr_results, "ncar12km_indicators", var_id)

    return aggr_results
