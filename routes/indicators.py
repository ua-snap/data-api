"""Endpoints for climate indicators

These endpoint(s) query a coverage containing summarized versions of the indicators dataset created from the 12km NCAR dataset. The thresholds and eras are preconfigured in the coverage. Calling this the "base" indicators for now (i.e., url suffix: /indicators/base).
"""

import asyncio
import numpy as np
from math import floor, isnan
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import *
from validate_request import (
    validate_latlon,
    validate_cmip6_indicators_latlon,
    project_latlon,
    validate_var_id,
)
from postprocessing import nullify_and_prune, postprocess
from csv_functions import create_csv
from . import routes
from config import WEST_BBOX, EAST_BBOX

indicators_api = Blueprint("indicators_api", __name__)
# Rasdaman targets
indicators_coverage_id = "ncar12km_indicators_era_summaries"
cmip6_indicators_coverage_id = "cmip6_indicators"

# dim encodings for the NCAR 12km BCSD indicators coverage
base_dim_encodings = asyncio.run(get_dim_encodings(indicators_coverage_id))
cmip6_dim_encodings = asyncio.run(get_dim_encodings(cmip6_indicators_coverage_id))


async def fetch_cmip6_indicators_point_data(lat, lon):
    """
    Make an async request for CMIP6 indicator data for a range of models, scenarios, and years at a specified point

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        list of data results from each of historical and future data at a specified point
    """

    # We must use EPSG:4326 for the CMIP6 indicators coverage to match the coverage projection
    wcs_str = generate_wcs_getcov_str(
        lon, lat, cov_id=cmip6_indicators_coverage_id, projection="EPSG:4326"
    )

    # Generate the URL for the WCS query
    url = generate_wcs_query_url(wcs_str)

    # Fetch the data
    point_data_list = await fetch_data([url])

    return point_data_list


# Base indicators endpoint:
async def fetch_base_indicators_point_data(x, y):
    """Make the async request for indicator data for a range of years at a specified point

    Args:
        x (float):
        y (float):

    Returns:
        list of data results from each of historical and future coverages
    """
    wcs_str = generate_wcs_getcov_str(
        x, y, cov_id=indicators_coverage_id, time_slice=("era", "0,2")
    )
    url = generate_wcs_query_url(wcs_str)
    point_data_list = await fetch_data([url])

    return point_data_list


def package_cmip6_indicators_data(point_data_list):
    """
    Package the CMIP6 indicator values for a given query

    Args:
        point_data_list (list): nested list of data from Rasdaman WCPS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from dim_encodings global variable
    """
    di = dict()

    # Loop through point_data_list and populate di with the values
    for si, scenario_li in enumerate(point_data_list):
        scenario = cmip6_dim_encodings["scenario"][si]
        di[scenario] = dict()
        for mi, model_li in enumerate(scenario_li):
            model = cmip6_dim_encodings["model"][mi]
            di[scenario][model] = dict()

            for yi, year_li in enumerate(model_li):
                year = yi + 1850

                # If the scenario is historical and the year is greater than 2014, we break the loop
                # since there is no more historical data
                if scenario == "historical" and year > 2014:
                    break

                # If the scenario is not historical and the year is less than 2015,
                # we continue to the next iteration
                if scenario != "historical" and year < 2015:
                    continue

                # If the year is greater than 2100, we break the loop since there is no more future data
                if year > 2100:
                    break
                di[scenario][model][year] = dict()
                for vi, value in enumerate(year_li.split(" ")):
                    indicator = cmip6_dim_encodings["indicator"][vi]

                    if indicator == "rx1day":
                        value = round(float(value), 1)
                        if isnan(value):
                            value = -9999

                    if value == "nan":
                        value = -9999

                    di[scenario][model][year][indicator] = int(value)

    return di


def package_base_indicators_data(point_data_list):
    """Package the indicator values for a given query

    Args:
        point_data_list (list): nested list of data from Rasdaman WCPS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from dim_encodings global variable
    """
    # base_dim_encodings
    # TO-DO: is there a function for recursively populating a dict like this? If not there should be, this is how we package all of our data
    di = dict()
    for vi, era_li in enumerate(point_data_list):
        indicator = base_dim_encodings["indicator"][vi]
        di[indicator] = dict()
        for ei, model_li in enumerate(era_li):
            era = base_dim_encodings["era"][ei]
            di[indicator][era] = dict()
            for mi, scenario_li in enumerate(model_li):
                model = base_dim_encodings["model"][mi]
                # Skip impossible combinations of era and model.
                if era == "historical" and model != "Daymet":
                    continue
                elif era != "historical" and model == "Daymet":
                    continue
                di[indicator][era][model] = dict()
                for si, stat_li in enumerate(scenario_li):
                    scenario = base_dim_encodings["scenario"][si]
                    # Skip impossible combinations of era and scenario.
                    if era == "historical" and scenario != "historical":
                        continue
                    elif era != "historical" and scenario == "historical":
                        continue
                    di[indicator][era][model][scenario] = dict()
                    for ti, value in enumerate(stat_li):
                        stat = base_dim_encodings["stat"][ti]
                        di[indicator][era][model][scenario][stat] = (
                            value
                            if (
                                indicator == "hd"
                                or indicator == "cd"
                                or indicator == "rx1day"
                                or indicator == "rx5day"
                            )
                            else floor(value)
                        )

    return di


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
    validation = validate_cmip6_indicators_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    try:
        point_data_list = asyncio.run(fetch_cmip6_indicators_point_data(lat, lon))
        results = package_cmip6_indicators_data(point_data_list)
        results = nullify_and_prune(results, "cmip6_indicators")

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(results, "cmip6_indicators", place_id, lat, lon)

        return results

    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404


@routes.route("/indicators/base/point/<lat>/<lon>")
def run_fetch_base_indicators_point_data(lat, lon):
    """Query the NCAR 12km indicators_climatologies rasdaman coverage which contains indicators summarized over NCR time eras

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested ALFRESCO data

    Notes:
        example request: http://localhost:5000/indicators/base/point/65.06/-146.16
    """
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    x, y = project_latlon(lat, lon, 3338)

    try:
        point_data_list = asyncio.run(fetch_base_indicators_point_data(x=x, y=y))
        results = package_base_indicators_data(point_data_list)
        results = nullify_and_prune(results, "ncar12km_indicators")

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(results, "ncar12km_indicators", place_id, lat, lon)

        return results

    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404


def summarize_within_poly_marr(ds, poly_mask_arr, dim_encodings, bandname="Gray"):
    """Summarize a single Data Variable of a xarray.DataSet within a polygon. Return the results as a nested dict.

    NOTE - This is a candidate for de-duplication! Only defining here because some
    things are out-of-sync with existing ways of doing things (e.g., key names
    in dim_encodings dicts in other endpoints are not equal to axis names in coverages)

    Args:
        ds (xarray.DataSet): DataSet with "Gray" as variable of interest
        poly_mask_arr (numpy.ma.core.MaskedArra): a masked array masking the cells intersecting the polygon of interest
        dim_encodings (dict): nested dictionary of thematic key value pairs that chacterize the data and map integer data coordinates to models, scenarios, variables, etc.
        bandname (str): name of variable in ds, defaults to "Gray" for rasdaman coverages where the name is not given at ingest

    Returns:
        Nested dict of results for all non-X/Y axis combinations,
    """
    # will actually operate on underlying DataArray

    da = ds[bandname]
    # get axis (dimension) names and make list of all coordinate combinations
    all_dims = da.dims
    dimnames = [dimname for dimname in all_dims if dimname not in ("X", "Y")]
    iter_coords = list(
        itertools.product(*[list(ds[dimname].values) for dimname in dimnames])
    )

    # generate all combinations of decoded coordinate values
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            dim_encodings[dimname][coord] for coord, dimname in zip(coords, dimnames)
        ]
        dim_combos.append(map_list)
    aggr_results = generate_nested_dict(dim_combos)

    data_arr = []
    for coords in iter_coords:
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        data_arr.append(da.sel(sel_di).values)
    data_arr = np.array(data_arr)

    # need to transpose the 2D spatial slices if X is the "rows" dimension
    if all_dims.index("X") < all_dims.index("Y"):
        data_arr = data_arr.transpose(0, 2, 1)

    data_arr_mask = np.broadcast_to(poly_mask_arr.mask, data_arr.shape)
    data_arr[data_arr_mask] = np.nan
    results = np.nanmean(data_arr, axis=(1, 2)).astype(float)
    results[np.isnan(results)] = -9999

    for map_list, result in zip(dim_combos, results):
        if len(map_list) > 1:
            data = get_from_dict(aggr_results, map_list[:-1])
            result = (
                round(result, 1)
                if map_list[0] in ["hd", "cd", "rx1day", "rx5day"]
                else floor(result)
            )
            data[map_list[-1]] = result
        else:
            aggr_results[map_list[0]] = round(result, 4)

    indicators = base_dim_encodings["indicator"].values()
    eras = base_dim_encodings["era"].values()
    models = base_dim_encodings["model"].values()
    scenarios = base_dim_encodings["scenario"].values()

    # Prune impossible (always nodata) historical/projected combos from results.
    for indicator, era, model, scenario in itertools.product(
        indicators, eras, models, scenarios
    ):
        if model in aggr_results[indicator][era]:
            if scenario in aggr_results[indicator][era][model]:
                # Remove impossible combinations of era and scenario.
                if era == "historical" and scenario != "historical":
                    del aggr_results[indicator][era][model][scenario]
                elif era != "historical" and scenario == "historical":
                    del aggr_results[indicator][era][model][scenario]
            # Remove impossible combinations of era and model.
            if era == "historical" and model != "Daymet":
                del aggr_results[indicator][era][model]
            elif era != "historical" and model == "Daymet":
                del aggr_results[indicator][era][model]

    return aggr_results


def run_aggregate_var_polygon(poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon.

    Args:
        poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal mean.

    Returns:
        aggr_results (dict): data representing zonal means within the polygon.

    Notes:
        Fetches data on the individual instances of the singular dimension combinations. Consider validating polygon IDs in `validate_data` or `lat_lon` module.
    """
    poly = get_poly_3338_bbox(poly_id)

    ds_list = asyncio.run(fetch_bbox_data(poly.bounds, indicators_coverage_id))

    bandname = "Gray"
    poly_mask_arr = get_poly_mask_arr(ds_list[0], poly, bandname)

    aggr_results = summarize_within_poly_marr(
        ds_list[-1], poly_mask_arr, base_dim_encodings, bandname
    )

    for era, summaries in aggr_results.items():
        aggr_results[era] = summaries

    return aggr_results


@routes.route("/indicators/base/area/<var_id>")
def indicators_area_data_endpoint(var_id):
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
        indicators_pkg = run_aggregate_var_polygon(var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    indicators_pkg = run_aggregate_var_polygon(var_id)

    if indicators_pkg in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    indicators_pkg = postprocess(indicators_pkg, "ncar12km_indicators")

    if request.args.get("format") == "csv":
        return create_csv(indicators_pkg, "ncar12km_indicators", var_id)

    return indicators_pkg
