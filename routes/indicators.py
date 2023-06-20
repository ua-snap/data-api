"""Endpoints for climate indicators

There are two types of endpoints being developed here currently: 
1. Endpoint(s) which queries a coverage containing summarized versions of the indicators dataset created from the 12km NCAR dataset. The thresholds and eras are preconfigured in the coverage. Calling this the "base" indicators for now (i.e., url suffix: /indicators/base). 
2. Endpoint(s) that produce dynamic indicators, which query coverages of specific variables in the base CORDEX dataset we have. Unfortunately, not all desired indicators are going to be easily produced in a similar fashion with this route given the limitations of WCPS. This could be called "dynamic" indicators?
"""

import asyncio
import numpy as np
from math import floor
from flask import Blueprint, render_template, request, Response
from shapely.geometry import Point

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import *
from validate_request import (
    validate_latlon, 
    project_latlon,
    validate_var_id
)
from validate_data import (
    get_poly_3338_bbox,
    nullify_and_prune,
    postprocess
)
from . import routes
from config import WEST_BBOX, EAST_BBOX

indicators_api = Blueprint("indicators_api", __name__)
# Rasdaman targets
indicators_coverage_id = "ncar12km_indicators_era_summaries"

# dim encodings for the NCAR 12km BCSD indicators coverage
base_dim_encodings = asyncio.run(get_dim_encodings(indicators_coverage_id))

#
# Dynamic indicators endpoints
#


def generate_tx_days_above_or_below_wcps_str(
    cov_id, tx, lat, lon, start_year, n_years, encoding="json", above=True
):
    """WCPS query to count the number of days in above or below a given threshold. Assumes coverage has scenario and model axes and iterates over all combinations.

    Args:
        cov_id (str): coverage ID
        tx (float/int): threshold value
        lat (float): latitude coordinate for point query, or string composed as "lower:upper" for bbox query, where lower and upper are lower and upper bounds of bbox
        lon (float): longitude coordinate for point query, or string composed as "lower:upper" for bbox query, where lower and upper are lower and upper bounds of bbox
        start_year (int): Starting year of era to summarize over
        n_years (int): number of consecutive years of era to summarize over
        tx_above (bool): count days above threshold tx. Set to False to count days below tx
        encoding (str): currently supports either "json" or "netcdf" for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """
    if above:
        operand = ">"
    else:
        operand = "<"

    ansi_str = (
        f'"{start_year}-01-01T12:00:00.000Z":"{start_year + 1}-01-01T12:00:00.000Z"'
    )

    query_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({cov_id}) "
            "return encode("
            "coverage tx_days_above_or_below "
            # <axis_name>(domain($c, <axis_name>)) specifies iteration over all available coordinates
            f"over $s scenario(domain($c, scenario)), $m model(domain($c, model)), $y year(0:{n_years - 1}) "
            "values count("
            f"$c[scenario($s), model($m), lat({lat}), lon({lon}), "
            "ansi("
            f"imageCrsDomain($c[ansi({ansi_str})], ansi).lo + ($y * 365):"
            f"imageCrsDomain($c[ansi({ansi_str})], ansi).hi + ($y * 365)"
            ")"
            f"] - 273.15 {operand} {tx}"
            f'), "application/{encoding}", "nodata=nan"'
            ")"
        )
    )

    return query_str


async def fetch_yearly_tx_above_or_below_point_data(indicator_id, **kwargs):
    """Make the async request for indicator data for a range of years at a specified point

    Args:
        lat (float):
        lon (float):
        start_year (int):
        end_year (int):

    Returns:
        list of data results from each of historical and future coverages
    """
    cov_id = cordex_indicator_coverage_lu[indicator_id]

    if indicator_id == "tx_days_above":
        above = True
    else:
        above = False

    url = generate_wcs_query_url(
        generate_tx_days_above_or_below_wcps_str(cov_id, above=above, **kwargs)
    )

    point_data_list = await fetch_data([url])

    return point_data_list


def package_era_indicator_results(point_data_list):
    """Package the indicator values for a given query (assumed for a given year range)

    Args:
        point_data_list (list): nested list of data from Rasdaman WCPS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from dim_encodings global variable
    """
    di = dict()
    for si, mod_li in enumerate(point_data_list):
        scenario = dim_encodings["scenario"][si]
        di[scenario] = dict()
        for mi, scenario_li in enumerate(mod_li):
            model = dim_encodings["model"][mi]
            di[scenario][model] = dict()
            di[scenario][model]["min"] = np.min(yr_li).round(1)
            di[scenario][model]["mean"] = np.mean(yr_li).round(1)
            di[scenario][model]["max"] = np.max(yr_li).round(1)

    return di


@routes.route("/indicators/dynamic/point/<indicator>/<tx>/<lat>/<lon>")
def run_fetch_tx_days_above_point_data(indicator, tx, lat, lon):
    """

    Args:
        tx (float): temeprature threshold value
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of indicator data derived from the CORDEX temperature coverages

    Notes:
        example request: http://localhost:5000/indicators/point/tx_days_above/20/65.0628/-146.1627
    """
    # TO-DO: validate_indicator(indicator)

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

    try:
        # TO-DO: maybe have a if clause here to check if threshold is within reasonable range?
        _ = float(tx)
        n_years = 30
        start_years = [2010, 2040, 2070]
        indicator_package = {}
        for year in start_years:
            point_data_list = asyncio.run(
                fetch_yearly_tx_above_or_below_point_data(
                    indicator,
                    tx=tx,
                    lat=lat,
                    lon=lon,
                    start_year=year,
                    n_years=n_years,
                    encoding="json",
                )
            )
            era_key = f"{year}-{year + n_years - 1}"
            indicator_package[era_key] = package_era_indicator_results(point_data_list)

    except ValueError:
        return render_template("400/bad_request.html"), 400

    return indicator_package


#
# Base indicators endpoint:
#


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
                di[indicator][era][model] = dict()
                for si, stat_li in enumerate(scenario_li):
                    scenario = base_dim_encodings["scenario"][si]
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


@routes.route("/indicators/base/point/<lat>/<lon>")
def run_fetch_base_indicators_point_data(lat, lon):
    """Query the cordex_indicators_climatologies rasdaman coverage which contains indicators summarized over NCR time eras

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested ALFRESCO data

    Notes:
        example request: http://localhost:5000/ TO-DO /point/65.0628/-146.1627
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

    except ValueError:
        return render_template("400/bad_request.html"), 400

    results = package_base_indicators_data(point_data_list)

    results = nullify_and_prune(results, "ncar12km_indicators")

    return results


def summarize_within_poly_marr(
    ds, poly_mask_arr, dim_encodings, bandname="Gray"
):
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
    
    #aggr_results = remove_invalid_dim_combos(aggr_results)

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

    # try:
    #     indicators_pkg = run_aggregate_var_polygon(var_id)
    # except:
    #     return render_template("422/invalid_area.html"), 422

    indicators_pkg = run_aggregate_var_polygon(var_id)

    if indicators_pkg in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    indicators_pkg = postprocess(indicators_pkg, "ncar12km_indicators")

    return indicators_pkg