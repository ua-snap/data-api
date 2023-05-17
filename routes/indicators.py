"""Endpoints for climate indicators

There are two types of endpoints being developed here currently: 
1. Endpoint(s) which queries a coverage containing summarized versions of the indicators dataset created for work with John W and TBEC. The thresholds and eras are preconfigured in the coverage. Calling this the "base" indicators for now. 
2. Endpoint(s) that produce dynamic indicators, which query coverages of specific variables in the base CORDEX dataset we have. Unfortunately, not all desired indicators are going to be easily produced in a similar fashion with this route given the limitations of WCPS. Calling this "dynamic" indicators here.
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
from validate_request import validate_latlon, project_latlon
from validate_data import (
    get_poly_3338_bbox,
    nullify_and_prune,
)
from . import routes
from config import WEST_BBOX, EAST_BBOX

indicators_api = Blueprint("indicators_api", __name__)

# dim encodings for the NCAR 12km BCSD indicators coverage
base_dim_encodings = asyncio.run(get_dim_encodings("ncar12km_indicators_era_summaries"))
print(base_dim_encodings)

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


@routes.route("/indicators/point/<indicator>/<tx>/<lat>/<lon>")
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
        x, y, cov_id="ncar12km_indicators_era_summaries", time_slice=("era", "0,2")
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
                        di[indicator][era][model][scenario][stat] = value

    return di


@routes.route("/indicators/base/<lat>/<lon>")
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
