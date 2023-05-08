"""Indicators"""

import asyncio
import io
import csv
import calendar
import numpy as np
from math import floor
from flask import Blueprint, render_template, request, Response
from shapely.geometry import Point

# local imports
from generate_urls import generate_wcs_query_url
from fetch_data import *
from validate_request import validate_latlon
from . import routes
from config import WEST_BBOX, EAST_BBOX

indicators_api = Blueprint("indicators_api", __name__)

# All of the cordex_<variable name> coverages should have the same encodings 
dim_encodings = asyncio.run(get_dim_encodings("cordex_tasmax"))

cordex_indicator_coverage_lu = {
    "tx_days_above": "cordex_tasmax",
    "tx_days_below": "cordex_tasmin"
}

def generate_tx_days_above_or_below_wcps_str(cov_id, tx, lat, lon, start_year, n_years, encoding="json", above=True):
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

    ansi_str = f'"{start_year}-01-01T12:00:00.000Z":"{start_year + 1}-01-01T12:00:00.000Z"'

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
        generate_tx_days_above_or_below_wcps_str(
            cov_id, above=above, **kwargs
        )
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
        for mi, yr_li in enumerate(mod_li):
            model = dim_encodings["model"][mi]
            yr_li = np.array(yr_li).astype(float)
            di[scenario][model] = dict()
            di[scenario][model]["min"] = np.min(yr_li).round(1)
            di[scenario][model]["mean"] = np.mean(yr_li).round(1)
            di[scenario][model]["max"] = np.max(yr_li).round(1)
    
    return di


@routes.route("/indicators/<indicator>/<tx>/point/<lat>/<lon>")
def run_fetch_tx_days_above_point_data(indicator, tx, lat, lon):
    """

    Args:
        tx (float): temeprature threshold value
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested ALFRESCO data

    Notes:
        example request: http://localhost:5000/ TO-DO /point/65.0628/-146.1627
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
                    indicator, tx=tx, lat=lat, lon=lon, start_year=year, n_years=n_years, encoding="json"
                )
            )
            era_key = f"{year}-{year + n_years - 1}"
            indicator_package[era_key] = package_era_indicator_results(point_data_list)

    except ValueError:
        return render_template("400/bad_request.html"), 400

    return indicator_package

