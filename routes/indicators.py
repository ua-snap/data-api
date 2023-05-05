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
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
    validate_year,
)
from validate_data import (
    get_poly_3338_bbox,
    nullify_and_prune,
    postprocess,
    place_name_and_type,
)
from . import routes
from config import WEST_BBOX, EAST_BBOX

indicators_api = Blueprint("indicators_api", __name__)

# All of the cordex_<variable name> coverages should have the same encodings 
cordex_dim_encodings = asyncio.run(get_dim_encodings("cordex_tas"))

cordex_indicator_coverage_lu = {
    "tx_days_above": "cordex_tasmax",
    "tx_days_below": "cordex_tasmin"
}

def generate_tx_days_above_or_below_wcps_str(cov_id, tx, lat, lon, start_year, n_years, encoding="json", above=True):
    """WCPS query to count the number of days in above or below a given threshold. Assumes coverage has scenario and model axes and iterates over all combinations.
    
    Args:
        cov_di (str): coverage ID
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
                f"over $s scenario(domain($c, scenario)), $m model(domain($c, model)), $y year(0:{n_years - 1}) "
                "values count("
                    f"$c[scenario($s), model($m), lat({lat}), lon({lon}), "
                        "ansi("
                            f"imageCrsDomain($c[ansi({ansi_str})], ansi).lo + ($y * 365):"
                            f"imageCrsDomain($c[ansi({ansi_str})], ansi).hi + ($y * 365)"
                        ")"
                    f"] - 273.15 {operand} {tx}"
                f'), "application/{encoding}"'
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
            cov_id, **kwargs
        )
    )
    
    point_data_list = await fetch_data([url])
    
    return point_data_list


@routes.route("/indicators/tx_days_above/<tx>/point/<lat>/<lon>")
def run_fetch_tx_days_above_point_data(tx, lat, lon):
    """

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

    try:
        # TO-DO: maybe have a if clause here to check if threshold is within reasonable range?
        _ = float(tx)
        # try:
        point_data_list = asyncio.run(
            fetch_yearly_tx_above_or_below_point_data(
                "tx_days_above", tx=tx, lat=lat, lon=lon, start_year=2006, n_years=30, encoding="json", above=True
            )
        )
            
    except ValueError:
        return render_template("400/bad_request.html"), 400

    return point_data_list

