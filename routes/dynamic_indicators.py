import asyncio
import ast
import logging
import xarray as xr
import io
import numpy as np
from flask import Blueprint, render_template, request
import datetime

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    validate_year,
)

# TODO: for additional postprocessing or csv output, uncomment these imports and add code
# from postprocessing import postprocess, prune_nulls_with_max_intensity
# from csv_functions import create_csv

from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("dynamic_indicators_api", __name__)


#### SETUP AND METADATA ####

coverage_ids = [
    "cmip6_daily_pr_historical",
    "cmip6_daily_pr_projected",
]


# function to get the basic metadata for all coverages
async def get_coverage_metadata(coverage_id):
    """Get the coverage metadata."""
    metadata = await describe_via_wcps(coverage_id)
    return metadata


# build dict to hold coverage metadata for each variable / scenario
var_coverage_metadata = {}
for coverage_id in coverage_ids:
    coverage_metadata = asyncio.run(get_coverage_metadata(coverage_id))
    # assumes only one variable per coverage!
    var = list(coverage_metadata["metadata"]["bands"].keys())[0]

    if var not in var_coverage_metadata:
        var_coverage_metadata[var] = {"projected": {}, "historical": {}}
    else:
        pass
    if "projected" in coverage_id:
        era = "projected"
    if "historical" in coverage_id:
        era = "historical"

    var_coverage_metadata[var][era] = {
        "coverage_id": coverage_id,
        # Convert the string representation of model encoding dict to an actual dict
        "model_encoding": ast.literal_eval(
            coverage_metadata["metadata"]["axes"]["model"]["encoding"]
        ),
        "scenario_encoding": ast.literal_eval(
            coverage_metadata["metadata"]["axes"]["scenario"]["encoding"]
        ),
        "time_units": coverage_metadata["metadata"]["axes"]["time"]["units"],
        # "time_min": int(coverage_metadata["metadata"]["axes"]["time"]["min_value"]),
        # "time_max": int(coverage_metadata["metadata"]["axes"]["time"]["max_value"]),
    }

print(var_coverage_metadata)


@routes.route("/dynamic_indicators")
def run_fetch_dynamic_indicator_point_data():
    return "you've reached the dynamic indicators endpoint"
