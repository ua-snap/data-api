import asyncio
import ast
import logging
import xarray as xr
import cftime
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

coverages = {
    "pr": [
        "cmip6_downscaled_pr_5ModelAvg_historical_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp126_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp245_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp370_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp585_wcs",
    ],
    "tasmin": [
        "cmip6_downscaled_tasmin_5ModelAvg_historical_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp126_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp245_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp370_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp585_wcs",
    ],
    "tasmax": [
        "cmip6_downscaled_tasmax_5ModelAvg_historical_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp126_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp245_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp370_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp585_wcs",
    ],
}

time_domains = {
    "historical": (1965, 2014),
    "projected": (2015, 2100),
}
