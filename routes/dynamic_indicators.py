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
from generate_requests import (
    construct_count_annual_days_above_or_below_threshold_wcps_query_string,
    construct_get_annual_mmm_stat_wcps_query_string,
)
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


@routes.route(
    "/dynamic_indicators/count_days_above/<threshold>/<units>/<variable>/<lat>/<lon>/"
)
def count_days_above(threshold, units, variable, lat, lon):
    # tasmax coverage
    # usage: .../dynamic_indicators/count_days_above/25/C/tasmax/64.5/-147.5/
    # usage: .../dynamic_indicators/count_days_above/5/mm/pr/64.5/-147.5/

    # sample URL for days above 25C
    url = generate_wcs_query_url(
        "ProcessCoverages&query="
        + construct_count_annual_days_above_or_below_threshold_wcps_query_string(
            "cmip6_downscaled_tasmax_5ModelAvg_ssp585_wcs",
            25,
            ">",
            2000,
            2010,
            350000,
            1700000,
        )
    )

    return None


@routes.route("/dynamic_indicators/count_days_below/<threshold>/<units>/<lat>/<lon>/")
def count_days_below(threshold, units, lat, lon):
    # tasmin coverage
    # usage: .../dynamic_indicators/count_days_below/-30/C/64.5/-147.5/
    return None


@routes.route("/dynamic_indicators/stat/<stat>/<variable>/<units>/<lat>/<lon>/")
def get_annual_stat(stat, variable, units, lat, lon):
    # pr, tasmax, or tasmin coverage
    # usage: .../dynamic_indicators/stat/max/pr/mm/64.5/-147.5/
    # usage: .../dynamic_indicators/stat/min/tasmin/C/64.5/-147.5/

    # sample URL for maxmimum daily precip
    url = generate_wcs_query_url(
        "ProcessCoverages&query="
        + construct_get_annual_mmm_stat_wcps_query_string(
            "cmip6_downscaled_prx_5ModelAvg_ssp585_wcs",
            "max",
            2000,
            2010,
            350000,
            1700000,
        )
    )
    return None


@routes.route("/dynamic_indicators/rank/<position>/<direction>/<variable>/<lat>/<lon>/")
def get_annual_stat(position, direction, variable, units, lat, lon):
    # pr, tasmax, or tasmin coverage
    # usage: .../dynamic_indicators/rank/6/highest/pr/64.5/-147.5/
    # usage: .../dynamic_indicators/rank/6/lowest/tasmin/64.5/-147.5/

    # sample URL for all values - postprocess this for ranking
    url = generate_wcs_query_url(
        "ProcessCoverages&query="
        + construct_get_annual_mmm_stat_wcps_query_string(
            "cmip6_downscaled_prx_5ModelAvg_ssp585_wcs",
            "",
            2000,
            2010,
            350000,
            1700000,
        )
    )
    return None
