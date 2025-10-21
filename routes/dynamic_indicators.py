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
from fetch_data import fetch_data
from generate_urls import generate_wcs_query_url
from generate_requests import (
    construct_count_annual_days_above_or_below_threshold_wcps_query_string,
    construct_get_annual_mmm_stat_wcps_query_string,
)
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    validate_year,
    project_latlon,
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


def validate_latlon_and_reproject_to_epsg_3338(lat, lon):
    lat = float(lat)
    lon = float(lon)
    if not latlon_is_numeric_and_in_geodetic_range(lat, lon):
        raise ValueError("Latitude and/or longitude are out of range or not numeric.")

    # TODO: add step to validate geographic lat/lon with geotiff

    lat, lon = project_latlon(lat, lon, from_epsg=4326, to_epsg=3338)
    return lat, lon


def validate_operator(operator):
    if operator not in ["above", "below"]:
        raise ValueError("Operator must be 'above' or 'below'.")
    if operator == "above":
        operator = ">"
    else:
        operator = "<"
    return operator


def validate_units_threshold_and_variable(units, variable):
    if variable in ["tasmax", "tasmin"]:
        if units not in ["C", "F"]:
            raise ValueError("Units for temperature must be 'C' or 'F'.")
        if units == "F":
            threshold = (float(threshold) - 32) * 5.0 / 9.0
        else:
            threshold = float(threshold)
    elif variable == "pr":
        if units not in ["mm", "in"]:
            raise ValueError("Units for precipitation must be 'mm' or 'in'.")
        if units == "in":
            threshold = float(threshold) * 25.4
        else:
            threshold = float(threshold)
    else:
        raise ValueError("Variable must be 'tasmax', 'tasmin', or 'pr'.")
    return units, threshold


@routes.route(
    "/dynamic_indicators/count_days/<operator>/<threshold>/<units>/<variable>/<lat>/<lon>/<start_year>/<end_year>/"
)
def count_days_above(
    operator, threshold, units, variable, lat, lon, start_year, end_year
):
    # tasmax coverage
    # usage: .../dynamic_indicators/count_days/above/25/C/tasmax/64.5/-147.5/2000/2050
    # usage: .../dynamic_indicators/count_days/above/5/mm/pr/64.5/-147.5/2000/2050/

    # Validate request params
    lat, lon = validate_latlon_and_reproject_to_epsg_3338(lat, lon)
    operator = validate_operator(operator)
    units = validate_units_threshold_and_variable(threshold, units, variable)
    validate_year(start_year)
    validate_year(end_year)

    # TODO: parse years and list appropriate coverages, for example:
    # year_ranges = [(2000,2014), (2015,2050), (2015,2050), (2015,2050), (2015,2050)]
    # coverages = ["cmip6_downscaled_pr_5ModelAvg_historical_wcs",
    #     "cmip6_downscaled_pr_5ModelAvg_ssp126_wcs",
    #     "cmip6_downscaled_pr_5ModelAvg_ssp245_wcs",
    #     "cmip6_downscaled_pr_5ModelAvg_ssp370_wcs",
    #     "cmip6_downscaled_pr_5ModelAvg_ssp585_wcs",]
    year_ranges = []
    coverages = []

    urls = []

    for coverage, year_range in zip(coverages, year_ranges):
        url = generate_wcs_query_url(
            "ProcessCoverages&query="
            + construct_count_annual_days_above_or_below_threshold_wcps_query_string(
                coverage,
                threshold,
                operator,
                year_range[0],
                year_range[1],
                lon,
                lat,
            )
        )
        urls.append(url)

    data = fetch_data[urls]

    # TODO: postprocess the results as needed

    return data


@routes.route(
    "/dynamic_indicators/stat/<stat>/<variable>/<units>/<lat>/<lon>/<start_year>/<end_year>/"
)
def get_annual_stat(stat, variable, units, lat, lon, start_year, end_year):
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


@routes.route(
    "/dynamic_indicators/rank/<position>/<direction>/<variable>/<lat>/<lon>/<start_year>/<end_year>/"
)
def get_annual_stat(
    position, direction, variable, units, lat, lon, start_year, end_year
):
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
