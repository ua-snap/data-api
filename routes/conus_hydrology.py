# import asyncio
# import numpy as np
import geopandas as gpd
import requests
from flask import render_template, request, current_app as app, jsonify

# local imports
# from fetch_data import fetch_wcs_point_data
from validate_request import validate_latlon

#    project_latlon,
# )
# from validate_data import *
# from postprocessing import postprocess
# from csv_functions import create_csv
from config import CONUS_BBOX
from . import routes

conus_hydrology_coverage_id = "conus_hydro_segments_crstephenson"
# TODO: change this to 'Rasdaman Encoding' once coverage is updated
encoding_attr = "Encoding"


def get_huc_from_lat_lon(lat, lon):
    """
    Function to get the HUC6 polygon from a given latitude and longitude.
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        GeoDataFrame containing a single HUC6 polygon
    """
    return huc6


def get_geom_ids_within_huc(huc):
    """
    Function to get the geometry IDs within a given HUC6 polygon.
    Args:
        huc (GeoDataFrame): GeoDataFrame containing a single HUC6 polygon
    Returns:
        List of geometry IDs (integers) within the HUC6 polygon
    """
    return geom_ids


def fetch_hydrology_data(geom_ids):
    """
    Function to fetch hydrology data from the geoserver.
    Args:
        geom_ids (list): List of geometry IDs (integers)
    Returns:
        Dictionary with hydrology statistics for each geom_id
    """
    return None


@routes.route("/conus_hydrology/point/<lat>/<lon>")
def run_get_conus_hydrology_point_data(lat, lon):
    """
    Function to pull demographics data as JSON or CSV.
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        JSON-like output of hydrology statistics for HUC surrounding the point.

    Notes:
           example: http://localhost:5000/conus_hydrology/point/64.2008/-149.4937
    """
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        # TODO: make a specific bad request template for CONUS: the existing one is specific to the Arctic!
        # for now, just render the 400 template
        return render_template("400/bad_request.html"), 422
        # return render_template("422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX), 422

    # get the HUC6 polygon
    huc = get_huc_from_lat_lon(lat, lon)

    # get the geometry IDs within the HUC6 polygon
    geom_ids = get_geom_ids_within_huc(huc)

    # get the hydrology statistics for geom_ids
    stats_dict = fetch_hydrology_data(geom_ids)

    return None
