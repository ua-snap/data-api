# import asyncio
# import numpy as np
import geopandas as gpd
import requests
import json
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)

# local imports
from generate_urls import generate_wfs_huc6_intersection_url

# from fetch_data import fetch_wcs_point_data
from validate_request import validate_latlon

#    project_latlon,
# )
# from validate_data import *
# from postprocessing import postprocess
# from csv_functions import create_csv
from config import CONUS_BBOX, GS_BASE_URL
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
    url = generate_wfs_huc6_intersection_url(lat, lon)
    # get the features
    with requests.get(
        url, verify=False  # verify=False is necessary for dev version of Geoserver
    ) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        else:
            try:
                r_json = r.json()
            except:
                print("Unable to decode as JSON, got raw text:\n", r.text)
                return render_template("500/server_error.html"), 500

    # create a valid geodataframe from the features
    # CRS is hardcoded to EPSG:4269!
    huc6_gdf = gpd.GeoDataFrame.from_features(r_json["features"], crs="EPSG:4269")
    huc6_gdf["geometry"] = huc6_gdf["geometry"].make_valid()

    return huc6_gdf


def get_bbox_features_and_clip(huc6_gdf):
    """
    Function to get features from a Geoserver layer within the HUC6 bounding box and clip them to the HUC6 polygon.
    Args:
        huc6_gdf (GeoDataFrame): GeoDataFrame containing a single HUC6 polygon
    Returns:
        GeoDataFrame containing the clipped features
    """
    # get the bounding box in correct CRS
    huc6_gdf = huc6_gdf.to_crs("EPSG:5070")
    xmin, ymin, xmax, ymax = huc6_gdf.bounds.values[0]

    # build the bbox string, double checking that the xmin/xmax values are in the correct order
    # this is somewhat redundant but when the projected coordinates are negative, the order can be flipped and produce errors
    bbox_string = (
        str(int(min(xmin, xmax)))
        + ", "
        + str(int(min(ymin, ymax)))
        + ", "
        + str(int(max(xmin, xmax)))
        + ", "
        + str(int(max(ymin, ymax)))
    )

    # define the base request string
    request_string = (
        GS_BASE_URL
        + "hydrology/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=hydrology:seg&outputFormat=application/json&bbox="
        + bbox_string
    )

    # get the features
    with requests.get(
        request_string, verify=False
    ) as r:  # verify=False is necessary for dev version of Geoserver
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        else:
            try:
                r_json = r.json()
            except:
                print("Unable to decode as JSON, got raw text:\n", r.text)
                return render_template("500/server_error.html"), 500

    # create a valid geodataframe from the features and clip the features to the polygon
    # CRS is hardcoded to EPSG:5070!
    bbox_gdf = gpd.GeoDataFrame.from_features(r_json["features"], crs="EPSG:5070")
    bbox_gdf["geometry"] = bbox_gdf["geometry"].make_valid()

    clipped_gdf = gpd.clip(bbox_gdf, huc6_gdf)

    return clipped_gdf


def build_data_dict(huc, huc_segments):
    """
    Function to get the geometry IDs, names of segments, and geometry of segments within a given HUC6 polygon, and build the dictionary to hold the data.
    Args:
        huc (GeoDataFrame): GeoDataFrame containing a single HUC6 polygon
        huc_segments (GeoDataFrame): GeoDataFrame containing the clipped features
    Returns:
        Dictionary with geometry IDs, segment names, and feature geometries
    """
    data_dict = dict({"huc6": huc["huc6"], "name": huc["name"], "segments": dict({})})

    # add the geometry ID, segment name, and feature geometry to the dictionary
    # also add an empty stats dict to populate later

    for idx, row in huc_segments.iterrows():

        geojson = (
            huc_segments[["seg_id_nat", "GNIS_NAME", "geometry"]]
            .loc[idx]
            .to_json(default_handler=str)
        )

        segment_dict = dict(
            {
                "name": row.GNIS_NAME,
                "stats": dict({}),
                "geojson": geojson,
            }
        )

        data_dict["segments"][row.seg_id_nat] = segment_dict

    return data_dict


def fetch_hydrology_data(geom_ids, segment_names):
    """
    Function to fetch hydrology data from the geoserver.
    Args:
        geom_ids (list): List of geometry IDs (integers)
        segment_names (list): List of segment names
    Returns:
        Dictionary with segment name and hydrology statistics for each geom_id
    """
    # like this:
    stats_dict = {
        123456: {
            "name": "Stream ABC",
            "stats": {"stat1": 0.5, "stat2": 0.1, "stat3": 0.3},
        }
    }

    return stats_dict


@routes.route("/conus_hydrology/")
def conus_hydrology_about():
    return render_template("/documentation/conus_hydrology.html")


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
           example: http://localhost:5000/conus_hydrology/point/39.8283,-98.5795
    """
    validation = validate_latlon(lat, lon, conus=True)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        # TODO: make a specific bad request template for CONUS: the existing one is specific to the Arctic!
        # for now, just render the 400 template
        return render_template("400/bad_request.html"), 422
        # return render_template("422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX), 422

    # get the HUC6 polygon
    huc = get_huc_from_lat_lon(lat, lon)

    # get the features within the HUC6 polygon
    huc_segments = get_bbox_features_and_clip(huc)

    # build a dictionary with geometry IDs, segment names, and feature geometries
    huc6_data_dict = build_data_dict(huc, huc_segments)

    # get the hydrology statistics for geom_ids and populate the dictionary
    # huc6_data_dict = fetch_hydrology_data(huc6_data_dict)

    # TODO: figure out why this fails!
    # return Flask JSON Response
    json_results = json.dumps(huc6_data_dict, indent=4)
    return Response(response=json_results, status=200, mimetype="application/json")
