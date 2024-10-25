# import asyncio
# import numpy as np
import xarray as xr
import io
import geopandas as gpd
import requests
import json
import xml.etree.ElementTree as ET
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
from generate_requests import generate_conus_hydrology_wcs_str

# from fetch_data import fetch_wcs_point_data
from validate_request import validate_latlon

#    project_latlon,
# )
# from validate_data import *
# from postprocessing import postprocess
# from csv_functions import create_csv
from config import CONUS_BBOX, GS_BASE_URL, RAS_BASE_URL
from . import routes


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

    # save json to test size of return
    with open("/home/jdpaul3/huc6.json", "w", encoding="utf-8") as f:
        json.dump(r_json, f, ensure_ascii=False, indent=4)

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

    # save json to test size of return
    with open("/home/jdpaul3/segments.json", "w", encoding="utf-8") as f:
        json.dump(r_json, f, ensure_ascii=False, indent=4)

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

    data_dict = dict(
        {"huc6": huc.huc6.loc[0], "name": huc.name.loc[0], "segments": dict({})}
    )

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
                "data_by_model": dict({}),
                "geojson": geojson,
            }
        )

        data_dict["segments"][row.seg_id_nat] = segment_dict

    return data_dict


def fetch_hydrology_data(cov_id, vars, lc, model, scenario, era):
    """
    Function to fetch hydrology data from Rasdaman.
    Args:
        coverage_id (str): Coverage ID for the hydrology data
        encoding_attr (str): Attribute name that holds dictionary of Rasdaman encodings
        vars (list): a list of variable names (e.g. ['dh3', 'dh15'])
        lc (str): Land cover type (dynamic or static)
        model (str): Model name (e.g. CCSM4)
        scenario (str): Scenario name (e.g. historical)
        era (str): Era name (e.g. 1976_2005)
    Returns:
        Xarray dataset with hydrological stats for the requested var/lc/model/scenario/era combination
    """
    lc_, model_, scenario_, era_ = encode_parameters(cov_id, lc, model, scenario, era)
    # TODO: use RAS_BASE_URL config env variable instead of hardcoded URL
    url = "https://zeus.snap.uaf.edu/rasdaman/" + generate_conus_hydrology_wcs_str(
        cov_id, vars, lc_, model_, scenario_, era_
    )

    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        ds = xr.open_dataset(io.BytesIO(r.content))

    # save nc to test size of return
    ds.to_netcdf("/home/jdpaul3/stats.nc", engine="h5netcdf")

    return ds


def encode_parameters(cov_id, lc, model, scenario, era):
    """
    Function to encode the parameters for the Rasdaman request.
    Searches the XML response from the DescribeCoverage request for the encodings metadata and
    returns the dictionary of encodings. Encodes the input parameters to integers for the WCS request.
    Args:
        lc (str): Land cover type (dynamic or static)
        model (str): Model name (e.g. CCSM4)
        scenario (str): Scenario name (e.g. historical)
        era (str): Era name (e.g. 1976_2005)
    Returns:
        Tuple of encoded parameters (integers) for Rasdaman request"""
    # TODO: change this to 'Rasdaman Encoding' once coverage is updated
    encoding_attr = "Encoding"
    url = f"https://zeus.snap.uaf.edu/rasdaman/ows?&SERVICE=WCS&VERSION=2.1.0&REQUEST=DescribeCoverage&COVERAGEID={cov_id}&outputType=GeneralGridCoverage"
    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        tree = ET.ElementTree(ET.fromstring(r.content))

    xml_search_string = str(".//{http://www.rasdaman.org}" + encoding_attr)
    encoding_dict_str = tree.findall(xml_search_string)[0].text
    encoding_dict = eval(encoding_dict_str)

    lc_ = encoding_dict["lc"][lc]
    model_ = encoding_dict["model"][model]
    scenario_ = encoding_dict["scenario"][scenario]
    era_ = encoding_dict["era"][era]

    return lc_, model_, scenario_, era_


def populate_stats(data_dict, stats_ds, lc, model, scenario, era):
    """
    Function to populate the data dictionary with the hydrology statistics.
    Args:
        data_dict (dict): Dictionary with geometry IDs, segment names, feature geometries, and empty stats dict
        stats_ds (Xarray dataset): Xarray dataset with hydrological stats for the requested var/lc/model/scenario/era combination
    Returns:
        Updated dictionary with the hydrology statistics
    """

    # subset the dataset using segment ids from the huc6 data dictionary
    huc6_ds = stats_ds.sel(
        geom_id=stats_ds.geom_id.isin(list(data_dict["segments"].keys()))
    )

    huc6_df = huc6_ds.to_dataframe().reset_index()
    # TODO: make sure dtype is correct here if coverage is updated to ints
    huc6_df["geom_id"] = huc6_df["geom_id"].astype(int)

    # find the segment ID in the dataframe and populate the stats dict
    for segment_id in list(data_dict["segments"].keys()):
        segment_stats = huc6_df[huc6_df["geom_id"] == int(segment_id)]
        segment_stats = segment_stats.drop(columns=["geom_id"])
        data_dict["segments"][segment_id]["data_by_model"] = {
            model: {
                lc: {
                    scenario: {era: {"stats": segment_stats.to_dict(orient="records")}}
                }
            }
        }

    return data_dict


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
           example: http://localhost:5000/conus_hydrology/point/39.828/-98.5795
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

    # define the coverage and slice of data we want to get
    cov_id = "conus_hydro_segments_crstephenson"
    # TODO: handle this via additional input parameters to the function, or maybe some GET args?
    # TODO: handle multiple models, eras, etc.
    # TODO: validate input args against metadata in actual Rasdaman dataset
    vars, lc, model, scenario, era = (
        # all available stat vars
        [
            "dh3",
            "dh15",
            "dl3",
            "dl16",
            "fh1",
            "fl1",
            "fl3",
            "ma12",
            "ma13",
            "ma14",
            "ma15",
            "ma16",
            "ma17",
            "ma18",
            "ma19",
            "ma20",
            "ma21",
            "ma22",
            "ma23",
            "ra1",
            "ra3",
            "th1",
            "tl1",
        ],
        # smaller subset of stat vars for testing
        # ["dh3", "dh15", "ra1", "ra3", "ma12", "ma13"],
        "dynamic",
        "CCSM4",
        "historical",
        "1976_2005",
    )

    # get the hydrology statistics from rasdaman
    stats_ds = fetch_hydrology_data(cov_id, vars, lc, model, scenario, era)

    # populate the stats in the data dictionary with the hydrology statistics
    huc6_data_dict = populate_stats(huc6_data_dict, stats_ds, lc, model, scenario, era)

    # return Flask JSON Response
    json_results = json.dumps(huc6_data_dict, indent=4)

    # save json to test size of return
    with open("/home/jdpaul3/result.json", "w", encoding="utf-8") as f:
        json.dump(json_results, f, ensure_ascii=False, indent=4)

    return Response(response=json_results, status=200, mimetype="application/json")
