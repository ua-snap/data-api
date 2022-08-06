"""
A module to validate latitude and longitude, contains
other functions that could be used across multiple endpoints.
"""

import os
import re
import json
from flask import render_template
from pyproj import Transformer
import numpy as np
from config import WEST_BBOX, EAST_BBOX
from luts import valid_huc_ids


def validate_latlon(lat, lon):
    """Validate the lat and lon values.
    Return True if valid or HTTP status code if validation failed
    """
    try:
        lat_float = float(lat)
        lon_float = float(lon)
    except:
        return 400  # HTTP status code
    lat_in_world = -90 <= lat_float <= 90
    lon_in_world = -180 <= lon_float <= 180
    if not lat_in_world or not lon_in_world:
        return 400  # HTTP status code

    # Validate against two different BBOXes to deal with antimeridian issues
    for bbox in [WEST_BBOX, EAST_BBOX]:
        valid_lat = bbox[1] <= lat_float <= bbox[3]
        valid_lon = bbox[0] <= lon_float <= bbox[2]
        if valid_lat and valid_lon:
            return True

    return 422


def validate_bbox(lat1, lon1, lat2, lon2):
    """Validate a bounding box given lat lon values
    LL: (lat1, lon), UR: (lat2, lon2)
    """
    lat_valid = lat1 < lat2
    validations = [lat_valid]

    # validate all four corners
    for lat in [lat1, lat2]:
        for lon in [lon1, lon2]:
            validations.append(validate_latlon(lat, lon))

    # Prioritize HTTP 400 errors over HTTP 422 errors
    if 400 in validations:
        return 400
    if 422 in validations:
        return 422

    valid = np.all(validations)

    return valid


def validate_seaice_year(start_year, end_year):
    if start_year is not None:
        if int(start_year) < 1850 or int(start_year) > 2021:
            return 400
    if end_year is not None:
        if int(end_year) < 1850 or int(end_year) > 2021:
            return 400

    return True


def validate_year(start_year, end_year):
    if (
        1900 < int(start_year) <= 2100
        and 1900 < int(end_year) <= 2100
        and int(start_year) <= int(end_year)
    ):
        return True
    else:
        return 400


def validate_var_id(var_id):
    if re.search("[^A-Za-z0-9]", var_id):
        return render_template("400/bad_request.html"), 400
    for filename in os.listdir("data/jsons/"):
        with open(os.path.join("data/jsons/", filename), "r") as f:
            for jsonline in json.loads(f.read()):
                if var_id == jsonline["id"]:
                    return jsonline["type"]
    return render_template("422/invalid_area.html"), 400


def project_latlon(lat1, lon1, dst_crs, lat2=None, lon2=None):
    """Reproject lat lon coords

    Args:
        lat1 (float): latitude (single point) or southern bound (bbox)
        lon1 (float): longitude (single point) or western bound (bbox)
        dst_crs (int): EPSG code for the destination
            coordinate reference system
        lat2 (float): northern bound (bbox)
        lon2 (float): eastern bound (bbox)

    Returns:
        Reprojected coordinates in order x, y
    """
    transformer = Transformer.from_crs(4326, dst_crs)

    if lat2 is None:
        projected_coords = transformer.transform(lat1, lon1)
    else:
        x1, y1 = transformer.transform(lat1, lon1)
        x2, y2 = transformer.transform(lat2, lon2)
        projected_coords = (x1, y1, x2, y2)

    return projected_coords
