"""A module to validate fetched data values."""
import json
import re
from flask import render_template
from luts import json_types

from fetch_data import add_titles

nodata_values = {
    "alfresco": [-9999],
    "degree_days": [-9999],
    "elevation": [-9999],
    "fire": [-9999],
    "forest": [65535],
    "geology": [],
    "glacier": [],
    "mean_annual_precip": [-9999],
    "permafrost": [-9999, -9999.0],
    "physiography": [],
    "taspr": [-9999, -9.223372e18, -9.223372036854776e18],
    "snow": [-9999],
    "seaice": [120, 254, 255],
    "landfastice": [0],
    "beetles": [0]
}


def nullify_nodata_value(value, endpoint):
    """Return None if a nodata value is detected, otherwise return the original value.

    Args:
        value: Original value
    Returns:
        value: The original value or None if a nodata value was detected
    """
    if str(value) in map(str, nodata_values[endpoint]):
        return None
    return value


def nullify_nodata(data, endpoint):
    """Traverse data dict recursively to convert nodata values to None.

    Args:
        data (dict): Results dict
    Returns:
        nullified (dict): The same results dict with nodata values set to None
    """
    if isinstance(data, list):
        return list(map(lambda x: nullify_nodata(x, endpoint), data))
    if isinstance(data, tuple):
        return tuple(map(lambda x: nullify_nodata(x, endpoint), data))
    if isinstance(data, dict):
        return dict(map(lambda x: nullify_nodata(x, endpoint), data.items()))

    nullified = nullify_nodata_value(data, endpoint)
    return nullified


def prune_nodata_dict(data):
    """Traverse dict recursively and prune empty or None branches.

    Args:
        data (dict): Dict with nodata values set to None
    Returns:
        pruned (dict): The same dict with empty and None branches pruned
    """
    pruned = {}
    for key, value in data.items():
        pruned[key] = prune_nodata(value)

    if any(value is not None for value in pruned.values()):
        return pruned

    return None


def prune_nodata_list(data):
    """Traverse list recursively and prune empty or None branches.

    Args:
        data (list): List with nodata values set to None
    Returns:
        pruned (list): The same list with empty and None branches pruned
    """
    pruned = []
    for value in data:
        if type(value) in [list, dict, tuple]:
            pruned_value = prune_nodata(value)
            if len(pruned_value) > 0:
                pruned.append(pruned_value)

    return pruned


def prune_nodata(data):
    """Traverse data structure recursively and prune empty or None branches.

    Args:
        data (dict, list): Data structure with nodata values set to None
    Returns:
        (dict): The same data with empty and None branches pruned
    """
    if isinstance(data, dict):
        return prune_nodata_dict(data)

    if isinstance(data, list):
        return prune_nodata_list(data)

    return data


def nullify_and_prune(data, endpoint):
    """Filter nodata values, prune empty branches, and return data"""
    nullified_data = nullify_nodata(data, endpoint)
    pruned_data = prune_nodata(nullified_data)
    return pruned_data


def postprocess(data, endpoint, titles=None):
    """Nullify and prune data, add titles, and return 404 if appropriate"""
    pruned_data = nullify_and_prune(data, endpoint)
    if pruned_data in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    if titles is not None:
        pruned_data = add_titles(pruned_data, titles)

    return pruned_data


def recursive_rounding(keys, values):
    to_return = {}
    for key, value in zip(keys, values):
        if isinstance(value, dict):
            rounded_value = recursive_rounding(value.keys(), value.values())
        elif isinstance(value, (tuple, list)):
            rounded_value = [round_by_type(x) for x in value]
        else:
            rounded_value = round_by_type(value)
        to_return[round_by_type(key)] = rounded_value
    return to_return


def round_by_type(to_round, round_amount=7):
    if isinstance(to_round, (int, float)):
        return round(to_round, round_amount)
    elif isinstance(to_round, (list, tuple)):
        return [round_by_type(x) for x in to_round]
    return to_round


def get_poly_3338_bbox(gdf, poly_id):
    """Get the Polygon Object corresponding to the the ID for a GeoDataFrame

    Args:
        gdf (geopandas.GeoDataFrame object): polygon features
        poly_id (str or int): ID of polygon e.g. "FWS12", or a HUC code (int).
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within.
        Inlcudes a 4-tuple (poly.bounds) of the bounding box enclosing the HUC
        polygon. Format is (xmin, ymin, xmax, ymax).
    """
    poly_gdf = gdf.loc[[poly_id]][["geometry"]].to_crs(3338)
    poly = poly_gdf.iloc[0]["geometry"]
    return poly


def is_di_empty(di):
    if len(di) == 0:
        return 404  # http status code


def place_name_and_type(place_id):
    """
    Determine if provided place_id corresponds to a known place.

    Args:
        place_id (str): place identifier (e.g., AK124)

    Returns:
        Name and type of the place if it was found, otherwise None and None
    """

    if place_id is None:
        return None, None

    # HUC12s, not getting names from them below
    if (not re.search("[^0-9]", place_id)) and (len(place_id) == 12):
        return None, "huc12"

    place_types = list(json_types.keys())
    place_types.remove("hucs")
    place_types.remove("huc12s")

    for place_type in place_types:
        f = open(json_types[place_type], "r")
        places = json.load(f)
        f.close()

        for place in places:
            if place_id == place["id"]:
                full_place = place["name"]
                if "alt_name" in place and place["alt_name"] is not None:
                    full_place += " (" + place["alt_name"] + ")"
                return full_place, place_type

    return None, None
