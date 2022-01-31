"""A module to validate fetched data values."""
from flask import render_template
from luts import huc8_gdf, akpa_gdf

nodata_values = {
    "fire": [-9999],
    "forest": [65535],
    "geology": [],
    "glacier": [],
    "mean_annual_precip": [-9999],
    "permafrost": [-9999, -9999.0],
    "physiography": [],
    "taspr": [-9999, -9.223372e+18, -9.223372036854776e+18],
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
        if type(value) in [list, dict, tuple]:
            pruned_value = prune_nodata(value)
            if pruned_value is not None and len(pruned_value) > 0:
                pruned[key] = pruned_value
        else:
            if value in [list, dict, tuple] and len(value) == 0:
                continue
            if value is not None:
                pruned[key] = value
    return pruned


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
            if pruned_value is not None and len(pruned_value) > 0:
                pruned.append(pruned_value)
        else:
            if value in [list, dict, tuple] and len(value) == 0:
                continue
            if value is not None:
                pruned.append(value)
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


def postprocess(data, endpoint, credits=None):
    """Filter nodata values, prune empty branches, add credits, and return 404
    if appropriate"""
    nullified_data = nullify_nodata(data, endpoint)
    pruned_data = prune_nodata(nullified_data)
    if pruned_data in [{}, None, 0]:
        return render_template("404/no_data.html"), 404
    if credits is not None:
        if isinstance(credits, str):
            nullified_data["title"] = credits
        else:
            for key, value in credits.items():
                nullified_data[key]["title"] = credits[key]
    return nullified_data


def get_poly_3338_bbox(gdf, poly_id):
    """Get the Polygon Object corresponding to the the ID for a GeoDataFrame

    Args:
        gdf (geopandas.GeoDataFrame object): polygon features
        polyid (str or int): ID of polygon e.g. "FWS12", or a HUC code (int).
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within.
        Inlcudes a 4-tuple (poly.bounds) of the bounding box enclosing the HUC
        polygon. Format is (xmin, ymin, xmax, ymax).
    """
    poly_gdf = gdf.loc[[poly_id]][["geometry"]].to_crs(3338)
    poly = poly_gdf.iloc[0]["geometry"]
    return poly
