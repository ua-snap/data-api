"""
A module to validate latitude and longitude, contains
other functions that could be used across multiple endpoints.
"""

from pyproj import Transformer


def validate(lat, lon):
    """Validate the lat and lon values,
    return bool for validity
    """
    try:
        lat_in_ak_bbox = 51.229 <= float(lat) <= 71.3526
        lon_in_ak_bbox = -179.1506 <= float(lon) <= -129.9795
        valid = lat_in_ak_bbox and lon_in_ak_bbox
    except ValueError:
        valid = False
    return valid


def reproject_latlon(lat, lon, dst_crs):
    """Reproject lat lon coords

    Args:
        lat (float): latitude
        lon (float): longitude
        dst_crs (int): EPSG code for the destination
            coordinate reference system

    Returns:
        Reprojected coordinates in order x, y
    """
    transformer = Transformer.from_crs(4326, dst_crs)

    return transformer.transform(lat, lon)
