"""
A module to validate latitude and longitude, contains
other functions that could be used across multiple endpoints.
"""

from pyproj import Transformer
import numpy as np
from config import VALID_BBOX


def validate(lat, lon):
    """Validate the lat and lon values,
    return bool for validity
    """
    try:
        lat_in_ak_bbox = VALID_BBOX[1] <= float(lat) <= VALID_BBOX[3]
        lon_in_ak_bbox = VALID_BBOX[0] <= float(lon) <= VALID_BBOX[2]
        valid = lat_in_ak_bbox and lon_in_ak_bbox
    except ValueError:
        valid = False
    return valid


def validate_bbox(lat1, lon1, lat2, lon2):
    """Validate a bounding box given lat lon values
    LL: (lat1, lon), UR: (lat2, lon2)
    """
    lat_valid = lat1 < lat2
    # validate all four corners
    ll = validate(lat1, lon1)
    lr = validate(lat1, lon2)
    ul = validate(lat2, lon1)
    ur = validate(lat2, lon2)

    valid = np.all([lat_valid, ll, lr, ul, ur])

    return valid


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
