# A module to validate latitude and longitude
# This function will get used across a variety of endpoints.


def validate(lat, lon):
    """Validate the lat and lon values,
    return bool for validity"""
    try:
        lat_in_ak_bbox = 51.229 <= float(lat) <= 71.3526
        lon_in_ak_bbox = -179.1506 <= float(lon) <= -129.9795
        valid = lat_in_ak_bbox and lon_in_ak_bbox
    except ValueError:
        valid = False
    return valid
