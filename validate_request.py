"""
A module to validate request parameters such as latitude and longitude for use across multiple endpoints.
"""

import asyncio
from flask import render_template
from pyproj import Transformer
import numpy as np
from config import WEST_BBOX, EAST_BBOX, SEAICE_BBOX
from generate_urls import generate_wfs_places_url
from fetch_data import fetch_data


def latlon_is_numeric_and_in_geodetic_range(lat, lon):
    """Validate that the lat and lon values are numeric and within the decimal degree boundaries of +- 90 (lat) and +- 180 (lon).

    Args:
        lat (int or float): latitude
        lon (int or float): longitude

    Returns:
        True if valid, or HTTP 400 status code if validation failed
    """
    try:
        lat_float = float(lat)
        lon_float = float(lon)
    except:
        return 400
    lat_in_world = -90 <= lat_float <= 90
    lon_in_world = -180 <= lon_float <= 180
    if not lat_in_world or not lon_in_world:
        return 400
    return True


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


def validate_seaice_latlon(lat, lon):
    """Validate the lat and lon values for pan arctic sea ice.
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
    for bbox in [SEAICE_BBOX]:
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
    if not var_id.isalnum():
        return render_template("400/bad_request.html"), 400

    var_id_check = asyncio.run(
        fetch_data(
            [generate_wfs_places_url("all_boundaries:all_areas", "type", var_id, "id")]
        )
    )

    if var_id_check["numberMatched"] > 0:
        return var_id_check["features"][0]["properties"]["type"]
    else:
        # Search for HUC12 ID if not found in other areas
        var_id_check = asyncio.run(
            fetch_data(
                [
                    generate_wfs_places_url(
                        "all_boundaries:ak_huc12", "type", var_id, "id"
                    )
                ]
            )
        )
        if var_id_check["numberMatched"] > 0:
            return "huc12"
        else:
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


def get_x_y_axes(coverage_metadata):
    """Extract the X and Y axes from the coverage metadata.

    We're doing this because we won't always know the axis ordering and position that come from Rasdaman. They are usually the last two axes, but their exact numbering might depend on on how many axes the coverage has. So we can iterate through the axes and find the ones with the axisLabel "X" and "Y" and grab them with `next()`.

    Args:
        coverage_metadata (dict): JSON-like dictionary containing coverage metadata

    Returns:
        tuple: A tuple containing the X and Y axes metadata
    """
    try:
        x_axis = next(
            axis
            for axis in coverage_metadata["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "X"
        )
        y_axis = next(
            axis
            for axis in coverage_metadata["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "Y"
        )
        return x_axis, y_axis
    except (KeyError, StopIteration):
        raise ValueError("Unexpected coverage metadata: 'X' or 'Y' axis not found")


def validate_xy_in_coverage_extent(
    x,
    y,
    coverage_metadata,
    east_tolerance=None,
    west_tolerance=None,
    north_tolerance=None,
    south_tolerance=None,
):
    """Validate if the x and y coordinates are within the bounding box of the coverage.

    Args:
        x (float): x-coordinate
        y (float): y-coordinate
        coverage_metadata (dict): JSON-like dictionary containing coverage metadata
        east_tolerance (float): Optional tolerance to expand the bounding box to the east, will be in units native to the coverage, e.g. meters for EPSG:3338. This parameter is included to hedge against the edge: query locations where the query is within the geographic bounding box, but not the projected bounding box due to distortion at the edges of conical projections. Without this, users may experience errors because it is possible for a geographic point to be within the geographic bounding box, but the same point, projected, to be outside the projected bounding box.
        west_tolerance (float): Optional tolerance to expand the bounding box to the west
        north_tolerance (float): Optional tolerance to expand the bounding box to the north
        south_tolerance (float): Optional tolerance to expand the bounding box to the south

    Returns:
        bool: True if the coordinates are within the bounding box, False otherwise
    """
    try:
        x_axis, y_axis = get_x_y_axes(coverage_metadata)
        if west_tolerance:
            x_axis["lowerBound"] -= west_tolerance
        if east_tolerance:
            x_axis["upperBound"] += east_tolerance
        if north_tolerance:
            y_axis["upperBound"] += north_tolerance
        if south_tolerance:
            y_axis["upperBound"] -= south_tolerance

        x_in_bounds = x_axis["lowerBound"] <= x <= x_axis["upperBound"]
        y_in_bounds = y_axis["lowerBound"] <= y <= y_axis["upperBound"]
        return x_in_bounds and y_in_bounds

    except ValueError:
        return False


def construct_latlon_bbox_from_coverage_bounds(coverage_metadata):
    """Construct a bounding box from the coverage metadata.

    We use this to trigger a 422 error if the user's lat/lon is outside the coverage bounds and avoid polluting the config with hardcoded bounding boxes for each coverage.

    Args:
        coverage_metadata (dict): JSON-like dictionary containing coverage metadata

    Returns:
        list: containing the bounding box [lon_min, lat_min, lon_max, lat_max]
    """
    try:
        transformer = Transformer.from_crs(
            coverage_metadata["envelope"]["srsName"].split("/")[-1], 4326
        )
    except KeyError:
        raise ValueError(
            "Unexpected coverage metadata: Could not parse CRS via `srsName` key."
        )
    try:
        x_axis, y_axis = get_x_y_axes(coverage_metadata)
        # CP note: seems like the tuple unpacking should be reversed, but I'm matching the coordinate ordering of the example BBOXES in config.py
        lat_min, lon_min = transformer.transform(
            x_axis["lowerBound"], y_axis["lowerBound"]
        )
        lat_max, lon_max = transformer.transform(
            x_axis["upperBound"], y_axis["upperBound"]
        )
        bbox = [
            round(lon_min, 4),
            round(lat_min, 4),
            round(lon_max, 4),
            round(lat_max, 4),
        ]
        return bbox
    except ValueError:
        raise ValueError(
            "Unexpected coverage metadata: lower or upper spatial bounds not found."
        )


def validate_latlon_in_bboxes(lat, lon, bboxes):
    """Validate if a lat and lon are within a list of bounding boxes.

    Args:
        lat (float): latitude
        lon (float): longitude
        bboxes (list): list of bounding boxes in the format [lon_min, lat_min, lon_max, lat_max]
    Returns:
        bool: True if the coordinates are within the bounding boxes, else 422
    """
    lat = float(lat)
    lon = float(lon)
    for bbox in bboxes:
        valid_lat = bbox[1] <= lat <= bbox[3]
        valid_lon = bbox[0] <= lon <= bbox[2]
        if valid_lat and valid_lon:
            return True
    return 422
