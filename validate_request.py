"""
A module to validate request parameters such as latitude and longitude for use across multiple endpoints.
"""

import asyncio
import ast
import rasterio
import os.path
import os

from flask import render_template
from pyproj import Transformer
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from rasterio.crs import CRS

from config import WEST_BBOX, EAST_BBOX, SEAICE_BBOX
from generate_urls import generate_wfs_places_url
from fetch_data import fetch_data
from luts import geotiff_projections


def check_poly_in_geotiffs(polygon, coverages):
    """Check if a polygon is completely within the data footprint of binary GeoTIFFs.
    Args:
        polygon (GeoDataFrame): GeoDataFrame representing the polygon to check; if multiple polygons are present, only the first will be evaluated.
        coverages (list): List of coverages to check for data availability.
    Returns:
        True if valid or if there is a problem processing the geotiff; HTTP 404 status code if no data was found.
    """
    for coverage in coverages:
        reference_geotiff = "geotiffs/" + coverage + ".tif"

        # Skip if the GeoTIFF file does not exist.
        if not os.path.isfile(reference_geotiff):
            return True

        try:
            with rasterio.open(reference_geotiff) as dataset:
                # reproject polygon to match geotiff CRS
                if coverage in geotiff_projections:
                    crs = geotiff_projections[coverage]
                else:
                    crs = "EPSG:3338"
                polygon_proj = polygon.to_crs(crs)

                # read binary mask from geotiff, convert to uint8 for rasterio
                mask = dataset.read(1)
                mask = mask.astype("uint8")
                transform = dataset.transform

                # convert binary mask to geometries
                mask_shapes = []
                for geom, value in rasterio.features.shapes(mask, transform=transform):
                    if value == 1:
                        mask_shapes.append(shape(geom))

                # create a unified geometry from the mask shapes
                mask_geometry = gpd.GeoSeries(mask_shapes, crs=crs).unary_union

                # check if the polygon is completely within the mask geometry
                if polygon_proj.geometry.iloc[0].within(mask_geometry):
                    return True
                else:
                    return 404
        except Exception as e:
            print(f"Error processing GeoTIFF {reference_geotiff}: {e}")
            return True


def check_geotiffs(lat, lon, coverages):
    """Load a binary GeoTIFF mask corresponding to the coverage(s) requested, then use this to check if lat/lon has data available.

    Args:
        lat (int or float): latitude
        lon (int or float): longitude
        coverages (list): list of coverages to check for data availability

    Returns:
        True if valid, or HTTP 404 status code if no data was found
    """
    for coverage in coverages:
        # Use the same GeoTIFF for all CMIP6 downscaled coverages.
        if coverage.startswith("cmip6_downscaled_"):
            coverage = "cmip6_downscaled"

        reference_geotiff = "geotiffs/" + coverage + ".tif"

        # Do not perform GeoTIFF check if the file does not exist.
        if not os.path.isfile(reference_geotiff):
            return True

        # Do not perform GeoTIFF check if the file does not open properly.
        # This seems safer than the alternative of hiding data due to a corrupt file.
        try:
            with rasterio.open(reference_geotiff) as dataset:
                if coverage in geotiff_projections:
                    crs = geotiff_projections[coverage]
                else:
                    crs = "EPSG:3338"
                if crs == "EPSG:4326":
                    # check if lat/lon is within the dataset's geographic bounds
                    if (
                        dataset.bounds.left <= lon <= dataset.bounds.right
                        and dataset.bounds.bottom <= lat <= dataset.bounds.top
                    ):
                        row, col = dataset.index(lon, lat)
                        if dataset.read(1)[row, col] == 1:
                            return True
                else:
                    x, y = project_latlon(lat, lon, crs)
                    row, col = dataset.index(x, y)
                    if 0 <= row < dataset.height and 0 <= col < dataset.width:
                        if dataset.read(1)[row, col] == 1:
                            return True
        except:
            return True

    return 404


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


def validate_latlon(lat, lon, coverages=[]):
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
    within_a_bbox = False
    for bbox in [WEST_BBOX, EAST_BBOX]:
        valid_lat = bbox[1] <= lat_float <= bbox[3]
        valid_lon = bbox[0] <= lon_float <= bbox[2]
        if valid_lat and valid_lon:
            within_a_bbox = True

    if not within_a_bbox:
        return 422

    if len(coverages) > 0:
        return check_geotiffs(lat, lon, coverages)

    return True


def validate_seaice_latlon(lat, lon, coverages):
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
    within_a_bbox = False
    for bbox in [SEAICE_BBOX]:
        valid_lat = bbox[1] <= lat_float <= bbox[3]
        valid_lon = bbox[0] <= lon_float <= bbox[2]
        if valid_lat and valid_lon:
            within_a_bbox = True

    if not within_a_bbox:
        return 422

    if len(coverages) > 0:
        return check_geotiffs(lat, lon, coverages)

    return True


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

    We're doing this because we won't always know the axis ordering and position that come from Rasdaman. They are usually the last two axes, but their exact numbering might depend on on how many axes the coverage has. So we can iterate through the axes and find the ones with the axisLabel "X" and "Y" or "lon" and "lat" and grab them with `next()`.

    Args:
        coverage_metadata (dict): JSON-like dictionary containing coverage metadata

    Returns:
        tuple: A tuple containing the X and Y axes metadata
    """
    # CP note: you'll notice that we reverse the order of the return when lat-lon labels are found - this is a follow-on impact of how coordinate transformers work when we don't specify the ordering, e.g., always x-y
    try:
        x_axis = next(
            axis
            for axis in coverage_metadata["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "X" or axis["axisLabel"] == "lon"
        )
        y_axis = next(
            axis
            for axis in coverage_metadata["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "Y" or axis["axisLabel"] == "lat"
        )
        if x_axis["axisLabel"] == "lon" and y_axis["axisLabel"] == "lat":
            return y_axis, x_axis
        return x_axis, y_axis
    except (KeyError, StopIteration):
        raise ValueError("Unexpected coverage metadata: 'X' or 'Y' axis not found")


def generate_time_index_from_coverage_metadata(meta):
    """Generate a pandas DatetimeIndex from the ansi/time axis coordinates
    in the coverage description metadata.

    Args:
        meta (dict): JSON-like dictionary containing coverage metadata

    Returns:
        pd.DatetimeIndex: corresponding to the ansi (i.e. time) axis coordinates
    """
    try:
        # we won't always know the axis positioning / ordering
        ansi_axis = next(
            axis
            for axis in meta["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "ansi" or axis["axisLabel"] == "time"
        )
        # this is a list of dates formatted like "1996-11-03T00:00:00.000Z"
        ansi_coordinates = ansi_axis["coordinate"]
        date_index = pd.DatetimeIndex(ansi_coordinates)
        return date_index
    except (KeyError, StopIteration):
        raise ValueError(
            "Unexpected coverage metadata: 'ansi' or 'time' axis not found"
        )


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


def validate_latlon_in_bboxes(lat, lon, bboxes, coverages=[]):
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

    within_a_bbox = False
    for bbox in bboxes:
        valid_lat = bbox[1] <= lat <= bbox[3]
        valid_lon = bbox[0] <= lon <= bbox[2]
        if valid_lat and valid_lon:
            within_a_bbox = True

    if not within_a_bbox:
        return 422

    if len(coverages) > 0:
        return check_geotiffs(lat, lon, coverages)

    return True


def get_coverage_encodings(coverage_metadata):
    """Extract the encoding dictionary from a coverage's metadata obtained via describe_via_wcps.

    This function extracts the "Encoding" component from a coverage's metadata, which typically matches the integer values used for axis labels and positions to descriptive strings (e.g., mapping coordinate values to model names, scenarios, variables, etc.)
    Args:
        coverage_metadata (dict): JSON-like dictionary containing coverage metadata from describe_via_wcps()

    Returns:
        dict: A dictionary mapping axis names to their encoding dictionaries. Each encoding dictionary maps integer values to their descriptive strings.

    Raises:
        ValueError: If the coverage metadata doesn't contain the expected encoding information

    Example:
        >>> metadata = await describe_via_wcps("alfresco_relative_flammability_30yr")
        >>> encodings = get_coverage_encodings(metadata)
        >>> print(encodings)
        {
            'era': {0: '1950-1979', 1: '1980-2008', ...},
            'model': {0: 'MODEL-SPINUP', 2: 'GFDL-CM3', ...},
            'scenario': {0: 'historical', 1: 'rcp45', ...}
        }
    """
    try:
        # encoding **should** be in the metadata in zeroth slice
        metadata = coverage_metadata.get("metadata", {})
        slices = metadata.get("slices", {}).get("slice", [])

        if not slices:
            encoding_str = metadata.get("Encoding")
        else:
            # get encoding string from first slice (all slices contain the same encoding)
            encoding_str = slices[0].get("Encoding")

        if not encoding_str:
            raise ValueError("No encoding information found in coverage metadata")

        # convert the string representation of dict to actual dict
        try:
            encodings = ast.literal_eval(encoding_str)
        except (SyntaxError, ValueError) as e:
            raise ValueError(f"Failed to parse encoding string: {str(e)}")
        # convert string keys to ints
        for dim in encodings:
            if isinstance(encodings[dim], dict):
                encodings[dim] = {int(k): v for k, v in encodings[dim].items()}

        return encodings

    except (KeyError, TypeError) as e:
        raise ValueError(f"Invalid coverage metadata format: {str(e)}")


def get_axis_coordinate_values(coverage_metadata):
    """
    Get axis encodings from the JSON output describing a coverage.

    Args:
        coverage_axis_metadata (dict): JSON-like dictionary containing the coverage axes description.

    Returns:
        dict: A dictionary where each axis (key) maps to its respective coordinates (value).

    Raises:
        ValueError: If required information is missing in the JSON data.

    Example:
    >>> metadata = await describe_via_wcps("alfresco_relative_flammability_30yr")
    >>> coord_values = get_axis_coordinate_values(metadata)
    >>> print(coord_values)
    {
        "era": [0, 1, 2, 3, 4],
        "model": [0, 1, 2, 3, 4, 5, 6],
        "scenario": [0, 1, 2, 3]
    }
    """
    try:
        # Navigate to the generalGrid section which contains the axis coordinate values
        domain_set = coverage_metadata.get("domainSet", {})
        general_grid = domain_set.get("generalGrid", {})
        axes = general_grid.get("axis", [])

        # Extract coordinate values for each axis
        axis_coords = {}
        for axis in axes:
            axis_label = axis.get("axisLabel")
            coordinates = axis.get("coordinate", [])
            if axis_label and coordinates:
                axis_coords[axis_label] = coordinates

        if not axis_coords:
            raise ValueError(
                "No axis coordinate values found in the coverage metadata."
            )
        return axis_coords

    except (KeyError, TypeError) as e:
        raise ValueError(f"Invalid coverage metadata format: {str(e)}")


def get_axis_encodings(coverage_metadata, encoding_attr="encoding"):
    """
    Get axis encodings from the JSON output describing a coverage.
    Args:
        coverage_axis_metadata (dict): JSON-like dictionary containing the coverage axes description.
        encoding_attr (str): The attribute name that stores the encoding dictionary.
    Returns:
        dict: A dictionary with axis names as keys and their encoding attributes as values.
        Encoding attributes are coverted from string to dict.
        If an axis does not have the specified encoding attribute, it will map to None.
    """
    # Navigate to the generalGrid section which contains the axis coordinate values
    try:
        metadata = coverage_metadata.get("metadata", {})
        axes = metadata.get("axes", {})

        axis_encodings = {}

        for axis in axes:
            encoding_str = metadata.get("axes").get(axis).get(encoding_attr, None)
            # convert the string representation of dict to actual dict
            try:
                encodings = ast.literal_eval(encoding_str)
            except (SyntaxError, ValueError) as e:
                raise ValueError(f"Failed to parse encoding string: {str(e)}")
            # convert string keys to ints
            for dim in encodings:
                if isinstance(encodings[dim], dict):
                    encodings[dim] = {int(k): v for k, v in encodings[dim].items()}
            axis_encodings[axis] = encodings
    except (KeyError, TypeError) as e:
        raise ValueError(f"Invalid coverage metadata format: {str(e)}")
    return axis_encodings


def get_coverage_crs_str(coverage_metadata):
    try:
        # Navigate to the generalGrid section which contains the CRS information
        domain_set = coverage_metadata.get("domainSet", {})
        general_grid = domain_set.get("generalGrid", {})
        srs_string = general_grid.get("srsName", [])
        epsg_code_str = srs_string.split("EPSG/0/")[1]
        # get rasterio CRS object from EPSG code
        crs = CRS.from_epsg(int(epsg_code_str))
    except:
        raise ValueError(
            "Unexpected coverage metadata: 'srsName' not found or metadata otherwise invalid."
        )

    return crs.to_string()
