"""
A module of data gathering functions for use across multiple endpoints.
"""

import copy
import io
import logging
import operator
import time
import asyncio
import xarray as xr
import geopandas as gpd
import json
import re
import ast
import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import defaultdict
from functools import reduce
from aiohttp import ClientSession
from flask import current_app as app

from generate_requests import (
    generate_wcs_getcov_str,
    generate_netcdf_wcs_getcov_str,
    generate_wcps_describe_coverage_str,
)
from generate_urls import (
    generate_wcs_query_url,
    generate_base_wms_url,
    generate_base_wfs_url,
    generate_wms_and_wfs_query_urls,
    generate_wfs_places_url,
    generate_describe_coverage_url,
)

logger = logging.getLogger(__name__)

required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
db_env_var_missing = [var for var in required_vars if not os.getenv(var)]

if db_env_var_missing:
    error_msg = (
        f"Missing required environment variables: {', '.join(db_env_var_missing)}"
    )
    logger.error(error_msg)
    raise ValueError(error_msg)


def get_landslide_db_connection():
    """
    Create a database connection using environment variables.
    Returns psycopg2 connection object.
    """

    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=5432,
        )
        return connection
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def get_landslide_db_row(place_name):
    """
    Fetch landslide data row for a specific place from the database.

    Args:
        place_name (str): The name of the place

    Returns:
        list: Query results from the database
    """
    connection = get_landslide_db_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            query = """
                SELECT * FROM landslide_risk 
                WHERE place_name = %s
                ORDER BY ts DESC
                LIMIT 1
            """

            cursor.execute(query, (place_name.capitalize(),))
            results = cursor.fetchall()
            return results
    except Exception as exc:
        logger.error(f"Database query failed: {exc}")
        raise exc
    finally:
        connection.close()


async def fetch_wcs_point_data(x, y, cov_id, var_coord=None):
    """Create the async request for data at the specified point.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_id (str): Rasdaman coverage ID
        var_coord (int): coordinate value corresponding to variable name to query,
            default=None will include all variables

    Returns:
        Data results from fetch_data()
    """
    urls = []
    request_str = generate_wcs_getcov_str(x, y, cov_id, var_coord)
    url = generate_wcs_query_url(request_str)
    urls.append(url)
    point_data = await fetch_data(urls)
    return point_data


async def fetch_layer_data(url, session, encoding="json"):
    """Make an awaitable GET request to a URL, return json
    or netcdf

    Args:
        url (str): WCS query URL
        session (aiohttp.ClientSession): the client session instance
        encoding (str): either "json" or "netcdf", specifying the encoding type

    Returns:
        Query result, deocded differently depending on encoding argument.
    """
    logger.info(f"Making HTTP request: GET {url}")
    start_time = time.time()
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    duration = time.time() - start_time
    logger.info(f"HTTP request completed in {duration:.2f}s: GET {url}")

    if encoding == "json":
        data = await resp.json()
    elif encoding == "netcdf":
        data = await resp.read()

    return data


async def fetch_geoserver_data(backend, workspace, wms_targets, wfs_targets, lat, lon):
    """Generic Data API for GeoServer queries - gather all async requests
    for specified data layers in a GeoServer workspace."""
    base_wms_url = generate_base_wms_url(backend, workspace, lat, lon)
    base_wfs_url = generate_base_wfs_url(backend, workspace, lat, lon)
    urls = generate_wms_and_wfs_query_urls(
        wms_targets, base_wms_url, wfs_targets, base_wfs_url
    )

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


async def make_get_request(url, session):
    """Make an awaitable GET request to a URL, return json
    or netcdf - duplicate of fetch_layer_data for now

    Args:
        url (str): WCS query URL
        session (aiohttp.ClientSession): the client session instance

    Returns:
        Query result, deocded differently depending on encoding argument.
    """
    cache_header = {"Cache-Control": "max-age=7776000"}
    logger.info(f"Making HTTP request: GET {url}")
    start_time = time.time()
    resp = await session.request(
        method="GET", url=url, headers=cache_header, verify_ssl=True
    )
    resp.raise_for_status()
    duration = time.time() - start_time
    logger.info(f"HTTP request completed in {duration:.2f}s: GET {url}")

    # way of auto-detecting encoding from URL
    if "application/json" in url:
        # If response has nans, attempting to parse as JSON will fail.
        # If this happens, replace nans with -9999 and try again.
        try:
            data = await resp.json()
        except json.JSONDecodeError as e:
            content = await resp.read()
            json_str = content.decode("utf-8")
            json_str = replace_nans(json_str)
            data = json.loads(json_str)
    elif "application/netcdf" in url:
        data = await resp.read()
    elif "GeoTIFF" in url:
        data = await resp.read()
    elif "DescribeCoverage" in url:
        # DescribeCoverage in URL ==> XML coming back
        data = await resp.text()
    else:
        # Only here when requesting a URL within the API.
        # Used by eds.py to return compiled JSON for all
        # ArcticEDS plates.
        data = await resp.json()

    return data


async def fetch_data(urls):
    """Wrapper for make_get_request() which gathers and
    executes the urls as asyncio tasks

    Args:
        urls (list): list of URLs as strings

    Returns:
        Results of query(ies) as either bytes or json
    """
    if len(urls) == 1:
        async with ClientSession() as session:
            results = await asyncio.create_task(make_get_request(urls[0], session))
    else:
        # not used yet
        async with ClientSession() as session:
            tasks = [make_get_request(url, session) for url in urls]
            results = await asyncio.gather(*tasks)

    return results


def get_poly(poly_id, crs=3338):
    """Get the GeoDataFrame corresponding to the polygon ID from GeoServer.
    Assumes GeoServer polygon is in EPSG:4326; returns in EPSG:3338 if CRS is not specified.
    Args:
        poly_id (str or int): ID of polygon e.g. "FWS12", or a HUC code (int).
        crs (int): EPSG CRS code
    Returns:
        poly (GeoDataFrame): GeoDataFrame of the polygon
    """
    geometry = asyncio.run(
        fetch_data(
            [
                generate_wfs_places_url(
                    "all_boundaries:all_areas",
                    "the_geom",
                    poly_id,
                    "id",
                )
            ]
        )
    )

    poly = gpd.GeoDataFrame.from_features(geometry).set_crs(4326).to_crs(crs)

    return poly


async def fetch_bbox_geotiff_from_gs(url):
    """Make the async request for GeoTIFF data within the specified bbox

    Args:
        url (str): URL for a WCS query to GeoServer
    Returns:
        geotiff: result of WCS GeoTIFF query
    """
    start_time = time.time()
    geotiff_bytes = await fetch_data(url)
    app.logger.info(
        f"Fetched BBOX data from GeoServer, elapsed time {round(time.time() - start_time)}s"
    )

    # create geotiff source from bytestring
    geotiff = io.BytesIO(geotiff_bytes)
    return geotiff


async def fetch_bbox_netcdf(url):
    """Make the async request for the data within the specified bbox

    Args:
        url (str): URL containing WCS request for bbox in netcdf format

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    start_time = time.time()
    netcdf_bytes = await fetch_data(url)
    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )
    # create xarray.DataSet from bytestring
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes))
    return ds


async def fetch_bbox_netcdf_list(urls):
    """Make the async request for the data within the specified bbox

    Args:
        urls (list): list of URL containing WCS request for bbox in netcdf format

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    start_time = time.time()
    netcdf_bytes_list = await fetch_data(urls)

    if not isinstance(netcdf_bytes_list, list):
        netcdf_bytes_list = [netcdf_bytes_list]

    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )
    # create xarray.DataSets from bytestring list
    ds_list = [xr.open_dataset(io.BytesIO(bytestr)) for bytestr in netcdf_bytes_list]
    return ds_list


def get_all_possible_dimension_combinations(iter_coords, dim_names, dim_encodings):
    """Get all possible combinations of dimension values for a given xarray dataset.
    Providing dimension names allows combinations to be limited to a subset of dimensions (e.g., ignoring X and Y).
    Args:
        iter_coords (list): list of tuples containing all possible combinations of dimension coordinates
        dim_names (list): list of dimension names to use for combinations
        dim_encodings (dict): dictionary of dimension encodings, mapping dimension names to their respective encoding values
    Returns:
        dim_combos (list): list of lists containing the corresponding encoded values for each combination
    """
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            dim_encodings[dimname][coord] for coord, dimname in zip(coords, dim_names)
        ]
        dim_combos.append(map_list)
    return dim_combos


def generate_nested_dict(dim_combos):
    """Dynamically generate a nested dict based on the different
    dimension name combinations

    Args:
        dim_combos (list): List of lists of decoded coordinate
            values (i.e. season, model, scenario names/values)

    Returns:
        Nested dict with empty dicts at deepest levels
    """

    def default_to_regular(d):
        """Convert a defaultdict to a regular dict

        Thanks https://stackoverflow.com/a/26496899/11417211
        """
        if isinstance(d, defaultdict):
            d = {k: default_to_regular(v) for k, v in d.items()}
        return d

    nested_dict = lambda: defaultdict(nested_dict)
    di = nested_dict()
    for map_list in dim_combos:
        get_from_dict(di, map_list[:-1])[map_list[-1]] = {}

    return default_to_regular(di)


def get_from_dict(data_dict, map_list):
    """Use a list to access a nested dict

    Thanks https://stackoverflow.com/a/14692747/11417211
    """
    return reduce(operator.getitem, map_list, data_dict)


def extract_nested_dict_keys(dict_, result_list=None, in_line_list=None):
    """Extract keys of nested dictionary to list of tuples

    Args:
        dict_ (dict): nested dictionary to extract keys from
        result_list (list): leave as None
        in_line_list (list): leave as None

    Notes:
        Thanks to https://stackoverflow.com/a/62928173/11417211
    """
    is_return_list = True if result_list is None else False
    if is_return_list:
        result_list = []
    is_create_new = True if in_line_list is None else False
    for k, v in dict_.items():
        if is_create_new:
            in_line_list = []
        out_line_list = copy.deepcopy(in_line_list)
        out_line_list.append(k)
        if not isinstance(v, dict) or len(v) == 0:
            result_list.append(out_line_list)
        else:
            extract_nested_dict_keys(dict_[k], result_list, out_line_list)
    if is_return_list:
        return result_list


def deepflatten(iterable, depth=None, types=None, ignore=None):
    """Flatten a nested list of unknown length. Adapted from the "iteration_utilities" library v. 0.11.0.

    Arguments:
        iterable -- the nested iterable (e.g., list) you want to flatten

    Keyword Arguments:
        depth -- flatten the iterable up to this depth (default: {None})
        types -- types to flatten (default: {None})
        ignore -- types to not flatten (default: {None})

    Yields:
        generator for the flattened iterable
    """
    if depth is None:
        depth = float("inf")
    if depth == -1:
        yield iterable
    else:
        for x in iterable:
            if ignore is not None and isinstance(x, ignore):
                yield x
            if types is None:
                try:
                    iter(x)
                except TypeError:
                    yield x
                else:
                    yield from deepflatten(x, depth - 1, types, ignore)
            elif not isinstance(x, types):
                yield x
            else:
                yield from deepflatten(x, depth - 1, types, ignore)


def replace_nans(json_str):
    """Replace nan values in a JSON string with -9999 to allow for parsing.

    Arguments:
        json_str -- the unparsed JSON string

    Returns:
        the JSON string with 'nan' values replaced with -9999
    """
    # Match only nans that have these characters on either side of them: ,[]
    # This is to prevent matches against strings that contain 'nan' within them.
    json_str = re.sub(r"(?<=[,\[\]])nan(?=[,\[\]])", "-9999", json_str)
    return json_str


async def describe_via_wcps(cov_id):
    """Get the metadata in JSON format via a WCPS describe() query request.

    Args:
        cov_id (str): rasdaman coverage ID

    Returns:
        json_description (dict): coverage description in JSON format
    """
    req_str = generate_wcps_describe_coverage_str(cov_id)
    req_url = generate_describe_coverage_url(req_str)
    json_description = await fetch_data([req_url])
    return json_description


def get_encoding_from_axis_attributes(axis, coverage_metadata):
    """Extract the axis encoding dictionary from the coverage metadata. Assumes that the
    coverage has an axis named <axis> with an "encoding" attribute. Assumes the attribute is a string
    representation of a dictionary, so it needs to be converted to an actual dictionary.

    Designed to be used with the output of describe_via_wcps(), like so:
        coverage_metadata = asyncio.run(describe_via_wcps(cov_id))
        model_encoding = get_encoding_from_model_axis_attributes("model", coverage_metadata)

    Args:
        axis (str): name of the axis to extract encoding from (e.g., "model", "scenario")
        coverage_metadata (dict): output of describe_via_wcps()

    Returns:
        axis_encoding (dict): dictionary mapping axis coordinate values to their encoded values
    """
    if (
        "metadata" not in coverage_metadata
        or "axes" not in coverage_metadata["metadata"]
        or axis not in coverage_metadata["metadata"]["axes"]
        or "encoding" not in coverage_metadata["metadata"]["axes"][axis]
    ):
        raise ValueError(
            f"Coverage metadata does not contain the expected '{axis}' axis with 'encoding' attribute."
        )
    axis_encoding = ast.literal_eval(
        coverage_metadata["metadata"]["axes"][axis]["encoding"]
    )
    return axis_encoding


def get_variables_from_coverage_metadata(coverage_metadata):
    """Extract variable names from coverage metadata (these are called "bands" in the
    rasdaman coverage metadata.) Assumes that the coverage has a "bands" attribute, which is a
    dictionary of band metadata using the band names as keys.
    Args:
        coverage_metadata (dict): output of describe_via_wcps()
    Returns:
        var_names (list): list of variable names (bands) in the coverage."""
    if (
        "metadata" not in coverage_metadata
        or "bands" not in coverage_metadata["metadata"]
    ):
        raise ValueError(
            "Coverage metadata does not contain the expected 'bands' attribute."
        )
    var_names = list(coverage_metadata["metadata"]["bands"].keys())
    return var_names


def get_attributes_from_time_axis(coverage_metadata):
    """Extract time axis attributes from coverage metadata. Assumes that the
    coverage has an axis named "time" with "units", "min_value", and "max_value" attributes.
    This function converts the time units to a base date.

    Designed to be used with the output of describe_via_wcps(), like so:
        coverage_metadata = asyncio.run(describe_via_wcps(cov_id))
        time_units, time_min, time_max = get_attributes_from_time_axis(coverage_metadata)
    """
    if (
        "metadata" not in coverage_metadata
        or "axes" not in coverage_metadata["metadata"]
        or "time" not in coverage_metadata["metadata"]["axes"]
        or "units" not in coverage_metadata["metadata"]["axes"]["time"]
        or "min_value" not in coverage_metadata["metadata"]["axes"]["time"]
        or "max_value" not in coverage_metadata["metadata"]["axes"]["time"]
    ):
        raise ValueError(
            "Coverage metadata does not contain the expected 'time' axis with 'units', 'min_value', and 'max_value' attributes."
        )
    time_units = coverage_metadata["metadata"]["axes"]["time"]["units"]
    # Example time_units: "days since 1850-01-01 00:00:00"
    match = re.match(r"days since (\d{4})-(\d{2})-(\d{2})", time_units)
    if not match:
        raise ValueError(
            f"Unexpected format for time units: '{time_units}'. Expected format like 'days since YYYY-MM-DD HH:MM:SS'."
        )
    year, month, day = map(int, match.groups())
    base_date = datetime.datetime(year, month, day)
    time_min = int(coverage_metadata["metadata"]["axes"]["time"]["min_value"])
    time_max = int(coverage_metadata["metadata"]["axes"]["time"]["max_value"])
    return base_date, time_min, time_max


def ymd_to_cftime_value(year, month, day, base_date):
    """Convert a year, month, and day to a CF-compliant time value (days since the base date)."""
    date = datetime.datetime(year, month, day)
    delta_days = (date - base_date).days
    return delta_days


def cftime_value_to_ymd(time_value, base_date):
    """Convert a time value in days since the base date to a year, month, day tuple."""
    date = base_date + datetime.timedelta(days=time_value)
    return date.year, date.month, date.day


def get_place_data(place_id):
    """
    Get comprehensive place data for a given place ID.

    Args:
        place_id (str): place identifier (e.g., AK124, AK182)

    Returns:
        dict or None: Complete place data if found, None if not found
    """
    if place_id is None:
        return None

    if place_id in all_areas_full:
        return all_areas_full[place_id]

    if place_id in all_communities_full:
        return all_communities_full[place_id]

    return None


communities_features = asyncio.run(
    fetch_data(
        [
            generate_wfs_places_url(
                "all_boundaries:all_communities",
                "name,alt_name,id,region,country,type,latitude,longitude,tags,is_coastal,ocean_lat1,ocean_lon1",
            )
        ]
    )
)["features"]

areas_features = asyncio.run(
    fetch_data(
        [
            generate_wfs_places_url(
                "all_boundaries:all_areas",
                "id,name,type,area_type,alt_name,zone,subzone",
            )
        ]
    )
)["features"]

# Creates dictionaries mapping place IDs to their property dictionaries
# for fast lookup by community or area ID ("AK124")
all_communities_full = {
    feature["properties"]["id"]: feature["properties"]
    for feature in communities_features
}

all_areas_full = {
    feature["properties"]["id"]: feature["properties"] for feature in areas_features
}
