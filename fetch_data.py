"""
A module of data gathering functions for use across multiple endpoints.
"""
import csv
import copy
import io
import itertools
import json
import operator
import time
import asyncio
import xml.etree.ElementTree as ET
import numpy as np
import rasterio as rio
import xarray as xr
from collections import defaultdict
from functools import reduce
from aiohttp import ClientSession
from flask import current_app as app, Response
from rasterstats import zonal_stats
from config import RAS_BASE_URL, WEB_APP_URL
from generate_requests import *
from generate_urls import *
from luts import json_types


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
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()

    if encoding == "json":
        data = await resp.json()
    elif encoding == "netcdf":
        data = await resp.read()

    return data


async def fetch_data_api(backend, workspace, wms_targets, wfs_targets, lat, lon):
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
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()

    # way of auto-detecting encoding from URL
    if "application/json" in url:
        data = await resp.json()
    elif "application/netcdf" in url:
        data = await resp.read()
    elif "GeoTIFF" in url:
        data = await resp.read()
    elif "DescribeCoverage" in url:
        # DescribeCoverage in URL ==> XML coming back
        data = await resp.text()

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
    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )
    # create xarray.DataSets from bytestring list
    ds_list = [xr.open_dataset(io.BytesIO(bytestr)) for bytestr in netcdf_bytes_list]
    return ds_list


def summarize_within_poly(ds, poly, dim_encodings, varname="Gray", roundkey="Gray"):
    """Summarize a single Data Variable of a xarray.DataSet within a polygon.
    Return the results as a nested dict.

    Args:
        ds (xarray.DataSet): DataSet with "Gray" as variable of
            interest
        poly (shapely.Polygon): polygon within which to summarize
        dim_encodings (dict): nested dictionary of thematic key value pairs that chacterize the data and map integer data coordinates to models, scenarios, variables, etc.
        varname (str): name of variable represented by ds
        roundkey (str): variable key that will fetch an integer that determines rounding precision (e.g. 1 for a single decimal place)

    Returns:
        Nested dict of results for all non-X/Y axis combinations,

    Notes:
        This default "Gray" is used because it is the default name for ingesting into Rasdaman from GeoTIFFs. Othwerwise it should be the name of a xarray.DataSet DataVariable, i.e. something in `list(ds.keys())`
    """
    # will actually operate on underlying DataArray

    da = ds[varname]
    # get axis (dimension) names and make list of all coordinate combinations
    all_dims = da.dims
    dimnames = [dimname for dimname in all_dims if dimname not in ("X", "Y")]
    iter_coords = list(
        itertools.product(*[list(ds[dimname].values) for dimname in dimnames])
    )

    # generate all combinations of decoded coordinate values
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            # dim_encodings[dimname][coord]
            dim_encodings[f"{dimname}s"][coord]
            for coord, dimname in zip(coords, dimnames)
        ]
        dim_combos.append(map_list)

    aggr_results = generate_nested_dict(dim_combos)

    data_arr = []
    for coords, map_list in zip(iter_coords, dim_combos):
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        data_arr.append(da.sel(sel_di).values)
    data_arr = np.array(data_arr)

    # need to transpose the 2D spatial slices if X is the "rows" dimension
    if all_dims.index("X") < all_dims.index("Y"):
        data_arr = data_arr.transpose(0, 2, 1)

    # get transform from a DataSet
    ds.rio.set_spatial_dims("X", "Y")
    transform = ds.rio.transform()
    poly_mask_arr = zonal_stats(
        poly,
        data_arr[0],
        affine=transform,
        nodata=np.nan,
        stats=["mean"],
        raster_out=True,
    )[0]["mini_raster_array"]

    data_arr_mask = np.broadcast_to(poly_mask_arr.mask, data_arr.shape)
    data_arr[data_arr_mask] = np.nan
    results = np.nanmean(data_arr, axis=(1, 2)).astype(float)

    for map_list, result in zip(dim_combos, results):
        get_from_dict(aggr_results, map_list[:-1])[map_list[-1]] = round(
            result, dim_encodings["rounding"][roundkey]
        )
    return aggr_results


def geotiff_zonal_stats(poly, arr, transform, stat_list):
    poly_mask_arr = zonal_stats(
        poly,
        arr,
        affine=transform,
        nodata=np.nan,
        stats=stat_list,
    )
    return poly_mask_arr


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


def parse_meta_xml_str(meta_xml_str):
    """Parse the DescribeCoverage request to get the XML and
    restructure to dict

    Args:
        meta_xml_str (str): decoded text XML response from
            DescribeCoverage WCS query
    """
    meta_xml = ET.ElementTree(ET.fromstring(meta_xml_str))
    # wow xml
    dim_encodings = eval(
        list(
            list(
                list(
                    list(
                        meta_xml.getroot()[0].iter(
                            "{http://www.opengis.net/gmlcov/1.0}metadata"
                        )
                    )[0].iter("{http://www.opengis.net/gmlcov/1.0}Extension")
                )[0].iter("{http://www.rasdaman.org}covMetadata")
            )[0].iter("Encoding")
        )[0].text
    )
    # make the coordinate value keys integers
    for axis, encoding_di in dim_encodings.items():
        dim_encodings[axis] = {int(k): v for k, v in encoding_di.items()}
    return dim_encodings


async def get_dim_encodings(cov_id):
    """Get the dimension encodings from a rasdaman
    coverage that has the encodings stored in an
    "encodings" attribute

    Args:
        cov_id (str): ID of the rasdaman coverage

    Rreturns:
        dict of encodings, with axis name as keys holding
        dicts of integer-keyed categories
    """
    meta_url = generate_wcs_query_url(f"DescribeCoverage&COVERAGEID={cov_id}")
    meta_xml_str = await fetch_data([meta_url])
    dim_encodings = parse_meta_xml_str(meta_xml_str)
    return dim_encodings


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


def add_titles(packaged_data, titles):
    """
    Adds title fields to a JSONlike data package and returns it.
    Args:
        packaged_data (json): JSONlike data package output
            from the run_fetch_* and run_aggregate_* functions
        titles (list, str): title or list of titles to add to the data package

    Returns:
        data package with titles added
    """
    if titles is not None:
        if isinstance(titles, str):
            packaged_data["title"] = titles
        else:
            for key in titles.keys():
                if key in packaged_data:
                    if packaged_data[key] is not None:
                        packaged_data[key]["title"] = titles[key]
    return packaged_data


def build_csv_dicts(packaged_data, package_coords, fill_di=None):
    """
    Returns a list of dicts to be written out later as a CSV.
    Args:
        packaged_data (json): JSONlike data package output
            from the run_fetch_* and run_aggregate_* functions
        package_coord (list): list of string values corresponding to
            levels of the packaged_data dict. Should be a subset of fieldnames arg.
        fill_di (dict): dict to fill in columns with fixed values.
            Keys should specify the field name and value should be the
            value to fill

    Returns:
        list of dicts with keys/values corresponding to fieldnames
    """
    # extract the coordinate values stored in keys. assumes uniform structure
    # across entire data package (i.e. n levels deep where n == len(fieldnames))
    data_package_coord_combos = extract_nested_dict_keys(packaged_data)
    rows = []
    for coords in data_package_coord_combos:
        row_di = {}
        # need more general way of handling fields to be inserted before or after
        # what are actually available in packaged dicts
        for field, coord in zip(package_coords, coords):
            row_di[field] = coord
        # fill in columns with fixed values if specified
        if fill_di:
            for fieldname, value in fill_di.items():
                row_di[fieldname] = value
        # write the actual value
        row_di["value"] = get_from_dict(packaged_data, coords)
        rows.append(row_di)
    return rows


def write_csv(csv_dicts, fieldnames, filename, metadata=None):
    """
    Creates and returns a downloadable CSV file from list of CSV dicts.

    Args:
        csv_dicts (list): CSV data created with build_csv_dicts function.
        fieldnames (list): list of fields used to create CSV header row
        filename (str): File name of downloaded CSV file

    Returns:
        CSV Response
    """
    output = io.StringIO()
    if metadata is not None:
        output.write(metadata)
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_dicts)
    response = Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Type": 'text/csv; name="' + filename + '"',
            "Content-Disposition": 'attachment; filename="' + filename + '"',
        },
    )
    return response


def place_name(place_type, place_id):
    """
    Determine if provided place_id corresponds to a known place.

    Args:
        place_type (str): point, huc, or pa
        place_id (str): place identifier (e.g., AK124)

    Returns:
        Name of the place if it exists, otherwise None
    """
    if place_type == "point":
        f = open(json_types["communities"], "r")
    elif place_type == "huc":
        f = open(json_types["huc8s"], "r")
    elif place_type == "pa":
        f = open(json_types["protected_areas"], "r")
    else:
        return None

    places = json.load(f)
    f.close()

    for place in places:
        if place_id == place["id"]:
            return place["name"]

    return None


def csv_metadata(place, place_id, place_type, lat=None, lon=None):
    """
    Creates metadata string to add to beginning of CSV file.

    Args:
        place (str): Name of the place, None of just lat/lon
        place_id (str): place identifier (e.g., AK124)
        place_type (str): point, huc, or pa
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons

    Returns:
        Multiline metadata string
    """
    metadata = "# Location: "
    if place is not None:
        metadata += place + " "

    if place is None:
        metadata += lat + " " + lon
    elif place_type == "point":
        metadata += "(" + place_id + ") " + lat + " " + lon
    elif place_type == "huc":
        metadata += "(HUC " + place_id + ")"
    elif place_type == "pa":
        metadata += "(" + place_id + ")"
    metadata += "\n"

    if place_type == "pa":
        place_path = "protected_area"
    elif place_type == "point" and place is not None:
        place_path = "community"
    else:
        place_path = place_type

    report_url = WEB_APP_URL + "report/"
    if place is None:
        report_url += lat + "/" + lon
    else:
        report_url += place_path + "/" + place_id
    metadata += "# View a report for this location at " + report_url + "\n"

    return metadata
