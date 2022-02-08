"""
A module of data gathering functions for use across multiple endpoints.
"""
import io
import itertools
import operator
import time
import asyncio
import numpy as np
import rasterio as rio
import xarray as xr
from collections import defaultdict
from functools import reduce
from aiohttp import ClientSession
from flask import current_app as app
from rasterstats import zonal_stats
from config import RAS_BASE_URL
from generate_requests import *
from generate_urls import *


async def fetch_wcs_point_data(x, y, cov_id, var_coord=None):
    """Create the async request for data at the specified point.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
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
    elif "application/netcdf":
        data = await resp.read()

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
        src containing results of WCS GeoTIFF query
    """
    start_time = time.time()
    geotiff_bytes = await fetch_data(url)
    app.logger.info(
        f"Fetched BBOX data from GeoServer, elapsed time {round(time.time() - start_time)}s"
    )

    # create rasterio source from bytestring
    return io.BytesIO(geotiff_bytes)
    # with rio.open as src:
    #    return src


async def fetch_bbox_netcdf(url):
    """Make the async request for the data within the specified bbox

    Args:
        y2 (float): upper y-coordinate bound
        var_coord (int): coordinate value corresponding to varname to query
        cov_ids (str): list of Coverage ids to fetch the same bbox over
        summary_decades (list): list of either None or 2-tuples of integers
            mapped to desired range of decades to summarise over,
            e.g. (6, 8) for 2070-2099. List items need to
            correspond to items in cov_ids.

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

    #
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
