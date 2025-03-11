"""
A module of data gathering functions for use across multiple endpoints.
"""

import copy
import io
import itertools
import operator
import time
import asyncio
import numpy as np
import xarray as xr
import rasterio
import rioxarray
from rasterio.features import rasterize
import geopandas as gpd
import json
import re
from collections import defaultdict
from functools import reduce
from aiohttp import ClientSession
from flask import current_app as app

from rasterstats import zonal_stats
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
    resp = await session.request(
        method="GET", url=url, headers=cache_header, verify_ssl=True
    )
    resp.raise_for_status()

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
    """Get the Polygon Object corresponding to the ID from GeoServer. Forces EPSG 3338 if CRS not specified.
    Args:
        poly_id (str or int): ID of polygon e.g. "FWS12", or a HUC code (int).
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within.
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
    if crs == 3338:
        poly_gdf = gpd.GeoDataFrame.from_features(geometry).set_crs(4326).to_crs(crs)
        poly = poly_gdf.iloc[0]["geometry"]
    else:
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


async def fetch_bbox_data(bbox_bounds, cov_id_str):
    """Make the async request for the data at the specified bbox for a specific coverage

    Args:
        bbox_bounds (tuple): 4-tuple of x,y lower/upper bounds: (<xmin>,<ymin>,<xmax>,<ymax>)
        cov_id_str (str): shared portion of coverage_ids to query

    Returns:
        list of data results from each of historical and future coverages
    """
    # set up WCS request strings
    request_strs = []
    request_strs.append(generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id_str))
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    bbox_ds_list = await fetch_bbox_netcdf_list(urls)
    return bbox_ds_list


def get_scale_factor(grid_cell_area, polygon_area):
    """Calculate the scale factor for a given grid cell area and polygon area. Inputs must be in the same units.
    Args:
        grid_cell_area (float): area of a grid cell
        polygon_area (float): area of a polygon
    Returns:
        int: scale factor, rounded up to the nearest integer
    """

    def hyp_function(x, m, b, c, h):
        y = (m * x + b) / (x - c) + h
        return y

    x = polygon_area / grid_cell_area
    m = 0
    b = 350
    c = -24
    h = 1

    return int(np.ceil(hyp_function(x, m, b, c, h)))


def interpolate(ds, var_name, x_dim, y_dim, scale_factor, method):
    """Interpolate the array for a single variable from an xarray dataset to a higher resolution.

    Args:
        ds (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        var_name (str): name of the variable to interpolate
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
        scale_factor (int): multiplier to increase the resolution by
        method (str): method to use for interpolation

    Returns:
        ds_new (xarray.DataArray): xarray data array interpolated to higher resolution
    """
    X = x_dim
    Y = y_dim

    new_lon = np.linspace(ds[X][0].item(), ds[X][-1].item(), ds.sizes[X] * scale_factor)
    new_lat = np.linspace(ds[Y][0].item(), ds[Y][-1].item(), ds.sizes[Y] * scale_factor)

    da_i = ds[var_name].interp(method=method, coords={X: new_lon, Y: new_lat})

    return da_i


def rasterize_polygon(da_i, x_dim, y_dim, polygon):
    """Rasterize a polygon to the same shape as the dataset.
    Args:
        da_i (xarray.DataArray): xarray data array, probably interpolated
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
        polygon (GeoDataFrame): geodataframe with a single polygon to rasterize. Must be in the same CRS as the dataset.
    Returns:
        rasterized_polygon_array (numpy.ndarray): 2D numpy array with the rasterized polygon
    """

    rasterized_polygon_array = rasterize(
        [
            (polygon.geometry.iloc[0].geoms[0], 1)
        ],  # allows for multipolygon geometries, but will only capture the first polygon
        out_shape=(
            da_i[y_dim].values.shape[0],
            da_i[x_dim].values.shape[0],
        ),  # must be YX order for numpy array!
        transform=da_i.rio.transform(
            recalc=True
        ),  # must recalc since we interpolated, otherwise the old stored transform is used and rasterized polygon is not aligned
        fill=0,
        all_touched=False,
    )

    return rasterized_polygon_array


def calculate_zonal_stats(da_i, poly_array):
    """Calculate zonal statistics for an xarray data array and a rasterized polygon array of the same shape.
    Args:
        da_i (xarray.DataArray): xarray data array, probably interpolated
        poly_array (numpy.ndarray): 2D numpy array with the rasterized polygon
    Returns:
        zonal_stats (dict): dictionary of zonal statistics
    """
    zonal_stats = {}

    values = da_i.values[poly_array == 1].tolist()

    if values:
        zonal_stats["mean"] = np.nanmean(values)
        zonal_stats["count"] = len(values)
    else:
        zonal_stats["mean"] = np.nan
        zonal_stats["count"] = 0

    return zonal_stats


def interpolate_and_compute_zonal_stats(
    polygon, dataset, var_name="Gray", x_dim="X", y_dim="Y"
):
    """Interpolate a dataset to a higher resolution and compute polygon zonal statistics for a single variable.
    Args:
        polygon (GeoDataFrame): geodataframe with a single polygon to rasterize. Must be in the same CRS as the dataset.
        dataset (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        var_name (str): name of the variable to interpolate. Default is "Gray", the default name used when ingesting into Rasdaman.
        x_dim (str): name of the x dimension. Default is "X".
        y_dim (str): name of the y dimension. Default is "Y".
    Returns:
        zonal_stats_dict (dict): dictionary of zonal statistics
    """

    # calculate the scale factor, assuming square pixels and projection in meters
    dataset.rio.set_spatial_dims(x_dim, y_dim)
    spatial_resolution = dataset.rio.resolution()
    grid_cell_area_m2 = spatial_resolution[0] * spatial_resolution[1]
    polygon_area_m2 = polygon.area
    scale_factor = get_scale_factor(grid_cell_area_m2, polygon_area_m2)

    # interpolate the dataset and rasterize the polygon
    da_i = interpolate(dataset, var_name, x_dim, y_dim, scale_factor, method="nearest")
    rasterized_polygon_array = rasterize_polygon(da_i, x_dim, y_dim, polygon)

    # calculate zonal statistics
    zonal_stats_dict = calculate_zonal_stats(da_i, rasterized_polygon_array)

    return zonal_stats_dict


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

    crop_shape = data_arr[0].shape
    cropped_poly_mask = poly_mask_arr[0 : crop_shape[0], 0 : crop_shape[1]]
    data_arr_mask = np.broadcast_to(cropped_poly_mask.mask, data_arr.shape)
    data_arr[data_arr_mask] = np.nan

    # Set any remaining nodata values to nan if they snuck through the mask.
    data_arr[np.isclose(data_arr, -9.223372e18)] = np.nan

    results = np.nanmean(data_arr, axis=(1, 2)).astype(float)

    for map_list, result in zip(dim_combos, results):
        get_from_dict(aggr_results, map_list[:-1])[map_list[-1]] = round(
            result, dim_encodings["rounding"][roundkey]
        )
    return aggr_results


def get_poly_mask_arr(ds, poly, bandname):
    """Get the polygon mask array from an xarray dataset, intended to be recycled for rapid zonal summary across results from multiple WCS requests for the same bbox. Wrapper for rasterstats zonal_stats().

    Args:
        ds (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        poly (shapely.Polygon): polygon to create mask from
        bandname (str): name of the DataArray containing the data

    Returns:
        cropped_poly_mask (numpy.ma.core.MaskedArra): a masked array masking the cells
            intersecting the polygon of interest, cropped to the right shape
    """
    # need a data layer of same x/y shape just for running a zonal stats
    xy_shape = ds[bandname].values.shape[-2:]
    data_arr = np.zeros(xy_shape)
    # get affine transform from the xarray.DataSet
    ds.rio.set_spatial_dims("X", "Y")
    transform = ds.rio.transform()
    poly_mask_arr = zonal_stats(
        poly,
        data_arr,
        affine=transform,
        nodata=np.nan,
        stats=["mean"],
        raster_out=True,
    )[0]["mini_raster_array"]
    cropped_poly_mask = poly_mask_arr[0 : xy_shape[1], 0 : xy_shape[0]]
    return cropped_poly_mask


def geotiff_zonal_stats(poly, arr, nodata_value, transform, stat_list):
    poly_mask_arr = zonal_stats(
        poly,
        arr,
        affine=transform,
        nodata=nodata_value,
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
