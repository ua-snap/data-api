"""
A module of data gathering functions for use across multiple endpoints.
"""
import copy
import io
import itertools
import operator
import time
import asyncio
import xml.etree.ElementTree as ET
import numpy as np
import xarray as xr
import geopandas as gpd
from collections import defaultdict
from functools import reduce
from aiohttp import ClientSession
from flask import current_app as app
from rasterstats import zonal_stats
from config import RAS_BASE_URL, WEB_APP_URL
from generate_requests import *
from generate_urls import *
from luts import place_type_labels


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
    cache_header = {"Cache-Control": "max-age=7776000"}
    resp = await session.request(method="GET", url=url, headers=cache_header)
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


def get_poly_3338_bbox(poly_id, crs=3338):
    """Get the Polygon Object corresponding to the ID from GeoServer

    Args:
        poly_id (str or int): ID of polygon e.g. "FWS12", or a HUC code (int).
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within.
        Includes a 4-tuple (poly.bounds) of the bounding box enclosing the HUC
        polygon. Format is (xmin, ymin, xmax, ymax).
    """
    try:
        geometry = asyncio.run(
            fetch_data(
                [
                    generate_wfs_places_url(
                        "all_boundaries:all_areas", "the_geom", poly_id, "id"
                    )
                ]
            )
        )
        if crs == 3338:
            poly_gdf = (
                gpd.GeoDataFrame.from_features(geometry).set_crs(4326).to_crs(crs)
            )
            poly = poly_gdf.iloc[0]["geometry"]
        else:
            poly = gpd.GeoDataFrame.from_features(geometry).set_crs(4326)
        return poly
    except:
        geometry = asyncio.run(
            fetch_data(
                [
                    generate_wfs_places_url(
                        "all_boundaries:ak_huc12", "the_geom", poly_id, "id"
                    )
                ]
            )
        )
        if crs == 3338:
            poly_gdf = (
                gpd.GeoDataFrame.from_features(geometry).set_crs(4326).to_crs(crs)
            )
            poly = poly_gdf.iloc[0]["geometry"]
        else:
            poly = gpd.GeoDataFrame.from_features(geometry).set_crs(4326)
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


def parse_meta_xml_str(meta_xml_str):
    """Parse the DescribeCoverage request to get the XML and
    restructure the block called "Encoding" to a dict.

    Arguments:
        meta_xml_str (str): string representation of the byte XML response from the WCS DescribeCoverage request

    Returns:
        dim_encodings (dict): lookup table to match data axes or parameters to integer encodings, e.g., '2': 'GFDL-CM3'
    """
    meta_xml = ET.ElementTree(ET.fromstring(meta_xml_str))
    # wow xml
    encoding_el = list(
        list(
            list(
                list(
                    list(
                        meta_xml.getroot().iter(
                            "{http://www.opengis.net/wcs/2.0}CoverageDescription"
                        )
                    )[0].iter("{http://www.opengis.net/gmlcov/1.0}metadata")
                )[0].iter("{http://www.opengis.net/gmlcov/1.0}Extension")
            )[0].iter("{http://www.rasdaman.org}covMetadata")
        )[0].iter("Encoding")
    )[0]

    dim_encodings = {}
    for dim in encoding_el.iter():
        if not dim.text.isspace():
            encoding_di = eval(dim.text)
            for key, value in encoding_di.items():
                if isinstance(value, dict):
                    dim_encodings[key] = {int(k): v for k, v in value.items()}
                else:
                    dim_encodings[dim.tag] = {int(k): v for k, v in encoding_di.items()}
    return dim_encodings


def get_xml_str_between_tags(meta_xml_str, tag, occurrence=1):
    """Get string encapsulated by a known XML tag. Use this function to retrieve time axis values that are not encapsulated by a dictionary and/or are not within the metadata 'Encoding' block.

    Arguments:
        meta_xml_str (str): string representation of the byte XML response from the WCS DescribeCoverage request
        tag (str): the xml string that encapsulates the desired information, e.g., 'gmlrgrid:coefficients'
        occurrence (int): the occurrence of the tag to parse. some tags are repeated several times in the XML response

    Returns:
        str_within_tag (str): string encapsulated by the provided XML tag
    """
    tag_open = f"<{tag}>"
    tag_close = f"</{tag}>"
    str_after_tag = meta_xml_str.split(tag_open)[occurrence]
    str_within_tag = str_after_tag.split(tag_close)[0]
    return str_within_tag


async def get_dim_encodings(cov_id, scrape=None):
    """Get the dimension encodings that map integer values to descriptive strings from a
    Rasdaman coverage that stores the encodings in a metadata "encodings" attribute. We handle exceptions where the coverage we are requesting encodings from does not exist on the backend to prevent Rasdaman work from blocking API development. We can use the same request to scrape various other parts of the DescribeCoverage XML response, but this optional.

    Args:
        cov_id (str): ID of the rasdaman coverage
        scrape (3-tuple): (description (str), tag to scrape between (str), and the occurrence (int) of the tag to search for)

    Returns:
        dim_encodings (nested dict): a lookup where coverage axis names are keys that store dicts of integer-keyed categories.
    """
    meta_url = generate_wcs_query_url(f"DescribeCoverage&COVERAGEID={cov_id}")
    try:
        meta_xml_str = await fetch_data([meta_url])
        dim_encodings = parse_meta_xml_str(meta_xml_str)
        if scrape is not None:
            scrape_desc, scrape_tag, occurrence = scrape
            dim_encodings[scrape_desc] = get_xml_str_between_tags(
                meta_xml_str, scrape_tag, occurrence
            )
        return dim_encodings
    except:
        print(
            f"Warning: Coverage '{cov_id}' is missing from the Rasdaman server {RAS_BASE_URL} you are using."
        )


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
