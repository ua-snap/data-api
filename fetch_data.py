"""
A module of data gathering and data integrity functions and variables that could be used across multiple endpoints.
"""
import asyncio
from aiohttp import ClientSession
from config import RAS_BASE_URL
from datetime import datetime


bbox_offset = 0.000000001


def generate_base_wms_url(backend, workspace, lat, lon):
    """Generate the foundational WMS URL for all WMS queries."""
    wms_base = (
        backend
        + f"{workspace}/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS={workspace}%3A{{0}}&STYLES&LAYERS={workspace}%3A{{0}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + bbox_offset}%2C{float(lat) + bbox_offset}"
    )
    return wms_base


def generate_base_wfs_url(backend, workspace, lat, lon):
    """Generate the foundational WFS URL for all WFS queries."""
    wfs_base = (
        backend
        + f"{workspace}/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat}%2C{lon}%2C{float(lat) + bbox_offset}%2C{float(lon) + bbox_offset}%2Curn:ogc:def:crs:EPSG:4326"
    )
    return wfs_base


def generate_query_urls(wms, wms_base, wfs, wfs_base):
    """Generate the URLs for querying individual data layers."""
    urls = []
    for lyr in wms:
        urls.append(wms_base.format(lyr))
    for veclyr in wfs:
        urls.append(wfs_base.format(veclyr, wfs[veclyr]))
    return urls


def get_wcs_request_str(x, y, var_coord, cov_id, encoding="json"):
    """Generic WCS GetCoverage request for fetching a
    subset of a coverage over X and Y axes

    x (float or str): x-coordinate for point query (float), or string
        composed as "x1,x2" for bbox query, where x1 and x2 are
        lower and upper bounds of bbox
    y (float or str): y-coordinate for point query (float), or string
        composed as "y1,y2" for bbox query, where y1 and y2 are
        lower and upper bounds of bbox
    var_coord (int): coordinate value corresponding to varname to query
    cov_id (str): Rasdaman coverage ID
    encoding (str): currently supports either "json" or "netcdf"
        for point or bbox queries, respectively

    """
    wcs = (
        f"GetCoverage&COVERAGEID={cov_id}"
        f"&SUBSET=X({x})&SUBSET=Y({y}&SUBSET=varname({var_coord}))"
        f"&FORMAT=application/{encoding}"
    )
    return wcs


def generate_wcs_query_url(request_str):
    """Make a WCS URL by plugging a request substring
     into the base WCS URL

    request_srtr (str): either a typical WCS 
    """
    # currently hardcoded to Rasdaman URL backend because it's the only one
    # we make WCS requests to (?)
    return f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST={request_str}"


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
    urls = generate_query_urls(wms_targets, base_wms_url, wfs_targets, base_wfs_url)

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


def check_for_nodata(di, varname, varval, nodata):
    """Evaluate if a specific "no data" value (e.g. -9999) is returned and replace with explanatory text."""
    if int(varval) == int(nodata):
        di.update({varname: "No data at this location"})


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
