"""
A module of data gathering and data integrity functions and variables that could be used across multiple endpoints.
"""
import asyncio
from aiohttp import ClientSession


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


async def fetch_layer_data(url, session, encoding="json"):
    """Make an awaitable GET request to URL, return json
    
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
        query_result = await resp.json()
    elif encoding == "netcdf":
        query_result = await resp.read()

    return query_result


async def fetch_data_api(backend, workspace, wms_targets, wfs_targets, lat, lon):
    """Generic Data API - gather all async requests for specified data layers in a workspace."""
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
