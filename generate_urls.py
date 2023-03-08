"""A module to generate query URLs"""

import asyncio
from aiohttp import ClientSession
from config import RAS_BASE_URL, GS_BASE_URL
from luts import bbox_offset


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


def generate_wfs_search_url(workspace, lat, lon, hucs_pa_only=False):
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}&outputFormat=application%2Fjson&cql_filter=DWithin(the_geom,%20POINT({lon}%20{lat}),%200.7,%20statute%20miles)"
    )
    if hucs_pa_only:
        wfs_url += "AND (type=%27huc%27%20OR%20type=%27protected_area%27)"
    return wfs_url


def generate_wfs_places_url(workspace, properties, filter=None):
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=2.0.0&request=GetFeature&typeName={workspace}&outputFormat=application%2Fjson&propertyName=({properties})"
    )
    if filter:
        wfs_url += f"&filter=%3CFilter%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3Etype%3C/PropertyName%3E%3CLiteral%3E{filter}%3C/Literal%3E%3C/PropertyIsEqualTo%3E%3C/Filter%3E"
    return wfs_url


def generate_wcs_query_url(request_str, backend=RAS_BASE_URL):
    """Make a WCS URL by plugging a request substring into a base WCS URL.

    The default backend is Rasdaman because that is typically the target for WCS requests. GS usage is expected to be less frequent. However, the GS service doesn't accept the "&FORMAT=application/" syntax so we'll strip that and default it to a "&FORMAT=GeoTIFF" URL suffix which returns a GeoTIFF.
    Args:
        request_str (str): a typical WCS string
        backend (str): URL for geospatial data server, typically imported from config.py
    Returns:
        URL for a WCS request
    """
    if backend == GS_BASE_URL:
        request_str = request_str.split("application/")[0] + "GeoTIFF"
    return f"{backend}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST={request_str}"


def generate_wms_and_wfs_query_urls(wms, wms_base, wfs, wfs_base):
    """Generate WMS and WFS query URLs for individual data layers."""
    urls = []
    for lyr in wms:
        urls.append(wms_base.format(lyr))
    for veclyr in wfs:
        urls.append(wfs_base.format(veclyr, wfs[veclyr]))
    return urls
