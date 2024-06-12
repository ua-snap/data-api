"""A module to generate query URLs"""

import asyncio
from aiohttp import ClientSession
import urllib.parse
from config import RAS_BASE_URL, GS_BASE_URL
from luts import bbox_offset


def generate_base_wms_url(backend, workspace, lat, lon):
    """Generate the foundational WMS URL for all WMS queries."""
    wms_base = (
        backend
        + f"{workspace}/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image/jpeg&TRANSPARENT=true&QUERY_LAYERS={workspace}:{{0}}&STYLES&LAYERS={workspace}:{{0}}&exceptions=application/vnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG:4326&WIDTH=1&HEIGHT=1&BBOX={lon},{lat},{float(lon) + bbox_offset},{float(lat) + bbox_offset}"
    )
    return wms_base


def generate_base_wfs_url(backend, workspace, lat, lon):
    """Generate the foundational WFS URL for all WFS queries."""
    wfs_base = (
        backend
        + f"{workspace}/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat},{lon},{float(lat) + bbox_offset},{float(lon) + bbox_offset},urn:ogc:def:crs:EPSG:4326"
    )
    return wfs_base


def generate_wfs_search_url(workspace, lat, lon, hucs_pa_only=False):
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}&outputFormat=application/json&cql_filter=DWithin(the_geom, POINT({lon} {lat}), 0.7, statute miles)"
    )
    if hucs_pa_only:
        wfs_url += "AND (type='huc' OR type='protected_area')"
    return wfs_url


def generate_wfs_places_url(
    workspace, properties=None, filter=None, filter_type="type"
):
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=2.0.0&request=GetFeature&typeName={workspace}&outputFormat=application/json"
    )
    if properties:
        wfs_url += f"&propertyName=({properties})"
    if filter:
        wfs_url += f"&filter=<Filter><PropertyIsEqualTo><PropertyName>{filter_type}</PropertyName><Literal>{filter}</Literal></PropertyIsEqualTo></Filter>"
    return wfs_url


def generate_wfs_huc12_intersection_url(lat, lon):
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=all_boundaries:ak_huc12&propertyName=(id)&outputFormat=application/json&cql_filter=INTERSECTS(the_geom, POINT({lon} {lat}))"
    )
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


def generate_wfs_species_huc12_intersection_url(lat, lon):
    """Generate WFS query URL to retrieve species data HUC12."""
    wfs_url = (GS_BASE_URL + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=species:huc12_species&propertyName=(HUC_12)&outputFormat=application/json&cql_filter=INTERSECTS(the_geom, POINT({lon} {lat}))")
    return wfs_url