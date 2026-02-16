"""A module to generate query URLs"""

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


def generate_wfs_search_url(
    workspace, lat, lon, hucs_pa_only=False, nearby_fires=False
):
    """
    Generate a WFS URL for searching for nearby features which is what is used by the NCR for
    finding nearby communities + polygon features, and the AFE for finding nearby active fires
    using the site's search interface.

    Args:
        workspace (str): the Geoserver workspace name to search in such as "all_boundaries:all_communities"
        lat (float): latitude of the requested point
        lon (float): longitude of the requested point
        hucs_pa_only (bool): whether to search only for HUCs and protected areas
        nearby_fires (bool): whether to search for nearby fires (uses 1.0 statute mile radius instead of 0.7)
    """
    distance = "0.7"
    if nearby_fires:
        """
        GeoServer only supports degrees, so this is a query for ~70 mile radius.
        https://gis.stackexchange.com/questions/132251/dwithin-wfs-filter-is-not-working
        """
        distance = "1.0"
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName={workspace}&outputFormat=application/json&cql_filter=DWithin(the_geom, POINT({lon} {lat}), {distance}, statute miles)"
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
        + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=all_boundaries:all_areas&propertyName=(id)&outputFormat=application/json&cql_filter=INTERSECTS(the_geom, POINT({lon} {lat})) AND area_type='HUC12'"
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


def generate_wfs_conus_hydrology_url(geom_id):
    wfs_url = (
        GS_BASE_URL
        + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=hydrology:seg_h8_outlet_stats&propertyName=(GNIS_NAME,h8_outlet,huc8,ma12_diff,ma13_diff,ma14_diff,ma15_diff,ma16_diff,ma17_diff,ma18_diff,ma19_diff,ma20_diff,ma21_diff,ma22_diff,ma23_diff,dh1_diff,dl1_diff,dh15_diff,dl16_diff,fh1_diff,fl1_diff,ma99_diff,ma99_hist,the_geom)&outputFormat=application/json&cql_filter=(seg_id_nat={geom_id})"
    )
    return wfs_url


def generate_describe_coverage_url(describe_coverage_str):
    """Generate a WCPS describe() URL from a query string.

    Args:
        describe_coverage_str (str): encoded WCPS describe() query string
    Returns:
        URL for a WCPS describe() request
    """
    return f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.1.0&REQUEST=ProcessCoverages&query={describe_coverage_str}"


def generate_wfs_conus_hydrology_url(stream_id):
    """
    Generate a WFS URL for fetching CONUS hydrology data for a given stream ID. Returns both attributes and geometry for a single stream ID.
    If the stream ID is an empty string, returns only attributes for all streams."""
    if stream_id == "":
        wfs_url = (
            GS_BASE_URL
            + "wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=playground:seg_h8_outlet_stats_simplified&outputFormat=application/json"
        )
        return wfs_url
    else:
        wfs_url = (
            GS_BASE_URL
            + f"wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=playground:seg_h8_outlet_stats_simplified&outputFormat=application/json&cql_filter=(seg_id_nat={stream_id})"
        )
    return wfs_url


def generate_usgs_gauge_daily_streamflow_data_url(gauge_id, start_date, end_date):
    """
    Generate a USGS OGC API URL for fetching daily streamflow data for a given gauge ID and date range.
    Args:
        gauge_id (str): USGS gauge ID (e.g. "USGS-07032000")
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    Returns:
        url (str): USGS OGC API URL for daily streamflow data
    """
    base_url = "https://api.waterdata.usgs.gov/ogcapi/v0/"
    param = "00060"  # parameter code for daily discharge in CFS (see here: https://help.waterdata.usgs.gov/parameter_cd?group_cd=PHY)
    properties = "time,value,unit_of_measure"
    time = start_date + "/" + end_date
    request_str = f"collections/daily/items?f=json&lang=en-US&limit=50000&properties={properties}&skipGeometry=true&sortby=%2Btime&offset=0&datetime={time}&monitoring_location_id={gauge_id}&parameter_code={param}&approval_status=Approved"
    url = base_url + request_str
    return url


def generate_usgs_gauge_metadata_url(gauge_id):
    """
    Generate a USGS OGC API URL for fetching metadata for a given gauge ID.
    Args:
        gauge_id (str): USGS gauge ID (e.g. "USGS-07032000")
    Returns:
        url (str): USGS OGC API URL for gauge metadata (gauge name and location)"""
    base_url = "https://api.waterdata.usgs.gov/ogcapi/v0/"
    properties = "monitoring_location_name"
    request_str = f"collections/monitoring-locations/items?f=json&lang=en-US&limit=1&properties={properties}&skipGeometry=false&offset=0&id={gauge_id}"
    url = base_url + request_str
    return url
