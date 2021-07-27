import asyncio
from aiohttp import ClientSession
from flask import abort, Blueprint
from . import routes

permafrost_api = Blueprint("permafrost_api", __name__)

# See notes about these first two functions in fire_api.py
def validate_latlon(lat, lon):
    """Validate the lat and lon values,
    return bool for validity"""
    try:
        lat_in_ak_bbox = 51.229 <= float(lat) <= 71.3526
        lon_in_ak_bbox = -179.1506 <= float(lon) <= -129.9795
        valid = lat_in_ak_bbox and lon_in_ak_bbox
    except ValueError:
        valid = False
    return valid


async def fetch_layer_data(url, session):
    """Make an awaitable GET request to URL, return json"""
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    json = await resp.json()
    return json


async def fetch_permafrost_data(lat, lon):
    """Permafrost API - gather all async requests for permafrost data"""
    bbox_offset = 0.000000001
    # base urls should work for all queries of same type (WMS, WFS)

    base_wms_url = f"http://gs.mapventure.org:8080/geoserver/permafrost_beta/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=permafrost_beta%3A{{0}}&STYLES&LAYERS=permafrost_beta%3A{{0}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + bbox_offset}%2C{float(lat) + bbox_offset}"

    base_wfs_url = f"http://gs.mapventure.org:8080/geoserver/permafrost_beta/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat}%2C{lon}%2C{float(lat) + bbox_offset}%2C{float(lon) + bbox_offset}%2Curn:ogc:def:crs:EPSG:4326"

    urls = []
    # append layer names for URLs
    urls.append(
        base_wms_url.format("magt_1m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2050_3338")
    )
    urls.append(
        base_wfs_url.format(
            "jorgenson_2008_pf_extent_ground_ice_volume", "GROUNDICEV,PERMAFROST"
        )
    )

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


def package_gipl(gipl_response):
    """Package GIPL data in dict"""
    gipl_package = {}
    if gipl_response["features"] == []:
        gipl_response[
            "ground temperature"
        ] = "There are no 1m ground temperature projections at this location."
    else:
        gipl_package["1m temperature"] = gipl_response["features"][0]["properties"][
            "GRAY_INDEX"
        ]
    return gipl_package


def package_pf_extent(pf_extent_response):
    """Package permafrost extent data in dict"""
    pf_extent_package = {}
    if pf_extent_response["features"] == []:
        pf_extent_package[
            "permafrost data"
        ] = "There is no permafrost data at this location."
    else:
        pf_extent_package["Ground Ice Volume"] = pf_extent_response["features"][0][
            "properties"
        ]["GROUNDICEV"]
        pf_extent_package["Permafrost zone"] = pf_extent_response["features"][0][
            "properties"
        ]["PERMAFROST"]
    return pf_extent_package


@routes.route("/🧊/<lat>/<lon>")
def run_fetch_permafrost_data(lat, lon):
    """Run the ansync permafrost data requesting and return data as json

    example request: http://localhost:5000/%F0%9F%A7%8A/65.0628/-146.1627"""
    if not validate_latlon(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(fetch_permafrost_data(lat, lon))
    gipl = package_gipl(results[0])
    pf_extent = package_pf_extent(results[1])
    data = {
        "GIPL Projection": gipl,
        "Permafrost Extent": pf_extent,
    }
    return data
