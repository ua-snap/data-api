import asyncio
from aiohttp import ClientSession
from flask import abort, Blueprint
from validate_latlon import validate
from . import routes
from config import GS_BASE_URL

permafrost_api = Blueprint("permafrost_api", __name__)

wms_targets = [
    "magt_1m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2010_3338",
    "magt_1m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2050_3338",
    "magt_3m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2010_3338",
    "magt_3m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2050_3338",
    "magt_5m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2010_3338",
    "magt_5m_c_iem_gipl2_ar5_ncar_ccsm4_rcp85_2050_3338",
    "alt_m_iem_gipl2_ar5_ncar_ccsm4_rcp85_2010_3338",
    "alt_m_iem_gipl2_ar5_ncar_ccsm4_rcp85_2050_3338",
    "obu_2018_magt",
]


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
    base_wms_url = (
        GS_BASE_URL
        + f"permafrost_beta/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=permafrost_beta%3A{{0}}&STYLES&LAYERS=permafrost_beta%3A{{0}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + bbox_offset}%2C{float(lat) + bbox_offset}"
    )
    base_wfs_url = (
        GS_BASE_URL
        + f"permafrost_beta/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat}%2C{lon}%2C{float(lat) + bbox_offset}%2C{float(lon) + bbox_offset}%2Curn:ogc:def:crs:EPSG:4326"
    )

    urls = []

    # append layer names for URLs
    for lyr in wms_targets:
        urls.append(base_wms_url.format(lyr))
    urls.append(
        base_wfs_url.format(
            "jorgenson_2008_pf_extent_ground_ice_volume", "GROUNDICEV,PERMAFROST"
        )
    )
    urls.append(base_wfs_url.format("obu_pf_extent", "PFEXTENT"))

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


def package_gipl_magt(gipl_magt_resp):
    """Package GIPL MAGT data in dict"""
    gipl_magt_pkg = {}

    for ix, i in enumerate(wms_targets[0:6]):
        if gipl_magt_resp[ix]["features"] == []:
            gipl_magt_resp["GIPL MAGT"] = "No data at this location."
        else:
            depth = i.split("_")[1] + "_"
            yr = i.split("_")[-2] + "_"
            key_str = "GIPL_" + yr + depth + "MAGT"
            gipl_magt_pkg[key_str] = round(
                gipl_magt_resp[ix]["features"][0]["properties"]["GRAY_INDEX"], 3
            )
    return gipl_magt_pkg


def package_gipl_alt(gipl_alt_resp):
    """Package GIPL ALT data in dict"""
    gipl_alt_pkg = {}

    for ix, i in enumerate(wms_targets[6:8]):
        if gipl_alt_resp[ix]["features"] == []:
            gipl_alt_resp["GIPL ALT"] = "No data at this location."
        else:
            yr = i.split("_")[-2] + "_"
            key_str = "GIPL_" + yr + "ALT"
            gipl_alt_pkg[key_str] = round(
                gipl_alt_resp[ix]["features"][0]["properties"]["GRAY_INDEX"], 3
            )
    return gipl_alt_pkg


def package_obu_magt(obu_magt_resp):
    """Package Obu MAGT data in dict"""
    obu_magt_pkg = {}

    if obu_magt_resp["features"] == []:
        obu_magt_resp["Obu MAGT"] = "No data at this location."
    else:
        key_str = "Obu 2000-2016 MAGT (Top of Permafrost)"
        obu_magt_pkg[key_str] = round(
            obu_magt_resp["features"][0]["properties"]["GRAY_INDEX"], 3
        )
    return obu_magt_pkg


def package_jorgenson(jorgenson_resp):
    """Package Jorgenson data in dict"""
    jorgenson_pkg = {}
    if jorgenson_resp["features"] == []:
        jorgenson_pkg["Jorgenson data"] = "No data at this location."
    else:
        jorgenson_pkg["Ground Ice Volume"] = jorgenson_resp["features"][0][
            "properties"
        ]["GROUNDICEV"]
        jorgenson_pkg["Permafrost Extent"] = jorgenson_resp["features"][0][
            "properties"
        ]["PERMAFROST"]
    return jorgenson_pkg


def package_obu_vector(obu_vector_resp):
    """Package obu_vector data in dict"""
    obu_vector_pkg = {}
    if obu_vector_resp["features"] == []:
        obu_vector_pkg["Obu vector data"] = "No data at this location."
    else:
        obu_vector_pkg["Permafrost Extent"] = obu_vector_resp["features"][0][
            "properties"
        ]["PFEXTENT"]
    return obu_vector_pkg


@routes.route("/permafrost/<lat>/<lon>")
def run_fetch_permafrost_data(lat, lon):
    """Run the ansync permafrost data requesting and return data as json
    example request: http://localhost:5000/permafrost/65.0628/-146.1627"""
    if not validate(lat, lon):
        abort(400)
    results = asyncio.run(fetch_permafrost_data(lat, lon))
    gipl_magt = package_gipl_magt(results[0:6])
    gipl_alt = package_gipl_alt(results[6:8])
    obu_magt = package_obu_magt(results[8])
    jorgenson = package_jorgenson(results[9])
    obu_pf_extent = package_obu_vector(results[10])
    data = {
        "GIPL Mean Annual Ground Temperature (deg. C)": gipl_magt,
        "Obu et al. (2018) Mean Annual Ground Temperature (deg. C) at Top of Permafrost": obu_magt,
        "Obu et al. (2018) Permafrost Extent": obu_pf_extent,
        "GIPL Active Layer Thickness (m)": gipl_alt,
        "Jorgenson et al. (2008) Permafrost Extent and Ground Ice Volume": jorgenson,
    }
    return data
