import asyncio
from aiohttp import ClientSession
from flask import abort, Blueprint, render_template
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
    """Package GIPL Mean Annual Ground Temp data"""
    gipl_magt = []

    for i, j in enumerate(wms_targets[0:6]):
        if gipl_magt_resp[i]["features"] == []:
            gipl_magt_resp["Data Status"] = "No data at this location."
        else:
            depth = j.split("_")[1][:-1] + ' m'
            year = j.split("_")[-2]
            title = f"GIPL {year} Mean Annual {depth} Ground Temperature (deg. C.)"
            temp = round(
                gipl_magt_resp[i]["features"][0]["properties"]["GRAY_INDEX"], 2
            )
            di = {'title': title, 'year': year, 'depth': depth, 'temp': temp}
            if int(temp) == -9999:
                di.update({'temp': "No data at this location."})
            gipl_magt.append(di)
    return gipl_magt


def package_gipl_alt(gipl_alt_resp):
    """Package GIPL Active Layer Thickness data"""
    gipl_alt_pkg = []

    for i, j in enumerate(wms_targets[6:8]):
        if gipl_alt_resp[i]["features"] == []:
            gipl_alt_resp["Data Status"] = "No data at this location."
        else:
            year = j.split("_")[-2]
            title = f"GIPL {year} Active Layer Thickness (m)"
            alt = round(gipl_alt_resp[i]["features"][0]["properties"]["GRAY_INDEX"], 2)
            di = {'title': title, 'year': year, 'thickness': alt}
            if int(alt) == -9999:
                di.update({'thickness': "No data at this location."})
            gipl_alt_pkg.append(di)
    return gipl_alt_pkg


def package_obu_magt(obu_magt_resp):
    """Package Obu MAGT data in dict"""
    ds_title = "Obu et al. (2018) Mean Annual Ground Temperature (deg. C)"
    if obu_magt_resp["features"] == []:
        di = {'title': ds_title, "Data Status": "No data at this location."}
    else:
        depth = "Top of Permafrost"
        year = "2000-2016"
        title = (
            f"Obu et al. (2018) {year} Mean Annual {depth} Ground Temperature (deg. C)"
        )

        temp = round(obu_magt_resp["features"][0]["properties"]["GRAY_INDEX"], 2)
        di = {'title': title, 'year': year, 'depth': depth, 'temp': temp}
        if int(temp) == -9999:
            di.update({'temp': "No data at this location."})
    return di


def package_jorgenson(jorgenson_resp):
    """Package Jorgenson data"""
    title = "Jorgenson et al. (2008) Permafrost Extent and Ground Ice Volume"

    if jorgenson_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        ice = jorgenson_resp["features"][0]["properties"]["GROUNDICEV"]
        pfx = jorgenson_resp["features"][0]["properties"]["PERMAFROST"]
        di = {'title': title, 'ice': ice, 'pfx': pfx}
    return di


def package_obu_vector(obu_vector_resp):
    """Package Obu Permafrost Extent Data"""
    title = "Obu et al. (2018) Permafrost Extent"

    if obu_vector_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        pfx = obu_vector_resp["features"][0]["properties"]["PFEXTENT"]
        di = {'title': title, 'pfx': pfx}
    return di


@routes.route("/permafrost")
@routes.route("/permafrost/about")
def permafrost():
    """Render permafrost page"""
    return render_template("permafrost.html")


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
    jorg = package_jorgenson(results[9])
    obu_pfx = package_obu_vector(results[10])
    data = {
        "gipl_magt": gipl_magt,
        "gipl_alt": gipl_alt,
        "obu_magt": obu_magt,
        "obupfx": obu_pfx,
        "jorg": jorg,
    }
    return data
