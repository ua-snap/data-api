import asyncio
from aiohttp import ClientSession
from flask import abort, Blueprint, render_template
from validate_latlon import validate
from . import routes
from config import GS_BASE_URL

fire_api = Blueprint("fire_api", __name__)

# hard-coded here for now - will go in a LUT-like thing later
landcover_names = {
    0: {"type": "No Data at this location.", "color": "#ffffff"},
    1: {"type": "Temperate or sub-polar needleleaf forest", "color": "#003d00"},
    2: {"type": "Sub-polar taiga or needleleaf forest", "color": "#949c70"},
    5: {
        "type": "Temperate or sub-polar broadleaf deciduous forest",
        "color": "#148c3d",
    },
    6: {"type": "Mixed forest", "color": "#5c752b"},
    8: {"type": "Temperate or sub-polar shrubland", "color": "#b38a33"},
    10: {"type": "Temperate or sub-polar grassland", "color": "#e1cf8a"},
    11: {"type": "Sub-polar or polar shrubland-lichen-moss", "color": "#9c7554"},
    12: {"type": "Sub-polar or polar grassland-lichen-moss", "color": "#bad48f"},
    13: {"type": "Sub-polar or polar barren-lichen-moss", "color": "#408a70"},
    14: {"type": "Wetland", "color": "#6ba38a"},
    15: {"type": "Cropland", "color": "#e6ae66"},
    16: {"type": "Barren land", "color": "#a8abae"},
    17: {"type": "Urban and built-up", "color": "#DD40D6"},
    18: {"type": "Water", "color": "#4c70a3"},
    19: {"type": "Snow and ice", "color": "#eee9ee"},
}
smokey_bear_names = {
    1: "Low",
    2: "Medium",
    3: "High",
    4: "Very High",
    5: "Extreme",
    6: "No data at this location.",
}
smokey_bear_styles = {
    1: "#2b83ba",
    2: "#abdda4",
    3: "#ffffbf",
    4: "#fdae61",
    5: "#d7191c",
    6: "#ffffff",
}
snow_status = {
    1: "Sea",
    2: False,
    3: "Sea ice",
    4: True,
    0: "No data at this location.",
}


async def fetch_layer_data(url, session):
    """Make an awaitable GET request to URL, return json"""
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    json = await resp.json()
    return json


async def fetch_fire_data(lat, lon):
    """Fire API - gather all async requests for fire data"""
    bbox_offset = 0.000000001
    # base urls should work for all queries of same type (WMS, WFS)

    base_wms_url = (
        GS_BASE_URL
        + f"alaska_wildfires/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=alaska_wildfires%3A{{0}}&STYLES&LAYERS=alaska_wildfires%3A{{0}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + bbox_offset}%2C{float(lat) + bbox_offset}"
    )

    base_wfs_url = (
        GS_BASE_URL
        + f"alaska_wildfires/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat}%2C{lon}%2C{float(lat) + bbox_offset}%2C{float(lon) + bbox_offset}%2Curn:ogc:def:crs:EPSG:4326"
    )

    urls = []
    # append layer names for URLs
    urls.append(base_wms_url.format("alaska_landcover_2015"))
    urls.append(base_wms_url.format("spruceadj_3338"))
    urls.append(base_wms_url.format("snow_cover_3338"))
    urls.append(
        base_wms_url.format("alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099")
    )
    urls.append(base_wfs_url.format("historical_fire_perimiters", "NAME,FIREYEAR"))

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


def package_fire_history(fihist_resp):
    """Package fire history data in dict"""
    title = "Historical fires"
    if fihist_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        di = {}
        for i in fihist_resp["features"]:
            fi_name = list(i.values())[-1]['NAME']
            fi_year = list(i.values())[-1]['FIREYEAR']
            di.update({fi_name: fi_year})
    return di


def package_flammability(flammability_resp):
    """Package flammability data in dict"""
    title = "Projected relative flammability"
    if flammability_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        flamm = round(flammability_resp["features"][0]["properties"]["GRAY_INDEX"], 4)
        di = {'title': title, "flamm": flamm}
        if int(flamm) == -9999:
            di.update({'flamm': "No data at this location."})
    return di


def package_snow(snow_resp):
    """Package snow cover data"""
    title = "Today's Snow Cover"
    if snow_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        snow = snow_status[snow_resp["features"][0]["properties"]["GRAY_INDEX"]]
        di = {'title': title, 'is_snow': snow}
    return di


def package_fire_danger(fire_danger_resp):
    """Package fire danger data in dict"""
    title = "Today's Fire Danger"
    if fire_danger_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        code = fire_danger_resp["features"][0]["properties"]["GRAY_INDEX"]
        fitype = smokey_bear_names[code]
        color = smokey_bear_styles[code]
        di = {'title': title, 'code': code, 'type': fitype, 'color': color}
    return di


def package_landcover(landcover_resp):
    """Package landcover data in dict"""
    title = "Land cover types"
    if landcover_resp["features"] == []:
        di = {'title': title, "Data Status": "No data at this location."}
    else:
        code = landcover_resp["features"][0]["properties"]["PALETTE_INDEX"]
        lctype = landcover_names[code]["type"]
        color = landcover_names[code]["color"]
        di = {'title': title, 'code': code, 'type': lctype, 'color': color}
    return di


@routes.route("/🔥")
@routes.route("/🔥/about")
def fire():
    """Render fire page"""
    return render_template("🔥.html")


@routes.route("/🔥/<lat>/<lon>")
def run_fetch_fire(lat, lon):
    """Run the ansync requesting and return data
    example request: http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
    """
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(fetch_fire_data(lat, lon))
    landcover = package_landcover(results[0])
    firedanger = package_fire_danger(results[1])
    snow = package_snow(results[2])
    relflammability = package_flammability(results[3])
    firehist = package_fire_history(results[4])
    data = {
        "lc": landcover,
        "is_snow": snow,
        "cfd": firedanger,
        "hist_fire": firehist,
        "prf": relflammability,
    }
    return data
