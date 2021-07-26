import asyncio
from aiohttp import ClientSession
from flask import Flask, render_template, abort

# hard-coded here for now - will go in a LUT-like thing later
landcover_names = {
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
}
smokey_bear_styles = {
    1: "#2b83ba",
    2: "#abdda4",
    3: "#ffffbf",
    4: "#fdae61",
    5: "#d7191c",
}
snow_status = {2: False, 4: True}


app = Flask(__name__)


@app.route("/")
def index():
    """Render index page"""
    return render_template("index.html")


@app.route("/🔥")
@app.route("/🔥/about")
def fire():
    """Render fire page"""
    return render_template("🔥.html")


async def fetch_layer_data(url, session):
    """Make an awaitable GET request to URL, return json"""
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    json = await resp.json()
    return json


async def fire_api(lat, lon):
    """Fire API - gather all async requests for fire data"""
    bbox_offset = 0.000000001
    # base urls should work for all queries of same type (WMS, WFS)

    base_wms_url = f"http://gs.mapventure.org:8080/geoserver/alaska_wildfires/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=alaska_wildfires%3A{{}}&STYLES&LAYERS=alaska_wildfires%3A{{}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + bbox_offset}%2C{float(lat) + bbox_offset}"

    base_wfs_url = f"http://gs.mapventure.org:8080/geoserver/alaska_wildfires/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat}%2C{lon}%2C{float(lat) + bbox_offset}%2C{float(lon) + bbox_offset}%2Curn:ogc:def:crs:EPSG:4326"

    urls = []
    # append layer names for URLs
    urls.append(base_wms_url.format("alaska_landcover_2015", "alaska_landcover_2015"))
    urls.append(base_wms_url.format("spruceadj_3338", "spruceadj_3338"))
    urls.append(base_wms_url.format("snow_cover_3338", "snow_cover_3338"))
    urls.append(
        base_wms_url.format(
            "alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099",
            "alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099",
        )
    )
    urls.append(base_wfs_url.format("historical_fire_perimiters", "NAME,FIREYEAR"))

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


def package_fire_history(fire_hist_response):
    """Package fire history data in dict"""
    fire_hist_package = {}
    if fire_hist_response["features"] == []:
        fire_hist_package["fire history"] = "There is no fire history at this location."
    else:
        fire_hist_package["Fire Year"] = fire_hist_response["features"][0][
            "properties"
        ]["FIREYEAR"]
        fire_hist_package["Fire Name"] = fire_hist_response["features"][0][
            "properties"
        ]["NAME"]
    return fire_hist_package


def package_flammability(flammability_response):
    """Package flammability data in dict"""
    flammability_package = {}
    if flammability_response["features"] == []:
        flammability_package[
            "flammability"
        ] = "There is no relative flammability projection at this location."
    else:
        flammability_package["Relative Flammability Index"] = flammability_response[
            "features"
        ][0]["properties"]["GRAY_INDEX"]
    return flammability_package


def package_snow(snow_response):
    """Package snow cover data in dict"""
    snow_package = {}
    if snow_response["features"] == []:
        snow_package["is_snow"] = "There is no snow information at this location."
    else:
        snow_package["is_snow"] = snow_status[
            snow_response["features"][0]["properties"]["GRAY_INDEX"]
        ]
    return snow_package


def package_fire_danger(fire_danger_response):
    """Package fire danger data in dict"""
    fire_danger_package = {}
    if fire_danger_response["features"] == []:
        fire_danger_package[
            "fire_danger"
        ] = "There is no fire danger information at this location."
    else:
        fire_danger_package["code"] = fire_danger_response["features"][0]["properties"][
            "GRAY_INDEX"
        ]
        fire_danger_package["type"] = smokey_bear_names[fire_danger_package["code"]]
        fire_danger_package["color"] = smokey_bear_styles[fire_danger_package["code"]]
    return fire_danger_package


def package_landcover(landcover_response):
    """Package landcover data in dict"""
    landcover_package = {}
    if landcover_response["features"] == []:
        landcover_package[
            "landcover"
        ] = "There is no landcover information at this location."
    else:
        code = landcover_response["features"][0]["properties"]["PALETTE_INDEX"]
        landcover_package["code"] = code
        landcover_package["type"] = landcover_names[code]["type"]
        landcover_package["color"] = landcover_names[code]["color"]
    return landcover_package


def validate_latlon(lat, lon):
    """Validate the lat and lon values,
    return bool for validity"""
    try:
        lat_numeric = isinstance(int(float(lat)), int) or isinstance(float(lat), float)
        lon_numeric = isinstance(int(float(lon)), int) or isinstance(float(lon), float)
        lat_in_ak_bbox = 51.229 <= float(lat) <= 71.3526
        lon_in_ak_bbox = -179.1506 <= float(lon) <= -129.9795
        valid = lat_in_ak_bbox and lon_in_ak_bbox
    except ValueError:
        valid = False
    return valid


@app.route("/🔥/<lat>/<lon>")
def run_fire_api(lat, lon):
    """Run the ansync requesting and return data"""
    if not validate_latlon(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(fire_api(lat, lon))
    landcover = package_landcover(results[0])
    firedanger = package_fire_danger(results[1])
    snow = package_snow(results[2])
    relflammability = package_flammability(results[3])
    firehist = package_fire_history(results[4])
    data = {
        "Land cover": landcover,
        "Snow cover": snow,
        "Current fire danger": firedanger,
        "Historical Fires": firehist,
        "Future Flammability": relflammability,
    }
    return data


# example request: http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
