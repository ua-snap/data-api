import asyncio
from aiohttp import ClientSession
from pyproj import Transformer
from pyproj.crs import CRS

from flask import Flask, request
from flask import render_template

# hard-coded here for now - will go in a LUT-like thing later
landcover_names = {
          1: "Temperate or sub-polar needleleaf forest",
          2: "Sub-polar taiga or needleleaf forest",
          5: "Temperate or sub-polar broadleaf deciduous forest",
          6: "Mixed forest",
          8: "Temperate or sub-polar shrubland",
          10: "Temperate or sub-polar grassland",
          11: "Sub-polar or polar shrubland-lichen-moss",
          12: "Sub-polar or polar grassland-lichen-moss",
          13: "Sub-polar or polar barren-lichen-moss",
          14: "Wetland",
          15: "Cropland",
          16: "Barren land",
          17: "Urban and built-up",
          18: "Water",
          19: "Snow and ice",
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
snow_status = {
    2: False,
    4: True
    }


app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/🔥")
@app.route("/🔥/about")
def fire():
    return render_template("🔥.html")


async def fetch_layer_data(url, session):
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    # logger.info("Got response [%s] for URL: %s", resp.status, url)
    json = await resp.json()
    return json


async def fire_api(lat, lon):
    bbox_offset = 0.000000001
    # base urls should work for all queries of same type (WMS, WFS)

    base_wms_url = f"http://gs.mapventure.org:8080/geoserver/alaska_wildfires/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=alaska_wildfires%3A{{}}&STYLES&LAYERS=alaska_wildfires%3A{{}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + bbox_offset}%2C{float(lat) + bbox_offset}"

    base_wfs_url = f"http://gs.mapventure.org:8080/geoserver/alaska_wildfires/wfs?SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&TypeName={{}}&PropertyName={{}}&outputFormat=application/json&srsName=urn:ogc:def:crs:EPSG:4326&BBOX={lat}%2C{lon}%2C{float(lat) + bbox_offset}%2C{float(lon) + bbox_offset}%2Curn:ogc:def:crs:EPSG:4326"

    urls = []
    # append layer names for URLs
    urls.append(base_wms_url.format("alaska_landcover_2015", "alaska_landcover_2015"))
    urls.append(base_wms_url.format("spruceadj_3338", "spruceadj_3338"))
    urls.append(base_wms_url.format("snow_cover_3338", "snow_cover_3338"))
    urls.append(base_wms_url.format("alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099", "alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099"))
    urls.append(base_wfs_url.format("historical_fire_perimiters","NAME,FIREYEAR"))

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)
    return results


def package_fire_history(fire_hist_response):
    fire_hist_package = {}
    if fire_hist_response['features'] == []:
        fire_hist_package['fire history'] = "There is no fire history at this location."
    else:
        fire_hist_package['Fire Year'] = fire_hist_response['features'][0]['properties']['FIREYEAR']
        fire_hist_package['Fire Name'] = fire_hist_response['features'][0]['properties']['NAME']
    return fire_hist_package


def package_flammability(flammability_response):
    flammability_package = {}
    if flammability_response['features'] == []:
        flammability_package['flammability'] = "There is no relative flammability projection at this location."
    else:
        flammability_package['Relative Flammability Index'] = flammability_response['features'][0]['properties']['GRAY_INDEX']
    return flammability_package


def package_snow(snow_response):
    snow_package = {}
    if snow_response['features'] == []:
        snow_package['is_snow'] = "There is no snow information at this location."
    else:
        snow_package['is_snow'] = snow_status[snow_response['features'][0]['properties']['GRAY_INDEX']]
    return snow_package


def package_fire_danger(fire_danger_response):
    fire_danger_package = {}
    if fire_danger_response['features'] == []:
        fire_danger_package['fire_danger'] = "There is no fire danger information at this location."
    else:
        fire_danger_package['code'] = fire_danger_response['features'][0]['properties']['GRAY_INDEX']
        fire_danger_package['type'] = smokey_bear_names[fire_danger_package['code']]
        fire_danger_package['color'] = smokey_bear_styles[fire_danger_package['code']]
    return fire_danger_package


def package_landcover(landcover_response):
    landcover_package = {}
    if landcover_response['features'] == []:
        landcover_package['landcover'] = "There is no landcover information at this location."
    else:
        landcover_package['code'] = landcover_response['features'][0]['properties']['PALETTE_INDEX']
        landcover_package['type'] = landcover_names[landcover_package['code']]
    return landcover_package


@app.route("/🔥/<lat>/<lon>")
def run_fire_api(lat, lon):
    # verify that lat/lon are present
    results = asyncio.run(fire_api(lat, lon))
    landcover = package_landcover(results[0])
    firedanger = package_fire_danger(results[1])
    snow = package_snow(results[2])
    relflammability = package_flammability(results[3])
    firehist = package_fire_history(results[4])
    data = {'Land cover': landcover, 'Snow cover': snow, "Current fire danger": firedanger, 'Historical Fires': firehist, 'Future Flammability': relflammability}
    return data

# example request: http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
