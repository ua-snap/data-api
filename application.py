import asyncio
from aiohttp import ClientSession
from pyproj import Transformer
from pyproj.crs import CRS

# import rasterio as rio
from flask import Flask, request
from flask import render_template

# not used unless really are supposed to be querying from within API
# landcover_fp = "/Users/kmredilla/Downloads/ASK_NALCMS_2015_LC_30m_AKalbers.tif"

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/🔥")
@app.route("/🔥/about")
def fire():
    return render_template("🔥.html")


# maybe make a function for each dataset?
# make the 5 different coroutines and gather them in fire_api?

# I must be misinterpreting this here but I just can't read around it.
# bit confused here about making an HTTP query to the API itself.
# To me that means calling a session.request with the URL to the
# API (confusing because it seems unnecessary?)

# Also confusing is the endpoint being endpoint/lat/lon. Why are lat/lon/
# in the URL this way? Is there some way to configure such that
# e.g. landcover/65/-135 can be parsed so that the first numeric value is
# taken to be the lat, and second value to be the lon? From the reading
# I was thinking the API endpoint would need to look something like
# ?lat=65&lon=-135

# also confused because the landcover data is in GeoServer - why are
# we going to query it via from within this python API instead?
# Confusing because example in API gdoc has sample WMS query for this very thing

# commenting out for now
# @app.route("/🔥/landcover/lat/lon")
# def query_landcover():
#     """Make an HTTP request to this API for the landcover data"""
#     # reproject here
#     lat = int(request.args["lat"])
#     lon = int(request.args["lon"])
#     transformer = Transformer.from_crs(4326, 3338)
#     x, y = transformer.transform(lat, lon)

#     with rio.open(landcover_fp) as src:
#         row, col = src.index(x, y)
#         # perform windowed read
#         # CODE
#         pass


# async def fetch_landcover(lat, lon):
#     url = "http"
#     session.request(method="GET", url=url)


def verify_latlon():
    # check that lat/lon are provided in request
    if "lat" in request.args:
        lat = float(request.args["lat"])
    else:
        return "Error: No latitude field provided. Please specify a latitude via 'lat'."
    if "lon" in request.args:
        lon = float(request.args["lon"])
    else:
        return (
            "Error: No longitude field provided. Please specify a longitude via 'lon'."
        )
    return lat, lon


def reproject_latlon(lat, lon, target_srs=3338):
    transformer = Transformer.from_crs(4326, target_srs)
    return transformer.transform(lat, lon)


# How it makes more sense to me:
@app.route("/🔥/landcover/lat/lon")
async def fetch_landcover(url, session):
    # verify that lat/lon are present
    # since this endpoint is through API, the logic of having this here
    # and below is that this could be requested independently of the
    # entire sweet of "fire_api" data. However, not sure how to make this
    # work with the ClientSession object yet

    # verify_result = verify_latlon()
    # try:
    #     lat, lon = verify_result
    # except ValueError:
    #     return verify_result

    lat = float(request.args["lat"])
    lon = float(request.args["lon"])
    x1, y1 = reproject_latlon(lat, lon)
    # cushion these values to get the BBOX values, difference between values in example is 1e-11
    # using 5e-11 because adding 1e-11 wasn't changing the floating point values?
    x2, y2 = x1 + 3e-10, y1 + 3e-10

    # can we skip the reprojection step here and pass "SRS=4326"?
    # Couldn't get this to work with specifying

    # currently adding 1e-9 to each of the BBOX coordinates
    url = f"http://gs.mapventure.org:8080/geoserver/alaska_wildfires/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=alaska_wildfires%3Aalaska_landcover_2015&STYLES&LAYERS=alaska_wildfires%3Aalaska_landcover_2015&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A3338&WIDTH=1&HEIGHT=1&BBOX={x1:.12f}%2C{y1:.12f}%2C{x2:.12f}%2C{y2:.12f}"
    print(url)

    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    # logger.info("Got response [%s] for URL: %s", resp.status, url)
    json = await resp.json()

    return json


# not used yet, will be used for other layers
async def fetch_layer_data(url, session):
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    # logger.info("Got response [%s] for URL: %s", resp.status, url)
    json = await resp.json()
    return json


async def fire_api(lat, lon):

    # I was looking at this article for understanding APIs with Flask:
    # https://programminghistorian.org/en/lessons/creating-apis-with-python-and-flask#finding-specific-resources
    # that's where I got this syntax
    # api lat/lon suffix will be used for each url?
    latlon_suffix = f"?lat={lat}&?lon={lon}"

    # want to wait on 5 things, right? 5 different queries?
    # but don't need to leave this function until
    # gather the 5 requests?

    # query to landcover from within the api

    landcover_url = f"http://localhost:5000/landcover/lat/lon/{latlon_suffix}"

    urls = []

    async with ClientSession() as session:
        tasks = [fetch_landcover(landcover_url, session)]
        for url in urls:
            tasks.append(fetch_layer_data(url, session))

        results = await asyncio.gather(*tasks)

    # initiate results accumulator
    # results = []
    return results


# seems that the WMS request only accepts bounding box, height/width, and
# x/y. so need to convert lat lon to bounding box coordinates
def latlon_to_bbox(lat, lon):
    """Seems that the WMS request accepts"""
    pass


# example request: http://localhost:5000/%F0%9F%94%A5/api?lon=-147&lat=65
@app.route("/🔥/api")
def run_fire_api():
    # verify that lat/lon are present
    verify_result = verify_latlon()
    try:
        lat, lon = verify_result
    except ValueError:
        return verify_result

    results = asyncio.run(fire_api(lat, lon))

    return results[0]
