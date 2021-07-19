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


# not used yet, will be used for other layers
async def fetch_layer_data(url, session):
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    # logger.info("Got response [%s] for URL: %s", resp.status, url)
    json = await resp.json()
    return json


async def fire_api(lat, lon):
    # currently adding 1e-9 to each of the BBOX coordinates
    # Also, currently querying in EPSG:4326 because apparently this is supported.
    # this url should be the same for all the WMS queries - landcover, snow cover, fire danger, flammability
    base_wms_url = f"http://gs.mapventure.org:8080/geoserver/alaska_wildfires/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetFeatureInfo&FORMAT=image%2Fjpeg&TRANSPARENT=true&QUERY_LAYERS=alaska_wildfires%3A{{}}&STYLES&LAYERS=alaska_wildfires%3A{{}}&exceptions=application%2Fvnd.ogc.se_inimage&INFO_FORMAT=application/json&FEATURE_COUNT=50&X=1&Y=1&SRS=EPSG%3A4326&WIDTH=1&HEIGHT=1&BBOX={lon}%2C{lat}%2C{float(lon) + 0.000000001}%2C{float(lat) + 0.000000001}"

    urls = []

    # just appneding landcover layer URL for now
    urls.append(base_wms_url.format("alaska_landcover_2015", "alaska_landcover_2015"))

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)

    return results


# example request: http://localhost:5000/%F0%9F%94%A5/65/-147
@app.route("/🔥/<lat>/<lon>")
def run_fire_api(lat, lon):
    # verify that lat/lon are present
    results = asyncio.run(fire_api(lat, lon))

    return results[0]
