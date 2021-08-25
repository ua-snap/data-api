import asyncio
from flask import abort, Blueprint, render_template
from . import routes
from validate_latlon import validate
from fetch_data import (
    fetch_layer_data,
    generate_query_urls,
    generate_base_wms_url,
    generate_base_wfs_url,
    fetch_data_api,
    check_for_nodata,
)
from config import GS_BASE_URL

geology_api = Blueprint("geology_api", __name__)

wms_targets = []
wfs_targets = {"detailed_geologic_units": "STATE_UNIT,AGE_RANGE"}


def package_usgsgeol(geol_resp):
    """Package geology data in dict"""
    title = "USGS Geologic Map of Alaska"
    if geol_resp[0]["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        di = {}
        gunit = geol_resp[0]["features"][0]["properties"]["STATE_UNIT"]
        age = geol_resp[0]["features"][0]["properties"]["AGE_RANGE"]
        di.update({"title": title, "name": gunit, "age": age})
    return di


@routes.route("/geology/")
@routes.route("/geology/abstract/")
def geo_about():
    return render_template("geology/abstract.html")


@routes.route("/geology/point/")
def geo_about_point():
    return render_template("geology/point.html")


@routes.route("/geology/point/<lat>/<lon>")
def run_fetch_geology(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/geology/60.606/-143.345
    """
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(
        fetch_data_api(GS_BASE_URL, "geology", wms_targets, wfs_targets, lat, lon)
    )
    data = package_usgsgeol(results)
    return data
