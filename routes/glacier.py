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

glacier_api = Blueprint("glacier_api", __name__)

wms_targets = []
wfs_targets = {
    "glacier_earlywisc": "EXTENT",
    "glacier_latewisc": "EXTENT",
    "glacier_pleistocene": "EXTENT",
    "glacier_modern": "EXTENT,glac_name",
}


def package_glaclimits(glaclim_resp):
    """Package glacier data in dict"""
    keys = ["ewisc", "lwisc", "pmax", "now"]
    title = "Past and Present Glaciology of Alaska"
    di = {"title": title}
    for k, resp in zip(keys, glaclim_resp):
        if resp["features"] == []:
            di.update({k: False})
        else:
            di.update({k: True})
            glaclim = resp["features"][0]["properties"]["EXTENT"]
            if glaclim == "Modern":
                glac_name = resp["features"][0]["properties"]["glac_name"]
                di.update({k: {"modern": True, "glacname": glac_name}})
    return di


@routes.route("/glacier/")
@routes.route("/glacier/abstract/")
def glac_about():
    return render_template("glacier/abstract.html")


@routes.route("/glacier/point/")
def glac_about_point():
    return render_template("glacier/point.html")


@routes.route("/glacier/point/<lat>/<lon>")
def run_fetch_glacier(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/glacier/60.606/-143.345
    """
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(
        fetch_data_api(GS_BASE_URL, "glacier", wms_targets, wfs_targets, lat, lon)
    )
    data = package_glaclimits(results)
    return data
