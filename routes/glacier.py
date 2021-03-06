import asyncio
from flask import (
    Blueprint,
    render_template,
)

# local imports
from fetch_data import fetch_data_api
from validate_request import validate_latlon
from validate_data import postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

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


@routes.route("/glaciers/")
@routes.route("/glaciers/abstract/")
@routes.route("/glacier/")
@routes.route("/glacier/abstract/")
@routes.route("/glaciology/")
@routes.route("/glaciology/abstract/")
def glac_about():
    return render_template("glacier/abstract.html")


@routes.route("/glaciers/")
@routes.route("/glaciers/point/")
@routes.route("/glacier/")
@routes.route("/glacier/point/")
@routes.route("/glaciology/")
@routes.route("/glaciology/point/")
def glac_about_point():
    return render_template("glacier/point.html")


@routes.route("/glacier/point/<lat>/<lon>")
def run_fetch_glacier(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/glacier/60.606/-143.345
    """
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    # verify that lat/lon are present
    try:
        results = asyncio.run(
            fetch_data_api(GS_BASE_URL, "glacier", wms_targets, wfs_targets, lat, lon)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
    data = package_glaclimits(results)
    return postprocess(data, "glacier")
