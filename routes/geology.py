import asyncio
from flask import (
    Blueprint,
    render_template,
)

# local imports
from fetch_data import fetch_data_api
from validate_request import validate_latlon
from postprocessing import postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

geology_api = Blueprint("geology_api", __name__)

wms_targets = []
wfs_targets = {"detailed_geologic_units": "STATE_UNIT,AGE_RANGE"}


def package_usgsgeol(geol_resp):
    """Package geology data in dict"""
    title = "USGS Geologic Map of Alaska"
    if geol_resp[0]["features"] == []:
        return None
    di = {}
    gunit = geol_resp[0]["features"][0]["properties"]["STATE_UNIT"]
    age = geol_resp[0]["features"][0]["properties"]["AGE_RANGE"]
    di.update({"title": title, "name": gunit, "age": age})
    return di


@routes.route("/geology/")
@routes.route("/geology/abstract/")
@routes.route("/geology/point/")
def geo_about():
    return render_template("documentation/geology.html")


@routes.route("/geology/point/<lat>/<lon>")
def run_fetch_geology(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/geology/60.606/-143.345
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
            fetch_data_api(GS_BASE_URL, "geology", wms_targets, wfs_targets, lat, lon)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
    data = package_usgsgeol(results)
    return postprocess(data, "geology")
