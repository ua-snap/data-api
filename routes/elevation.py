import asyncio
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)

# local imports
from generate_urls import generate_wcs_query_url
from fetch_data import fetch_data, fetch_data_api
from validate_request import validate_latlon
from validate_data import nullify_nodata, postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

elevation_api = Blueprint("elevation_api", __name__)

wms_targets = ["astergdem_min_max_avg"]
wfs_targets = {}


def package_astergdem(astergdem_resp):
    """Package ASTER GDEM data in dict"""
    title = "ASTER Global Digital Elevation Model"
    if astergdem_resp[0]["features"] == []:
        return None
    elevation_m = astergdem_resp[0]["features"][0]["properties"]

    di = {
        "title": title,
        "z": elevation_m,
        "units": "meters difference from sea level",
        "res": "1 kilometer",
    }
    return di


@routes.route("/elevation/")
@routes.route("/elevation/abstract/")
def elevation_about():
    return render_template("elevation/abstract.html")


@routes.route("/elevation/point/")
def elevation_about_point():
    return render_template("elevation/point.html")


@routes.route("/elevation/point/<lat>/<lon>")
def run_fetch_elevation(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/elevation/60.606/-143.345
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
    try:
        results = asyncio.run(
            fetch_data_api(GS_BASE_URL, "dem", wms_targets, wfs_targets, lat, lon)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data"), 404
        return render_template("500/server_error.html"), 500

    elevation = package_astergdem(results)
    return elevation


@routes.route("/elevation/huc/")
def z_about_huc():
    return render_template("elevation/huc.html")


@routes.route("/elevation/protectedarea/")
def z_about_protectedarea():
    return render_template("elevation/protectedarea.html")
