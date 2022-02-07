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
from fetch_data import fetch_data, fetch_data_api
from validate_request import validate_latlon
from validate_data import nullify_nodata, postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

elevation_api = Blueprint("elevation_api", __name__)

wms_targets = ["astergdem"]
wfs_targets = {}


def package_astergdem(astergdem_resp):
    """Package ASTER GDEM data in dict"""
    title = "ASTER Global Digital Elevation Model"
    if astergdem_resp[0]["features"] == []:
        di = {"title": title, "Data Status": nodata_message}
    else:
        elevation_m = astergdem_resp[0]["features"][0]["properties"]["GRAY_INDEX"]
        if elevation_m == -9999:
            di = {"title": title, "Data Status": nodata_message}
        else:
            di = {
                "title": title,
                "z": elevation_m,
                "units": "meters difference from sea level",
                "res": "1 kilometer",
            }
            for k in di.keys():
                check_for_nodata(di, k, elevation_m, -9999)
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
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(
        fetch_data_api(GS_BASE_URL, "dem", wms_targets, wfs_targets, lat, lon)
    )
    elevation = package_astergdem(results)
    return elevation
