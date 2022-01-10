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
from validate_latlon import validate, project_latlon
from validate_data import check_for_nodata, nodata_message
from config import GS_BASE_URL
from . import routes

physiography_api = Blueprint("physiography_api", __name__)

wms_targets = []
wfs_targets = {"ak_level3_ecoregions": "ECOREGION"}


def package_epaecoreg(eco_resp):
    """Package physiography data in dict"""
    title = "EPA Level III Ecoregions of Alaska"
    if eco_resp[0]["features"] == []:
        di = {"title": title, "Data Status": nodata_message}
    else:
        di = {}
        ecoreg = eco_resp[0]["features"][0]["properties"]["ECOREGION"]
        di.update({"title": title, "name": ecoreg})
    return di


@routes.route("/physiography/")
@routes.route("/physiography/abstract/")
def phys_about():
    return render_template("physiography/abstract.html")


@routes.route("/physiography/point/")
def phys_about_point():
    return render_template("physiography/point.html")


@routes.route("/physiography/point/<lat>/<lon>")
def run_fetch_physiography(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/physiography/60.606/-143.345
    """
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(
        fetch_data_api(GS_BASE_URL, "physiography", wms_targets, wfs_targets, lat, lon)
    )
    physio = package_epaecoreg(results)
    return physio
