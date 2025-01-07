import asyncio
from flask import (
    Blueprint,
    render_template,
)

# local imports
from fetch_data import fetch_geoserver_data
from validate_request import validate_latlon
from postprocessing import postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

ecoregions_api = Blueprint("ecoregions_api", __name__)

wms_targets = []
wfs_targets = {"ak_level3_ecoregions": "ECOREGION"}


def package_epaecoreg(eco_resp):
    """Package physiography data in dict"""
    title = "EPA Level III Ecoregions of Alaska"
    if eco_resp[0]["features"] == []:
        return None
    di = {}
    ecoreg = eco_resp[0]["features"][0]["properties"]["ECOREGION"]
    di.update({"title": title, "name": ecoreg})
    return di


@routes.route("/ecoregions/")
@routes.route("/ecoregions/abstract/")
@routes.route("/ecoregions/point/")
def phys_about():
    return render_template("documentation/ecoregions.html")


@routes.route("/ecoregions/point/<lat>/<lon>")
def run_fetch_ecoregions(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/ecoregions/60.606/-143.345
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
            fetch_geoserver_data(
                GS_BASE_URL, "ecoregions", wms_targets, wfs_targets, lat, lon
            )
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
    physio = package_epaecoreg(results)
    return postprocess(physio, "ecoregions")
