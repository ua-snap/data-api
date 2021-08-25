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

physiography_api = Blueprint("physiography_api", __name__)

wms_targets = []
wfs_targets = {"ak_level3_ecoregions": "ECOREGION"}


def package_epaecoreg(eco_resp):
    """Package physiography data in dict"""
    title = "EPA Level III Ecoregions of Alaska"
    if eco_resp[0]["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
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
