import asyncio
from flask import (
    Blueprint,
    render_template,
)

# local imports
from fetch_data import fetch_data_api
from validate_request import validate_latlon
from validate_data import nullify_nodata, postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from luts import ak_veg_di
from . import routes

forest_api = Blueprint("forest_api", __name__)

wms_targets = ["ak_veg_wetland_composite"]
wfs_targets = {}


def package_akvegwetland(akvegwet_resp):
    """Package forest data in dict"""
    title = "Alaska Vegetation and Wetland Composite"
    if akvegwet_resp[0]["features"] == []:
        return None
    veg_wet_code = akvegwet_resp[0]["features"][0]["properties"]["GRAY_INDEX"]
    veg_wet_code = nullify_nodata(veg_wet_code, "forest")
    if veg_wet_code is None:
        return None
    finelc = ak_veg_di[veg_wet_code]["Fine_LC"]
    coarselc = ak_veg_di[veg_wet_code]["Coarse_LC"]
    nwi = ak_veg_di[veg_wet_code]["NWI_Gen"]
    di = {
        "title": title,
        "code": veg_wet_code,
        "finelc": finelc,
        "coarselc": coarselc,
        "nwi": nwi,
    }
    return di


@routes.route("/forest/")
@routes.route("/vegetation/")
@routes.route("/forest/abstract/")
@routes.route("/vegetation/abstract/")
def forest_about():
    return render_template("forest/abstract.html")


@routes.route("/vegetation/point/")
@routes.route("/forest/point/")
def forest_about_point():
    return render_template("forest/point.html")


@routes.route("/vegetation/point/<lat>/<lon>")
@routes.route("/forest/point/<lat>/<lon>")
def run_fetch_forest(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/forest/60.606/-143.345
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
            fetch_data_api(GS_BASE_URL, "forest", wms_targets, wfs_targets, lat, lon)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
    data = package_akvegwetland(results)
    return postprocess(data, "forest")
