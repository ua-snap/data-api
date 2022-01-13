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
from luts import ak_veg_di

forest_api = Blueprint("forest_api", __name__)

wms_targets = ["ak_veg_wetland_composite"]
wfs_targets = {}


def package_akvegwetland(akvegwet_resp):
    """Package forest data in dict"""
    title = "Alaska Vegetation and Wetland Composite"
    if akvegwet_resp[0]["features"] == []:
        di = {"title": title, "Data Status": nodata_message}
    else:
        veg_wet_code = akvegwet_resp[0]["features"][0]["properties"]["GRAY_INDEX"]
        if veg_wet_code == 65535:
            di = {"title": title, "Data Status": nodata_message}
        else:
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
            for k in di.keys():
                check_for_nodata(di, k, veg_wet_code, 65535)
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
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(
        fetch_data_api(GS_BASE_URL, "forest", wms_targets, wfs_targets, lat, lon)
    )
    forest_data = package_akvegwetland(results)
    return forest_data
