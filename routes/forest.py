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
from luts import ak_veg_di

forest_api = Blueprint("forest_api", __name__)

wms_targets = ["ak_veg_wetland_composite"]
wfs_targets = {}


def package_akvegwetland(akvegwet_resp):
    """Package forest data in dict"""
    title = "Alaska Vegetation and Wetland Composite"
    if akvegwet_resp[0]["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        veg_wet_code = akvegwet_resp[0]["features"][0]["properties"]["GRAY_INDEX"]
        if veg_wet_code == 65535:
            di = {"title": title, "Data Status": "No data at this location."}
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
@routes.route("/forest/abstract/")
def forest_about():
    return render_template("forest/abstract.html")


@routes.route("/forest/point/")
def forest_about_point():
    return render_template("forest/point.html")


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
