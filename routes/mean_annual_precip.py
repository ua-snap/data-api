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

mean_annual_precip_api = Blueprint("mean_annual_precip_api", __name__)

wms_targets = [
    "pr_decadal_mean_annual_total_mm_5modelAvg_rcp60_2050_2059",
    "pr_decadal_mean_annual_total_mm_5modelAvg_rcp85_2050_2059",
]
wfs_targets = {}


def package_decadal_mapr(decadal_mapr_resp):
    """Package Mean Annual Precipitation (mm) Decadal Summaries"""
    dec_mapr = []

    for i, j in enumerate(wms_targets[0:2]):

        model = j.split("_")[-4]
        scenario = j.split("_")[-3]
        dec_start = j.split("_")[-2]
        dec_end = j.split("_")[-1]
        title = f"Mean Annual Precipitation (mm) Decadal Summary Projection for {dec_start}-{dec_end}"
        if decadal_mapr_resp[i]["features"] == []:
            di = {"title": title, "Data Status": nodata_message}
            dec_mapr.append(di)
        else:
            pr_mm = decadal_mapr_resp[i]["features"][0]["properties"]["GRAY_INDEX"]
            di = {
                "title": title,
                "dec_start": dec_start,
                "dec_end": dec_end,
                "model": model,
                "scenario": scenario,
                "pr_mm": pr_mm,
            }
            check_for_nodata(di, "pr_mm", pr_mm, -9999)
            dec_mapr.append(di)
    return dec_mapr


@routes.route("/mean_annual_precip/")
@routes.route("/mean_annual_precip/abstract/")
def mapr_about():
    return render_template("mean_annual_precip/abstract.html")


@routes.route("/mean_annual_precip/point/")
def mapr_about_point():
    return render_template("mean_annual_precip/point.html")


@routes.route("/mean_annual_precip/point/<lat>/<lon>")
def run_fetch_mapr_data(lat, lon):
    """Run the async mean annual precipitation data requesting and return data as json
    example request: http://localhost:5000/mean_annual_precipitation/65.0628/-146.1627"""
    if not validate(lat, lon):
        abort(400)
    results = asyncio.run(
        fetch_data_api(
            GS_BASE_URL, "mean_annual_precip", wms_targets, wfs_targets, lat, lon
        )
    )
    dec_mapr = package_decadal_mapr(results[0:2])
    data = {
        "dec_mapr": dec_mapr,
    }
    return data
