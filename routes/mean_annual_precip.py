import asyncio
from flask import (
    Blueprint,
    render_template,
)

# local imports
from fetch_data import fetch_data_api
from validate_latlon import validate
from validate_data import postprocess
from config import GS_BASE_URL, VALID_BBOX
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
        if not decadal_mapr_resp[i]["features"] == []:
            pr_mm = decadal_mapr_resp[i]["features"][0]["properties"]["GRAY_INDEX"]
            di = {
                "title": title,
                "dec_start": dec_start,
                "dec_end": dec_end,
                "model": model,
                "scenario": scenario,
                "pr_mm": pr_mm,
            }
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
        return render_template("404/invalid_latlon.html", bbox=VALID_BBOX), 404

    try:
        results = asyncio.run(
            fetch_data_api(
                GS_BASE_URL, "mean_annual_precip", wms_targets, wfs_targets, lat, lon
            )
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        raise

    data = {
        "dec_mapr": package_decadal_mapr(results[0:2]),
    }

    return postprocess(data, "mean_annual_precip")
