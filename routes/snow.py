import asyncio
import numpy as np
from flask import Blueprint, render_template, request, jsonify

# local imports
from fetch_data import (
    fetch_wcs_point_data,
    get_dim_encodings,
    deepflatten,
)
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import nullify_and_prune, postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

snow_api = Blueprint("snow_api", __name__)
# rasdaman targets
sfe_coverage_id = "mean_annual_snowfall_mm"


def package_sfe_data(sfe_resp):
    """Package the SFE data into a nested JSON-like dict.

    Arguments:
        sfe_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all SFE values
    """
    # intialize the output dict
    sfe_encodings = asyncio.run(get_dim_encodings(sfe_coverage_id))
    models = list(sfe_encodings["model"].values())
    scenarios = list(sfe_encodings["scenario"].values())
    decades = list(sfe_encodings["decade"].values())
    di = {
        m: {sc: {dec: {"SFE": None} for dec in decades} for sc in scenarios}
        for m in models
    }
    # populate the dict with the response
    flat_list = list(deepflatten(sfe_resp))
    i = 0
    for model in di.keys():
        for scenario in di[model].keys():
            for decade in di[model][scenario]:
                di[model][scenario][decade]["SFE"] = flat_list[i]
                i += 1
    # remove the nonsense encoding combinations
    # e.g., CRU-TS/RCP8.5 or NCAR-CCSM4/1920-1929
    projection_decades = [
        "2010-2019",
        "2020-2029",
        "2030-2039",
        "2040-2049",
        "2050-2059",
        "2060-2069",
        "2070-2079",
        "2080-2089",
        "2090-2099",
    ]
    historical_decades = list(set(decades) - set(projection_decades))
    di["CRU-TS"].pop("rcp45", None)
    di["CRU-TS"].pop("rcp60", None)
    di["CRU-TS"].pop("rcp85", None)
    for proj_dec in projection_decades:
        di["CRU-TS"]["historical"].pop(proj_dec, None)
    for model in ["GFDL-CM3", "GISS-E2-R", "IPSL-CM5A-LR", "MRI-CGCM3", "NCAR-CCSM4"]:
        di[model].pop("historical")
        for scenario in ["rcp45", "rcp60", "rcp85"]:
            for hist_dec in historical_decades:
                di[model][scenario].pop(hist_dec, None)
    return di


def summarize_mmm_sfe(all_sfe_di):
    """Generate min-mean-max summaries of the historical and projected SFE data.

    Arguments:
        all_sfe_di -- the intial nested dict package of all SFE data

    Returns:
        mmm_sfe_di -- a nested dict that is a subset of the intial package
    """
    mmm_sfe_di = {}
    mmm_sfe_di["historical"] = {}
    mmm_sfe_di["projected"] = {}
    hist_vals = [
        all_sfe_di["CRU-TS"]["historical"][k]["SFE"]
        for k in all_sfe_di["CRU-TS"]["historical"].keys()
    ]
    mmm_sfe_di["historical"]["sfemin"] = min(hist_vals)
    mmm_sfe_di["historical"]["sfemax"] = max(hist_vals)
    mmm_sfe_di["historical"]["sfemean"] = round(np.mean(hist_vals))
    proj_vals = []
    for model in ["GFDL-CM3", "GISS-E2-R", "IPSL-CM5A-LR", "MRI-CGCM3", "NCAR-CCSM4"]:
        for scenario in ["rcp45", "rcp60", "rcp85"]:
            model_scenario_vals = [
                all_sfe_di[model][scenario][k]["SFE"]
                for k in all_sfe_di[model][scenario].keys()
            ]
            for mod_sc_val in model_scenario_vals:
                proj_vals.append(mod_sc_val)
    mmm_sfe_di["projected"]["sfemin"] = min(proj_vals)
    mmm_sfe_di["projected"]["sfemax"] = max(proj_vals)
    mmm_sfe_di["projected"]["sfemean"] = round(np.mean(proj_vals))
    return mmm_sfe_di


@routes.route("/eds/snow/<lat>/<lon>")
def eds_snow_data(lat, lon):
    snow = dict()

    summary = run_point_fetch_all_sfe(lat, lon, summarize=True)
    # Check for error response from summary response
    if isinstance(summary, tuple):
        return summary

    snow["summary"] = summary

    preview = run_point_fetch_all_sfe(lat, lon, preview=True)
    # Check for error responses in the preview
    if isinstance(preview, tuple):
        # Returns error template that was generated for invalid request
        return preview

    snow_csv = preview.data.decode("utf-8")
    first = "\n".join(snow_csv.split("\n")[3:9]) + "\n"
    last = "\n".join(snow_csv.split("\n")[-6:])

    snow["preview"] = first + last

    return jsonify(snow)


@routes.route("/snow/")
def about_mmm_snow():
    return render_template("documentation/snow.html")


@routes.route("/snow/snowfallequivalent/<lat>/<lon>")
def run_point_fetch_all_sfe(lat, lon, summarize=None, preview=None):
    """Run the async request for SFE data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of SFE data
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
    x, y = project_latlon(lat, lon, 3338)

    # validate args explicitly

    try:
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, sfe_coverage_id))
        # if summarize or preview, return either mmm summary or CSV
        # the preview and summary args should be mutually exclusive, and should never occur with additional request args
        if summarize:
            point_pkg = summarize_mmm_sfe(package_sfe_data(rasdaman_response))
            return postprocess(point_pkg, "snow")
        if preview:
            try:
                point_pkg = package_sfe_data(rasdaman_response)
                point_pkg = nullify_and_prune(point_pkg, "snow")
                if point_pkg in [{}, None, 0]:
                    return render_template("404/no_data.html"), 404
                return create_csv(point_pkg, "snow", lat=lat, lon=lon)
            except KeyError:
                return render_template("400/bad_request.html"), 400

        # if no request args, return unsummarized data package
        if len(request.args) == 0:
            point_pkg = package_sfe_data(rasdaman_response)
            return postprocess(point_pkg, "snow")

        # if there are request args, validate them
        elif all(key in request.args for key in ["summarize", "format"]):
            pass
        else:
            return render_template("400/bad_request.html"), 400

        # if valid args for only mmm, return distilled tidy package
        if "summarize" in request.args:
            if request.args.get("summarize") == "mmm":
                point_pkg = summarize_mmm_sfe(package_sfe_data(rasdaman_response))
                return postprocess(point_pkg, "snow")
            else:
                return render_template("400/bad_request.html"), 400

        # if valid args for only csv, return unsummarized CSV
        if "format" in request.args:
            if request.args.get("format") == "csv":
                try:
                    point_pkg = package_sfe_data(rasdaman_response)
                    point_pkg = nullify_and_prune(point_pkg, "snow")
                    if point_pkg in [{}, None, 0]:
                        return render_template("404/no_data.html"), 404
                    return create_csv(point_pkg, "snow", lat=lat, lon=lon)
                except KeyError:
                    return render_template("400/bad_request.html"), 400
            else:
                return render_template("400/bad_request.html"), 400

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
