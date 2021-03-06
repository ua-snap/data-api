import asyncio
import numpy as np
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from fetch_data import (
    fetch_wcs_point_data,
    get_dim_encodings,
    deepflatten,
    build_csv_dicts,
    write_csv,
)
from validate_request import (
    validate_latlon,
    project_latlon,
)
from validate_data import nullify_and_prune, postprocess
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


def create_csv(data_pkg, lat=None, lon=None):
    """Create CSV file with metadata string and location based filename.
    Args:
        data_pkg (dict): JSON-like object of data
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons
    Returns:
        CSV response object
    """
    fieldnames = [
        "model",
        "scenario",
        "decade",
        "variable",
        "value",
    ]
    csv_dicts = build_csv_dicts(
        data_pkg,
        fieldnames,
    )
    metadata = "#SFE is the total annual snowfall equivalent in millimeters for the specified model-scenario-decade\n"
    filename = "SFE for " + lat + ", " + lon + ".csv"
    return write_csv(csv_dicts, fieldnames, filename, metadata)


@routes.route("/mmm/snow/")
@routes.route("/mmm/snow/snowfallequivalent/")
def about_mmm_snow():
    return render_template("mmm/snow.html")


@routes.route("/mmm/snow/snowfallequivalent/<lat>/<lon>")
# the above route is included to avoid a 500 error when horp is omitted entirely
@routes.route("/mmm/snow/snowfallequivalent/<horp>/<lat>/<lon>")
def run_point_fetch_all_sfe(lat, lon, horp="hp"):
    """Run the async request for SFE data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude
        horp (string): one of "historical", "projected", "hp", or "all")

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
    try:
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, sfe_coverage_id))
        horp_case_di = {
            "all": package_sfe_data(rasdaman_response),
            "hp": summarize_mmm_sfe(package_sfe_data(rasdaman_response)),
            "historical": {
                "historical": summarize_mmm_sfe(package_sfe_data(rasdaman_response))[
                    "historical"
                ]
            },
            "projected": {
                "projected": summarize_mmm_sfe(package_sfe_data(rasdaman_response))[
                    "projected"
                ]
            },
        }
        try:
            if horp == "all" and request.args.get("format") == "csv":
                point_pkg = nullify_and_prune(horp_case_di[horp], "snow")
                if point_pkg in [{}, None, 0]:
                    return render_template("404/no_data.html"), 404
                return create_csv(point_pkg, lat, lon)
            else:
                return postprocess(horp_case_di[horp], "snow")
        except KeyError:
            return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
