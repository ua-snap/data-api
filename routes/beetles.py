import asyncio
import io
import csv
import calendar
import numpy as np
from math import floor
from flask import (
    Blueprint,
    render_template,
    request,
    Response
)

# local imports
from fetch_data import (
    fetch_data,
    get_from_dict,
    summarize_within_poly,
    csv_metadata,
    fetch_wcs_point_data,
)
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
    validate_year,
)
from validate_data import (
    get_poly_3338_bbox,
    nullify_and_prune,
    postprocess,
    place_name_and_type,
)
from . import routes
from config import WEST_BBOX, EAST_BBOX

beetles_api = Blueprint("beetles_api", __name__)
# Rasdaman targets
beetle_coverage_id = "template_beetle_risk"

dim_encodings = {
    "models": {
        0: "GFDL-ESM2M",
        1: "HadGEM2-ES",
        2: "MRI-CGCM3",
        3: "NCAR-CCSM4",

    },
    "scenarios": {
        0: "rcp45",
        1: "rcp85",
    },
    "eras": {
        0: "2010-2039",
        1: "2040-2069",
        2: "2070-2099",
    },
    "snowpack": {
        0: "high",
        1: "low",
        2: "medium",
    },
}

def create_csv(
    packaged_data, place_id, lat=None, lon=None
):
    """
    Returns a CSV version of the fetched data, as a string.

    Args:
        packaged_data (json): JSON-like data package output
            from the run_fetch_* and run_aggregate_* functions
        place_id (str): community or area ID unless just a lat/lon value
        lat: latitude unless an area
        lon: longitude unless an area

    Returns:
        string of CSV data
    """

    output = io.StringIO()

    place_name, place_type = place_name_and_type(place_id)

    metadata = csv_metadata(place_name, place_id, place_type, lat, lon)
    metadata += "# Values shown are given as high risk = 0 low risk = 1 and medium risk = 2\n"
    output.write(metadata)

    fieldnames = [
        "era",
        "model",
        "scenario",
        "snowpack-level",
        "beetle-risk",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()

    for era in dim_encodings["eras"].values():
        for model in dim_encodings["models"].values():
            for scenario in dim_encodings["scenarios"].values():
                for snowpack in dim_encodings["snowpack"].values():
                    try:
                        writer.writerow(
                            {
                                "era": era,
                                "model": model,
                                "scenario": scenario,
                                "snowpack-level": snowpack,
                                "beetle-risk": packaged_data[era][model][scenario][snowpack],
                            }
                        )
                    except KeyError:
                        # if single var query, just ignore attempts to
                        # write the non-chosen var
                        pass

    return output.getvalue()


def return_csv(csv_data, place_id, lat=None, lon=None):
    """Return the CSV data as a download

    Args:
        csv_data (?): csv data created with create_csv() function
        place_id (str): community or area ID unless just a lat/lon value
        lat: latitude unless an area
        lon: longitude unless an area

    Returns:
        CSV Response
    """

    place_name, place_type = place_name_and_type(place_id)

    if place_name is not None:
        filename = "Beetle Risk for " + quote(place_name) + ".csv"
    else:
        filename = "Beetle Risk for " + lat + ", " + lon + ".csv"

    response = Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": 'attachment; filename="'
            + filename
            + "\"; filename*=utf-8''\""
            + filename
            + '"',
        },
    )

    return response


def package_beetle_data(beetle_resp):
    """Package the beetle risk data into a nested JSON-like dict.

    Arguments:
        beetle_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all beetle risk values
    """
    # initialize the output dict
    di = dict()

    # Dimensions:
    # era (0 = 2010-2039, 1 = 2040-2069, 2 = 2070-2099)
    # models (0 = GFDL-ESM2M, 1 = HadGEM2-ES, 2 = MRI-CGCM3, 3 = NCAR-CCSM4)
    # scenarios (0 = rcp45, 1 = rcp85)
    # snowpack (0 = high, 1 = low, 2 = medium)
    # risk (1 = low, 2 = medium, 3 = high)
    for ei, mod_li in enumerate(beetle_resp):
        era = dim_encodings["eras"][ei]
        di[era] = dict()
        for mi, sc_li in enumerate(mod_li):
            model = dim_encodings["models"][mi]
            di[era][model] = dict()
            for si, sn_li in enumerate(sc_li):
                scenario = dim_encodings["scenarios"][si]
                di[era][model][scenario] = dict()
                for sni, ri_li in enumerate(sn_li):
                    snowpack = dim_encodings["snowpack"][sni]
                    di[era][model][scenario][snowpack] = beetle_resp[ei][mi][si][sni]

    return di


@routes.route("/beetles/")
@routes.route("/beetles/abstract/")
def about_beetles():
    return render_template("seaice/abstract.html")


@routes.route("/beetles/point/")
def about_beetles_point():
    return render_template("seaice/point.html")


@routes.route("/beetles/point/<lat>/<lon>/")
def run_point_fetch_all_beetles(lat, lon):
    """Run the async request for beetle risk data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of beetle risk for a single lat / lon point.
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
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, beetle_coverage_id))
        beetle_risk = postprocess(package_beetle_data(rasdaman_response), "beetles")
        if request.args.get("format") == "csv":
            if type(beetle_risk) is not dict:
                # Returns errors if any are generated
                return beetle_risk
            # Returns CSV for download
            return return_csv(create_csv(
                postprocess(beetle_risk, "beetles"), None, lat, lon
            ), None, lat, lon)
        # Returns beetle risk levels
        return beetle_risk
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
