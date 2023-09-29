import asyncio
from urllib.parse import quote
from flask import (
    Blueprint,
    render_template,
    request,
    current_app as app,
    jsonify,
)

# local imports
from fetch_data import fetch_wcs_point_data, fetch_data
from validate_request import (
    validate_latlon,
    project_latlon,
)
from generate_requests import generate_average_wcps_str
from generate_urls import generate_wcs_query_url
from validate_data import *
from postprocessing import postprocess
from csv_functions import create_csv
from config import WEST_BBOX, EAST_BBOX
from . import routes

hydrology_api = Blueprint("hydrology_api", __name__)
hydrology_coverage_id = "hydrology"

dim_encodings = {
    "varnames": {
        0: "evap",
        1: "glacier_melt",
        2: "iwe",
        3: "pcp",
        4: "runoff",
        5: "sm1",
        6: "sm2",
        7: "sm3",
        8: "snow_melt",
        9: "swe",
        10: "tmax",
        11: "tmin",
    },
    "models": {
        0: "ACCESS1-3",
        1: "CCSM4",
        2: "CSIRO-Mk3-6-0",
        3: "CanESM2",
        4: "GFDL-ESM2M",
        5: "HadGEM2-ES",
        6: "MIROC5",
        7: "MPI-ESM-MR",
        8: "MRI-CGCM3",
        9: "inmcm4",
    },
    "scenarios": {
        0: "rcp45",
        1: "rcp85",
    },
    "months": {
        0: "apr",
        1: "aug",
        2: "dec",
        3: "feb",
        4: "jan",
        5: "jul",
        6: "jun",
        7: "mar",
        8: "may",
        9: "nov",
        10: "oct",
        11: "sep",
    },
    "eras": {
        0: "1950-1959",
        1: "1960-1969",
        2: "1970-1979",
        3: "1980-1989",
        4: "1990-1999",
        5: "2000-2009",
        6: "2010-2019",
        7: "2020-2029",
        8: "2030-2039",
        9: "2040-2049",
        10: "2050-2059",
        11: "2060-2069",
        12: "2070-2079",
        13: "2080-2089",
        14: "2090-2099",
    },
}


def run_fetch_hydrology_point_data(lat, lon):
    """Fetch all hydrology data for a
       given latitude and longitude.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and
        longitude for all variables
    """
    x, y = project_latlon(lat, lon, 3338)

    rasdaman_response = asyncio.run(
        fetch_wcs_point_data(
            x,
            y,
            hydrology_coverage_id,
            var_coord=None,
        )
    )

    # package point data with decoded coord values (names)
    point_pkg = dict()
    for model_coord in dim_encodings["models"].keys():
        model_name = dim_encodings["models"][model_coord]
        point_pkg[model_name] = dict()
        for scenario_coord in dim_encodings["scenarios"].keys():
            scenario_name = dim_encodings["scenarios"][scenario_coord]
            point_pkg[model_name][scenario_name] = dict()
            for month_coord in dim_encodings["months"].keys():
                month_name = dim_encodings["months"][month_coord]
                point_pkg[model_name][scenario_name][month_name] = dict()
                for era_coord in dim_encodings["eras"].keys():
                    era_name = dim_encodings["eras"][era_coord]
                    point_pkg[model_name][scenario_name][month_name][era_name] = dict()
                    for var_coord in dim_encodings["varnames"].keys():
                        var_name = dim_encodings["varnames"][var_coord]
                        point_pkg[model_name][scenario_name][month_name][era_name][
                            var_name
                        ] = rasdaman_response[model_coord][scenario_coord][month_coord][
                            era_coord
                        ].split(
                            " "
                        )[
                            var_coord
                        ]

    return point_pkg


### unfinished function, need to fix WCPS request string
def run_fetch_hydrology_point_data_mmm(lat, lon):
    """Fetch summarized hydrology data for a
       given latitude and longitude.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and
        longitude for all variables
    """
    x, y = project_latlon(lat, lon, 3338)

    wcps_str = generate_average_wcps_str(x, y, hydrology_coverage_id, "eras", (0, 14))
    wcps_query_url = generate_wcs_query_url(wcps_str)
    rasdaman_response = asyncio.run(fetch_data([wcps_query_url]))
    return rasdaman_response

    # package point data with decoded coord values (names)
    # point_pkg = dict()
    # return point_pkg


@routes.route("/hydrology/")
@routes.route("/hydrology/abstract/")
@routes.route("/hydrology/point/")
def hydro_about():
    return render_template("documentation/hydrology.html")


@routes.route("/hydrology/point/<lat>/<lon>")
def run_get_hydrology_point_data(lat, lon):
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

    # validate request arguments if they exist; set summarize argument accordingly
    if len(request.args) == 0:
        try:
            point_pkg = run_fetch_hydrology_point_data(lat, lon)
            return postprocess(point_pkg, "hydrology")

        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500

    elif all(key in request.args for key in ["summarize", "format"]):
        if (request.args.get("summarize") == "mmm") & (
            request.args.get("format") == "csv"
        ):
            #### run_fetch_hydrology_point_data_mmm() and create_csv() routine
            return "mmm and csv route"
        else:
            return render_template("400/bad_request.html"), 400

    elif "summarize" in request.args:
        if request.args.get("summarize") == "mmm":
            #### run_fetch_hydrology_point_data_mmm() and return JSON
            return "mmm only route"
        else:
            return render_template("400/bad_request.html"), 400

    elif "format" in request.args:
        if request.args.get("format") == "csv":
            point_pkg = run_fetch_hydrology_point_data(lat, lon)
            place_id = request.args.get("community")
            if place_id:
                return create_csv(
                    point_pkg, "hydrology", place_id=place_id, lat=lat, lon=lon
                )
            return create_csv(point_pkg, "hydrology", place_id=None, lat=lat, lon=lon)

    else:
        return render_template("400/bad_request.html"), 400
