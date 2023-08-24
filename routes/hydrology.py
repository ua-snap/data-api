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
)
from generate_requests import generate_wcs_getcov_str
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import nullify_and_prune, postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

hydrology_api = Blueprint("hydrology_api", __name__)
# rasdaman targets
hydrology_coverage_id = "hydrology_test"

dim_encodings = {
    "variables": {
        0: "sm1",
        1: "sm2",
        2: "sm3",
        3: "tmax",
        4: "tmin",
        5: "iwe",
        6: "swe",
        7: "evap",
        8: "glacier_melt",
        9: "pcp",
        10: "runoff",
        11: "snow_melt",
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
    """Fetch projected precipitation data for a
       given latitude and longitude.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and
        longitude
    """
    x, y = project_latlon(lat, lon, 3338)
    rasdaman_response = list()

    # Due to the large amount of dimensions, it was found that splitting up
    # our Rasdaman requests across each of the variable dimension (12 different values)
    # and recombining on the API was much faster than simply requesting all of the
    # data at once.
    for variable in range(12):
        rasdaman_response.append(
            asyncio.run(
                fetch_wcs_point_data(
                    x, y, hydrology_coverage_id, var_slice=("variable", variable)
                )
            )
        )

    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = dict()
    for variable in range(len(dim_encodings["variables"])):
        variable_key = dim_encodings["variables"][variable]
        point_pkg[variable_key] = dict()
        for model in range(len(dim_encodings["models"])):
            model_key = dim_encodings["models"][model]
            point_pkg[variable_key][model_key] = dict()
            for scenario in range(len(dim_encodings["scenarios"])):
                scenario_key = dim_encodings["scenarios"][scenario]
                point_pkg[variable_key][model_key][scenario_key] = dict()
                for month in range(len(dim_encodings["months"])):
                    month_key = dim_encodings["months"][month]
                    point_pkg[variable_key][model_key][scenario_key][month_key] = dict()
                    for era in range(len(dim_encodings["eras"])):
                        era_key = dim_encodings["eras"][era]
                        point_pkg[variable_key][model_key][scenario_key][month_key][
                            era_key
                        ] = rasdaman_response[variable][model][scenario][month][era]

    return point_pkg


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
    try:
        point_pkg = run_fetch_hydrology_point_data(lat, lon)
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    if request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(point_pkg, "hydrology")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        place_id = request.args.get("community")
        return create_csv(point_pkg, "hydrology", place_id, lat, lon)

    return postprocess(point_pkg, "hydrology")
