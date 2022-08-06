import asyncio
import numpy as np
from math import floor
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
    validate_seaice_year,
)
from validate_data import nullify_and_prune, postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

seaice_api = Blueprint("seaice_api", __name__)
# rasdaman targets
seaice_coverage_id = "hsia_arctic_production"


def package_seaice_data(seaice_resp):
    """Package the sea ice concentration data into a nested JSON-like dict.

    Arguments:
        seaice_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all SFE values
    """
    # intialize the output dict
    di = dict()
    year = dict()
    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    for i in range(len(seaice_resp)):
        year[f"{months[i%12]}"] = seaice_resp[i]
        if i % 12 == 11:
            di[f"{1850 + floor(i / 12)}"] = year
            year = dict()

    return di


def package_mmm_seaice_data(seaice_resp, start_year=None, end_year=None):
    """Package the sea ice concentration data into a nested JSON-like dict of
    min, mean, and max values.

     Arguments:
         seaice_resp -- the response(s) from the WCS GetCoverage request(s).
         start_year -- starting year to find mmm
         end_year -- ending year to find mmm

     Returns:
         di -- a nested dictionary of all sea ice concentration values
    """
    # intialize the output dict
    di = dict()
    start_index = 0
    end_index = len(seaice_resp)
    if start_year != None:
        start_index = (int(start_year) - 1850) * 12
    if end_year != None:
        end_index = (int(end_year) - 1850) * 12

    di["seaice_min"] = min(seaice_resp[start_index:end_index])
    di["seaice_max"] = max(seaice_resp[start_index:end_index])
    di["seaice_mean"] = round(np.mean(seaice_resp[start_index:end_index]))

    return di


@routes.route("/mmm/seaice/")
def about_mmm_seaice():
    return render_template("mmm/seaice.html")


@routes.route("/mmm/seaice/<lat>/<lon>")
@routes.route("/mmm/seaice/<lat>/<lon>/<start_year>/<end_year>")
def run_mmm_point_fetch_all_seaice(lat, lon, start_year=None, end_year=None):
    """Run the async request for sea ice concentration data at a single point.
    Finds minimum, maximum, and mean for date range if supplied.

     Args:
         lat (float): latitude
         lon (float): longitude
         start_year (int): starting year to find mmm
         end_year(int): ending year to find mmm

     Returns:
         JSON-like dict of min, mean, and max of sea ice concentration data
    """
    validation = validate_latlon(lat, lon)
    date_validation = validate_seaice_year(start_year, end_year)
    if validation == 400 or date_validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    x, y = project_latlon(lat, lon, 3572)
    try:
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, seaice_coverage_id))
        return package_mmm_seaice_data(rasdaman_response, start_year, end_year)
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/seaice/<lat>/<lon>/<hsia>")
def run_point_fetch_all_seaice(lat, lon, hsia=None):
    """Run the async request for sea ice concentration data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of sea ice concentration data
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
    x, y = project_latlon(lat, lon, 3572)
    try:
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, seaice_coverage_id))
        if (hsia is not None):
            return rasdaman_response
        else:
            return package_seaice_data(rasdaman_response)
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
