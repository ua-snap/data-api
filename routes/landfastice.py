import asyncio
import pandas as pd
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from fetch_data import (
    fetch_wcs_point_data,
    get_dim_encodings,
)
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

landfastice_api = Blueprint("landfastice_api", __name__)
landfastice_coverage_id = "landfast_sea_ice_extent"
landfastice_encodings = asyncio.run(
    get_dim_encodings(
        landfastice_coverage_id, scrape=("time", "gmlrgrid:coefficients", 1)
    )
)


def generate_time_index():
    """Generate a Pythonic time index for a single October-July ice season.

    Returns:
        dt_range (pandas DatetimeIndex): a time index with daily frequency
    """

    timestamps = [x[1:-2] for x in landfastice_encodings["time"].split(" ")]
    date_index = pd.DatetimeIndex(timestamps)
    return date_index


def package_landfastice_data(landfastice_resp):
    """Package landfast ice extent data into a nested JSON-like dict.

    Arguments:
        time_index (pandas DateRange object) -- a time index with daily frequency
        landfastice_resp (list) -- the response from the WCS GetCoverage request

    Returns:
        di (dict) -- a dict where the key is a single date and the value is the landfast ice status (1 indicates landfast ice is present)
    """
    time_index = generate_time_index()
    di = {}
    for t, x in zip(list(time_index), landfastice_resp):
        di[t.date().strftime("%m-%d-%Y")] = x
    return di


@routes.route("/landfastice/")
@routes.route("/landfastice/abstract/")
@routes.route("/landfastice/point/")
def about_landfastice():
    return render_template("documentation/landfastice.html")


@routes.route("/landfastice/point/<lat>/<lon>/")
def run_point_fetch_all_landfastice(lat, lon):
    """Run the async request for all landfast ice extent data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of landfast ice extent data
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
        rasdaman_response = asyncio.run(
            fetch_wcs_point_data(x, y, landfastice_coverage_id)
        )
        landfastice_time_series = package_landfastice_data(rasdaman_response)
        if request.args.get("format") == "csv":
            return create_csv(landfastice_time_series, "landfastice", lat=lat, lon=lon)
        return postprocess(landfastice_time_series, "landfastice")
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
