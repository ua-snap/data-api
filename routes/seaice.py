import asyncio
from math import floor
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from fetch_data import (
    fetch_wcs_point_data,
    describe_via_wcps,
)
from csv_functions import create_csv
from validate_request import (
    validate_seaice_latlon,
    project_latlon,
    get_axis_encodings,
)
from validate_data import validate_seaice_timestring
from postprocessing import postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

seaice_api = Blueprint("seaice_api", __name__)
# Rasdaman targets
seaice_coverage_id = "hsia_arctic_production"


async def get_seaice_metadata():
    """Get the coverage metadata and encodings for HSIA seaice coverage"""
    metadata = await describe_via_wcps(seaice_coverage_id)
    return get_axis_encodings(metadata)


def package_seaice_data(seaice_resp):
    """Package the sea ice concentration data into a nested JSON-like dict.

    Arguments:
        seaice_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all SFE values
    """
    # initialize the output dict
    di = dict()

    # For each year and month, checks to see if sea ice percentage is greater than 100,
    # and sets the value of the percentage for each month to the returned value or 0.
    for i in range(len(seaice_resp)):
        di[f"{1850 + floor(i / 12)}-{str((i%12) + 1).zfill(2)}"] = (
            seaice_resp[i]
            if (seaice_resp[i] is not None and seaice_resp[i] <= 100)
            else 0
        )

    return di


@routes.route("/seaice/")
@routes.route("/seaice/abstract/")
@routes.route("/seaice/point/")
def about_seaice():
    return render_template("documentation/seaice.html")


@routes.route("/seaice/point/<lat>/<lon>/")
def run_point_fetch_all_seaice(lat, lon):
    """Run the async request for sea ice concentration data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of sea ice concentration data
    """
    validation = validate_seaice_latlon(lat, lon, [seaice_coverage_id])
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 404:
        return (
            render_template("404/no_data.html"),
            404,
        )
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
        seaice_conc = postprocess(package_seaice_data(rasdaman_response), "seaice")
        if request.args.get("format") == "csv":
            if type(seaice_conc) is not dict:
                # Returns errors if any are generated
                return seaice_conc
            # Returns CSV for download
            data = postprocess(package_seaice_data(rasdaman_response), "seaice")
            return create_csv(data, "seaice", lat=lat, lon=lon)
        # Returns sea ice concentrations across years & months
        return postprocess(package_seaice_data(rasdaman_response), "seaice")
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/seaice/enddate/")
def seaice_enddate():
    """Get the latest date of sea ice concentration data available in the Rasdaman server.
    Args:
        None

    Returns:
        JSON-like dict of the latest year and month of sea ice concentration data

    """
    hsia_encodings = asyncio.run(get_seaice_metadata())
    latest_date = hsia_encodings["ansi"][-1]

    try:
        valid_date = validate_seaice_timestring(latest_date)
        return {"year": valid_date.year, "month": valid_date.month}
    except Exception:
        return render_template("500/server_error.html"), 500
