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
    describe_via_wcps,
)
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_xy_in_coverage_extent,
)
from postprocessing import postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

landfastice_api = Blueprint("landfastice_api", __name__)

beaufort_daily_slie_id = "ardac_beaufort_daily_slie"
chukchi_daily_slie_id = "ardac_chukchi_daily_slie"
# get the description of coverages from rasdaman
beaufort_meta = asyncio.run(describe_via_wcps(beaufort_daily_slie_id))
chukchi_meta = asyncio.run(describe_via_wcps(chukchi_daily_slie_id))


def generate_time_index_from_coverage_metadata(meta):
    """Generate a pandas DatetimeIndex from the ansi (i.e. time) axis coordinates in the coverage description metadata.

    Args:
        meta (dict): JSON-like dictionary containing coverage metadata

    Returns:
        pd.DatetimeIndex: corresponding to the ansi (i.e. time) axis coordinates
    """
    try:
        # we won't always know the axis positioning / ordering
        ansi_axis = next(
            axis
            for axis in meta["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "ansi"
        )
        # this is a list of dates formatted like "1996-11-03T00:00:00.000Z"
        ansi_coordinates = ansi_axis["coordinate"]
        date_index = pd.DatetimeIndex(ansi_coordinates)
        return date_index

    except (KeyError, StopIteration):
        raise ValueError("Unexpected coverage metadata: 'ansi' axis not found")


def package_landfastice_data(landfastice_resp, meta):
    """Package landfast ice extent data into a JSON-like dict.

    Arguments:
        landfastice_resp (list) -- the response from the WCS GetCoverage request

    Returns:
        di (dict) -- a dict where the key is a single date and the value is the landfast ice status (1 indicates landfast ice is present)
    """
    time_index = generate_time_index_from_coverage_metadata(meta)
    di = {}
    for dt, ice_value in zip(list(time_index), landfastice_resp):
        di[dt.date().strftime("%m-%d-%Y")] = ice_value
    return di


@routes.route("/landfastice/")
@routes.route("/landfastice/abstract/")
@routes.route("/landfastice/point/")
def about_landfastice():
    return render_template("documentation/landfastice.html")


@routes.route("/landfastice/point/<lat>/<lon>/")
def run_point_fetch_all_landfastice(lat, lon):
    """Run the async request for all landfast ice data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of landfast ice extent data
    """
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400

    x, y = project_latlon(lat, lon, 3338)

    if validate_xy_in_coverage_extent(x, y, beaufort_meta):
        target_coverage = beaufort_daily_slie_id
        target_meta = beaufort_meta
    elif validate_xy_in_coverage_extent(x, y, chukchi_meta):
        target_coverage = chukchi_daily_slie_id
        target_meta = chukchi_meta
    else:
        return "out of coverage"
        # return render_template("422/invalid"), 422
        # maybe return 422 invalid lat-lon instead?

    # try:
    rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, target_coverage))
    landfastice_time_series = package_landfastice_data(rasdaman_response, target_meta)
    postprocessed = postprocess(landfastice_time_series, "landfast_sea_ice")
    if request.args.get("format") == "csv":
        return create_csv(postprocessed, "landfast_sea_ice", lat=lat, lon=lon)
    return postprocessed
    # except Exception as exc:
    #     if hasattr(exc, "status") and exc.status == 404:
    #         return render_template("404/no_data.html"), 404
    #     return render_template("500/server_error.html"), 500
