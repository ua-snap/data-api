import asyncio
import logging
from flask import Blueprint, render_template, request


from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import (
    fetch_data,
    describe_via_wcps,
)
from validate_request import (
    project_latlon,
    generate_time_index_from_coverage_metadata,
)

from csv_functions import create_csv
from . import routes

tmy_api = Blueprint("tmy_api", __name__)

tmy_coverage_id = "cp_test_tmy"

tmy_meta = asyncio.run(describe_via_wcps(tmy_coverage_id))

logger = logging.getLogger(__name__)


def package_tmy_point_data(data_response, coverage_meta):
    """Package TMY data with time-first structure.

    Args:
        data_response (list): data
        coverage_meta (dict): Coverage metadata containing time axis

    Returns:
        dict: Time-first structured data {date: value}
    """

    time_index = generate_time_index_from_coverage_metadata(coverage_meta)

    # package data with time keys at top level
    packaged_data = {}
    for i, timestamp in enumerate(time_index):
        date_key = timestamp.strftime("%Y-%m-%d")
        packaged_data[date_key] = data_response[i]

    return packaged_data


@routes.route("/tmy/")
@routes.route("/tmy/abstract/")
@routes.route("/tmy/point/")
def tmy_about():
    return render_template("documentation/tmy.html")


async def fetch_tmy_point_data(x, y):
    """Fetch TMY data asynchronously.

    Args:
        x (float): x-coordinate
        y (float): y-coordinate

    Returns:
        list: TMY data arrays
    """

    tasks = []
    request_str = generate_wcs_getcov_str(x, y, tmy_coverage_id)
    url = generate_wcs_query_url(request_str)
    tasks.append(fetch_data([url]))

    results = await asyncio.gather(*tasks)
    return results[0]


@routes.route("/tmy/point/<lat>/<lon>")
def tmy_point(lat, lon):
    """TMY point data endpoint.
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        JSON-like object of TMY data
    """

    x, y = project_latlon(lat, lon, 3338)

    all_data = asyncio.run(fetch_tmy_point_data(x, y))

    packaged_data = package_tmy_point_data(all_data, tmy_meta)

    # if request.args.get("format") == "csv":
    #     place_id = request.args.get("community")
    #     return create_csv(
    #         packaged_data, "tmy_4km", place_id=place_id, lat=lat, lon=lon
    #     )
    return packaged_data
    # return all_data
