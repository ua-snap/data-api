from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)
import asyncio
import os
from fetch_data import fetch_data
from . import routes

eds_api = Blueprint("eds_api", __name__)


async def run_fetch_all_eds(lat, lon):
    """
    Fetches a list of data API end points to generate a multiple
    variable JSON that can be used to generate a full report in the
    Arctic EDS website.

    Args:
        lat (float): latitude
        lon (float): longitude


    Returns:
        Multiple variable JSON object
    """
    host = request.host_url
    all_requests = [
        f"{host}eds/temperature/{lat}/{lon}",
        f"{host}eds/precipitation/{lat}/{lon}",
        f"{host}mmm/snow/snowfallequivalent/hp/{lat}/{lon}",
        f"{host}design_index/freezing/hp/point/{lat}/{lon}",
        f"{host}design_index/thawing/hp/point/{lat}/{lon}",
        f"{host}eds/degree_days/freezing_index/{lat}/{lon}",
        f"{host}eds/degree_days/heating/{lat}/{lon}",
        f"{host}eds/degree_days/thawing_index/{lat}/{lon}",
        f"{host}geology/point/{lat}/{lon}",
        f"{host}physiography/point/{lat}/{lon}",
        f"{host}permafrost/point/{lat}/{lon}",
    ]

    eds = await asyncio.gather(*[fetch_data([request]) for request in all_requests])

    return eds


@routes.route("/eds/all/<lat>/<lon>")
def fetch_all_eds(lat, lon):
    """
    Endpoint for requesting all data required for the Arctic EDS reports.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/eds/all/68.0764/-154.5501

        Order of returned JSON
        0 - Temperature
        1 - Precipitation
        2 - Snowfall
        3 - Design Freezing Index
        4 - Design Thawing Index
        5 - Freezing Index
        6 - Heating Degree Days
        7 - Thawing Index
        8 - Geology
        9 - Physiography
       10 - Permafrost
    """

    return asyncio.run(run_fetch_all_eds(lat, lon))
