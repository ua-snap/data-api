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
    return asyncio.run(run_fetch_all_eds(lat, lon))
