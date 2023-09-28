from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)
import asyncio
from aiohttp import ClientSession
from fetch_data import make_get_request
from . import routes

eds_api = Blueprint("eds_api", __name__)


async def fetch_data(url):
    """Wrapper for make_get_request() which gathers and
    executes the urls as asyncio tasks

    Args:
        url (string): URL being requested from API.

    Returns:
        Results of query as JSON
    """
    try:
        async with ClientSession() as session:
            results = await asyncio.create_task(make_get_request(url, session))
        return results
    except:
        # If any of the URLs returns a status other than HTTP status 200,
        # it will return a blank section of the JSON in the place of the
        # ArcticEDS report section.
        return dict()


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
    all_urls = [
        f"{host}eds/temperature/{lat}/{lon}",
        f"{host}eds/precipitation/{lat}/{lon}",
        f"{host}eds/snow/{lat}/{lon}",
        f"{host}eds/degree_days/freezing_index/{lat}/{lon}",
        f"{host}eds/degree_days/heating/{lat}/{lon}",
        f"{host}eds/degree_days/thawing_index/{lat}/{lon}",
        f"{host}geology/point/{lat}/{lon}",
        f"{host}physiography/point/{lat}/{lon}",
        f"{host}eds/permafrost/{lat}/{lon}",
        f"{host}eds/wet_days_per_year/point/{lat}/{lon}",
        f"{host}elevation/point/{lat}/{lon}",
        f"{host}proj_precip/point/{lat}/{lon}",
    ]

    all_keys = [
        "temperature",
        "precipitation",
        "snowfall",
        "freezing_index",
        "heating_degree_days",
        "thawing_index",
        "geology",
        "physiography",
        "permafrost",
        "wet_days_per_year",
        "elevation",
        "proj_precip",
    ]

    results = await asyncio.gather(*[fetch_data(url) for url in all_urls])

    eds = dict()
    for index in range(len(results)):
        eds[all_keys[index]] = results[index]
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
    """

    return asyncio.run(run_fetch_all_eds(lat, lon))
