from flask import Blueprint, current_app as app, Response
import asyncio
import json
import requests
import time
import logging
from . import routes
from luts import host, cached_urls
from generate_urls import generate_wfs_places_url
from fetch_data import fetch_data
from .vectordata import filter_by_tag

recache_api = Blueprint("recache_api", __name__)

logging.basicConfig(
    filename="output.log",  # File to write logs to
    filemode="a",  # Append mode
    level=logging.INFO,  # Logging level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log message format
)


def all_routes():
    """Generates all routes defined in the Flask app

    Args:
        None.

    Returns:
        A list of all routes from the API
    """
    all_routes = []
    for rule in app.url_map.iter_rules():
        all_routes.append(rule.rule)
    return all_routes


def log_error(url, status):
    """Logs any errors during HTTP request during the re-caching process

    Args:
        url - The URL that caused the status
        status - Python requests status object
    """
    app.logger.warning(str(status) + ": " + url)


def get_endpoint(curr_route, curr_type, place):
    """Requests a specific endpoint of the API with parameters coming from
    the JSON of communities, HUCs, or protected areas.

     Args:
         curr_route - Current route ex. https://earthmaps.io/taspr/huc/
         curr_type - One of three types: community, huc, or pa
         place - One item of the JSON for community, huc, or protected area

     Returns:
         Nothing.

    """
    # Build the URL to query based on type
    if curr_type == "community":
        url = host + curr_route + str(place["latitude"]) + "/" + str(place["longitude"])
    else:
        url = host + curr_route + str(place["id"])

    start_time = time.time()
    logging.info(f"Requesting URL: {url}")
    # Collects returned status from GET request
    status = requests.get(url)
    logging.info(f"Response Status: {status.status_code}")
    logging.info(f"Time to get {url} endpoint: {time.time() - start_time:.2f} seconds")

    # Logs the status and URL if the HTTP status code != 200
    if status.status_code != 200:
        log_error(url, status.status_code)


def get_all_route_endpoints(curr_route, curr_type, tag=None):
    """Generates all possible endpoints given a particular route & type

    Args:
        curr_route - Current route ex. https://earthmaps.io/taspr/huc/
        curr_type - One of the many types availabe such as community or huc.

    Returns:
        Nothing.
    """
    # Opens the JSON file for the current type and replaces the "variable" portions
    # of the route to allow for the JSON items to fill in those fields.
    if curr_type == "community":
        places = asyncio.run(
            fetch_data(
                [
                    generate_wfs_places_url(
                        "all_boundaries:all_communities",
                        "name,latitude,longitude,tags",
                    )
                ]
            )
        )["features"]
        places = filter_by_tag(places, tag)
    else:
        places = asyncio.run(
            fetch_data([generate_wfs_places_url("all_boundaries:all_areas", "id")])
        )["features"]

    # For each JSON item in the JSON object array
    start_time = time.time()
    for place in places:
        get_endpoint(curr_route, curr_type, place["properties"])
    logging.info(
        f"Time to get all {curr_type} endpoints for {curr_route}: {time.time() - start_time:.2f} seconds"
    )


@routes.route("/recache/<limit>")
@routes.route("/cache/<limit>")
def recache(limit=False):
    """Runs through all endpoints that we expect for our web applications.
    This function can be used to pre-populate our API cache.
     Args:
         limit (str) - Any text will cause the function to limit the recache
         to what is in luts.cached_urls.
     Returns:
         JSON dump of all the endpoints in the API.
    """
    if limit:
        routes = cached_urls
    else:
        routes = all_routes()
    full_time = time.time()
    for route in routes:
        if (route.find("point") != -1 or route.find("local") != -1) and (
            route.find("lat") == -1
        ):
            get_all_route_endpoints(route, "community", ["eds,ncr"])
        elif route.find("all") != -1 and (route.find("lat") == -1):
            get_all_route_endpoints(route, "community", ["eds"])
        elif route.find("area") != -1 and route.find("var_id") == -1:
            get_all_route_endpoints(route, "area")

    end_time = time.time()
    logging.info("Time to fully recache: " + str(end_time - full_time))
    return Response(
        response=json.dumps(routes), status=200, mimetype="application/json"
    )
