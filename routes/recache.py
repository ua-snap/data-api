from flask import Blueprint, current_app as app, Response
import os
import json
import requests
from . import routes
from luts import type_di, host, cached_urls

recache_api = Blueprint("recache_api", __name__)


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
    # Stores log in data directory for now
    log = open("data/error-log.txt", "a")
    log.write(str(status) + ": " + url + "\n")
    log.close()


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
        url = host + curr_route + str(place)

    # Collects returned status from GET request
    status = requests.get(url)

    # Logs the status and URL if the HTTP status code != 200
    if status.status_code != 200:
        log_error(url, status.status_code)


def get_all_route_endpoints(curr_route, curr_type):
    """Generates all possible endpoints given a particular route & type

    Args:
        curr_route - Current route ex. https://earthmaps.io/taspr/huc/
        curr_type - One of four types: community, huc, pa, or local

    Returns:
        Nothing.
    """
    # Uses the GeoPandas GeoDataFrames for community or area types to generate
    # endpoints to cache.
    if curr_type == "community":
        for index, place in type_di["community"].iterrows():
            get_endpoint(curr_route, curr_type, place)
    elif curr_type == "area":
        # Copy the type dictionary containing all GDFs
        areas_di = type_di.copy()

        # Remove the community GDF since that is done differently for
        # API endpoints.
        del areas_di["community"]

        # Loop through all GDFs for AOIs
        for area_type in areas_di:
            for place in areas_di[area_type].iterrows():
                get_endpoint(curr_route, curr_type, place[0])


@routes.route("/recache/<limit>")
@routes.route("/cache/<limit>")
def recache(limit):
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
    for route in routes:
        if (
            route.find("point") != -1
            or route.find("local") != -1
            or route.find("all") != -1
        ) and (route.find("lat") == -1):
            get_all_route_endpoints(route, "community")
        elif route.find("area") != -1 and route.find("var_id") == -1:
            get_all_route_endpoints(route, "area")

    return Response(
        response=json.dumps(routes), status=200, mimetype="application/json"
    )
