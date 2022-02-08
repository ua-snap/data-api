from flask import Blueprint, current_app as app, Response
from luts import host
import json
import requests
from . import routes

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
    log.write(str(status.status_code) + ": " + url + "\n")
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
        url = host + curr_route + str(place["id"])

    # Collects returned status from GET request
    status = requests.get(url)

    # Logs the status and URL if the HTTP status code != 200
    if status.status_code != 200:
        log_error(url, status)


def get_all_route_endpoints(curr_route, curr_type):
    """Generates all possible endpoints given a particular route & type

        Args:
            curr_route - Current route ex. https://earthmaps.io/taspr/huc/
            curr_type - One of three types: community, huc, or pa

        Returns:
            Nothing.
    """
    # Opens the JSON file for the current type and replaces the "variable" portions
    # of the route to allow for the JSON items to fill in those fields.
    if curr_type == "community":
        f = open("data/jsons/ak_communities.json", "r")
        curr_route = curr_route.replace("<lat>/<lon>", "")
    elif curr_type == "huc":
        f = open("data/jsons/ak_hucs.json", "r")
        curr_route = curr_route.replace("<huc8_id>", "")
        curr_route = curr_route.replace("<huc_id>", "")
    else:
        f = open("data/jsons/ak_protected_areas.json", "r")
        curr_route = curr_route.replace("<akpa_id>", "")

    # Creates a JSON object from opened file
    places = json.load(f)

    # Closes open file handle
    f.close()

    # For each JSON item in the JSON object array
    for place in places:
        # For any URL route that has <var_ep>, we must replace that
        # variable with temperature, precipitation, and taspr to cache
        # all possible values.
        if curr_route.find("var_ep") != -1:
            for var in ["temperature", "precipitation", "taspr"]:
                var_ep_route = curr_route.replace("<var_ep>", var)
                get_endpoint(var_ep_route, curr_type, place)
        else:
            # If the URL doesn't have the <var_ep> value, we simply
            # ask for the endpoint to be requested via GET.
            get_endpoint(curr_route, curr_type, place)


@routes.route("/cache/")
def recache():
    """Runs through all endpoints that we expect for our web applications.
       This function can be used to pre-populate our API cache.

        Args:
            None.

        Returns:
            JSON dump of all the endpoints in the API.

    """
    routes = all_routes()
    for route in routes:
        if (route.find("point") != -1) and (route.find("lat") != -1):
            get_all_route_endpoints(route, "community")
        elif route.find("huc8_id") != -1 or route.find("huc_id") != -1:
            get_all_route_endpoints(route, "huc")
        elif route.find("akpa_id") != -1:
            get_all_route_endpoints(route, "pa")
    return Response(
        response=json.dumps(routes), status=200, mimetype="application/json"
    )
