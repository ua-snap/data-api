from flask import Blueprint, request, current_app as app, jsonify
from . import routes
from .taspr import get_temperature_plate, get_precipitation_plate, proj_precip_point
from .snow import eds_snow_data
from .degree_days import get_dd_plate
from .permafrost import permafrost_eds_request
from .wet_days_per_year import get_wet_days_per_year_plate
from .elevation import run_fetch_elevation
from .hydrology import eds_hydrology_data
import logging
import time

eds_api = Blueprint("eds_api", __name__)

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def extract_json(response):
    """
    Function to extract JSON data from a response.

    Args:
        response (tuple, Flask response, or dict): The response from the endpoint.

    Returns:
        dict: The JSON data from the response.
    """
    # If a 500 HTTP error code is returned from any of the individual dataset
    # endpoints, the error will be returned from the entire /eds/all endpoint.
    # I.e., treat 500 error codes as a total failure, not just a nodata
    # response.
    if isinstance(response, tuple):
        if len(response) == 2:
            status_code = response[1]
            if status_code == 500:
                return response
        # Treat non-500 error codes as invalid/nodata responses
        return {}

    # Extract JSON from the response if it's a Flask response
    if hasattr(response, "get_json"):
        return response.get_json()
    elif hasattr(response, "json"):
        return response.json

    # Return the dictionary if not a Flask response
    return response


def fetch_all_eds(lat, lon):
    """
    Endpoint for requesting all data required for the Arctic EDS reports.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/eds/all/68.0764/-154.5501
    """

    # List of dataset keys and their corresponding getter functions
    endpoints = [
        ("temperature", get_temperature_plate),
        ("precipitation", get_precipitation_plate),
        ("snowfall", eds_snow_data),
        ("freezing_index", lambda lat, lon: get_dd_plate("freezing_index", lat, lon)),
        ("heating_degree_days", lambda lat, lon: get_dd_plate("heating", lat, lon)),
        ("thawing_index", lambda lat, lon: get_dd_plate("thawing_index", lat, lon)),
        ("permafrost", permafrost_eds_request),
        ("wet_days_per_year", get_wet_days_per_year_plate),
        ("elevation", run_fetch_elevation),
        ("precip_frequency", proj_precip_point),
        ("hydrology", eds_hydrology_data),
    ]

    # Combine all results into a single dictionary for return to ArcticEDS
    results = {}
    for key, func in endpoints:
        result = extract_json(func(lat, lon))

        # If results is a tuple, this is an error response. Return it directly.
        if isinstance(result, tuple):
            return result

        results[key] = result

    return results


@routes.route("/eds/all/<lat>/<lon>")
def fetch_all_eds_route(lat, lon):
    """
    Endpoint for requesting all data required for the Arctic EDS reports.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/eds/all/68.0764/-154.5501
    """
    start_time = time.time()
    logger.info(f"EDS all endpoint accessed: lat={lat}, lon={lon}")
    results = fetch_all_eds(lat, lon)

    # If results is a tuple, this is an error response. Return it directly.
    if isinstance(results, tuple):
        elapsed = time.time() - start_time
        logger.error(
            f"EDS all endpoint error for lat={lat}, lon={lon} (in {elapsed:.3f} seconds)"
        )
        return results

    elapsed = time.time() - start_time
    logger.info(
        f"EDS all endpoint returned JSON: lat={lat}, lon={lon} (in {elapsed:.3f} seconds)"
    )
    return jsonify(results)
