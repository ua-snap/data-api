from flask import Blueprint, request, current_app as app, jsonify
from . import routes
from .taspr import get_temperature_plate, get_precipitation_plate, proj_precip_point
from .snow import eds_snow_data
from .degree_days import get_dd_plate
from .permafrost import permafrost_eds_request
from .wet_days_per_year import get_wet_days_per_year_plate
from .elevation import run_fetch_elevation
from .hydrology import eds_hydrology_data

eds_api = Blueprint("eds_api", __name__)


def extract_json(response):
    """
    Function to extract JSON data from a response.

    Args:
        response (tuple, Flask response, or dict): The response from the endpoint.

    Returns:
        dict: The JSON data from the response.
    """
    # If response is a tuple, the endpoint failed and we should return an empty dict
    if isinstance(response, tuple):
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
    # Get data from each endpoint directly using their functions
    temperature = extract_json(get_temperature_plate(lat, lon))
    precipitation = extract_json(get_precipitation_plate(lat, lon))
    snow = extract_json(eds_snow_data(lat, lon))
    freezing_index = extract_json(get_dd_plate("freezing_index", lat, lon))
    heating_degree_days = extract_json(get_dd_plate("heating", lat, lon))
    thawing_index = extract_json(get_dd_plate("thawing_index", lat, lon))
    permafrost = extract_json(permafrost_eds_request(lat, lon))
    wet_days_per_year = extract_json(get_wet_days_per_year_plate(lat, lon))
    elevation = extract_json(run_fetch_elevation(lat, lon))
    precip_frequency = extract_json(proj_precip_point(lat, lon))
    hydrology = extract_json(eds_hydrology_data(lat, lon))

    # Combine all results into a single dictionary for return to ArcticEDS
    return {
        "temperature": temperature,
        "precipitation": precipitation,
        "snowfall": snow,
        "freezing_index": freezing_index,
        "heating_degree_days": heating_degree_days,
        "thawing_index": thawing_index,
        "permafrost": permafrost,
        "wet_days_per_year": wet_days_per_year,
        "elevation": elevation,
        "precip_frequency": precip_frequency,
        "hydrology": hydrology,
    }


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
    return jsonify(fetch_all_eds(lat, lon))
