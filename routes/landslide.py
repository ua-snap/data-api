from flask import render_template, jsonify
import logging

from . import routes
from fetch_data import get_landslide_db_row

logger = logging.getLogger(__name__)


def validate_place_name(place_name):
    """
    Validate that the place name is one of the supported locations.

    Args:
        place_name (str): The name of the place to validate

    Returns:
        bool: True if valid, False if not valid
    """
    valid_places = ["Kasaan", "Craig"]
    place_name_capitalized = place_name.capitalize()

    return place_name_capitalized in valid_places


def package_landslide_data(landslide_resp):
    """Package landslide data in dict"""
    if not landslide_resp or landslide_resp == []:
        return None

    data = landslide_resp[0] if isinstance(landslide_resp, list) else landslide_resp

    di = {
        "place_name": data.get("place_name"),
        "timestamp": str(data.get("ts", "")),
        "expires_at": str(data.get("expires_at", "")),
        "hour": data.get("hour"),
        "precipitation_mm": data.get("precip"),
        "precipitation_inches": data.get("precip_inches"),
        "precipitation_24hr": data.get("precip24hr"),
        "precipitation_2days": data.get("precip2days"),
        "precipitation_3days": data.get("precip3days"),
        "risk_level": data.get("risk_level"),
        "risk_probability": data.get("risk_prob"),
        "risk_24hr": data.get("risk24hr"),
        "risk_2days": data.get("risk2days"),
        "risk_3days": data.get("risk3days"),
        "risk_is_elevated_from_previous": data.get("risk_is_elevated_from_previous"),
    }
    return di


@routes.route("/landslide/<place_name>")
def run_fetch_landslide_data(place_name):
    """
    Run the landslide data fetch for a specific place.

    Args:
        place_name (str): Name of the place (Kasaan or Craig)

    Returns:
        Rendered template or JSON response with landslide data

    Example request: http://localhost:5000/landslide/Kasaan
    """
    if not validate_place_name(place_name):
        return render_template("400/bad_request.html"), 400

    try:
        results = get_landslide_db_row(place_name)
        landslide_data = package_landslide_data(results)
        return jsonify(landslide_data)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
