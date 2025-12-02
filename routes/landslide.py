from flask import render_template, jsonify
import logging

from . import routes
from fetch_data import get_landslide_db_row, get_place_data
from validate_data import place_name_and_type

logger = logging.getLogger(__name__)


def validate_community_id(community_id):
    """
    Validate that the community ID is both a valid place ID and one of the supported landslide locations.
    Uses the existing place validation system, then restricts to AK182 and AK91.

    Args:
        community_id (str): The community ID to validate (AK182 for Kasaan, AK91 for Craig)

    Returns:
        str or None: Place name if valid community ID, None if invalid
    """
    place_name, place_type = place_name_and_type(community_id)

    if place_name is None:
        return None

    community_mapping = {"AK182": "Kasaan", "AK91": "Craig"}

    community_id_upper = community_id.upper()
    supported_place = community_mapping.get(community_id_upper)

    return supported_place


def package_landslide_data(landslide_resp, community_data=None):
    """Package landslide data in dict, optionally including community data"""
    if not landslide_resp or landslide_resp == []:
        return None

    data = landslide_resp[0] if isinstance(landslide_resp, list) else landslide_resp

    di = {
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

    # Add community data if provided
    if community_data:
        di["community"] = community_data

    return di


@routes.route("/landslide/")
def landslide_about():
    return render_template("documentation/landslide.html")


@routes.route("/landslide/<community_id>")
def run_fetch_landslide_data(community_id):
    """
    Run the landslide data fetch for a specific community.

    Args:
        community_id (str): Community ID (AK182 for Kasaan, AK91 for Craig)

    Returns:
        Rendered template or JSON response with landslide data including community info

    Example request: http://localhost:5000/landslide/AK182
    """
    place_name = validate_community_id(community_id)
    if not place_name:
        return render_template("400/bad_request.html"), 400

    try:
        results = get_landslide_db_row(place_name)

        community_data = get_place_data(community_id)

        landslide_data = package_landslide_data(results, community_data)
        if landslide_data is None:
            return render_template("404/no_data.html"), 404
        return jsonify(landslide_data)

    except Exception as exc:
        logger.error(f"Error in landslide endpoint for {community_id}: {exc}")
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
