from flask import render_template, jsonify, abort
import logging
from datetime import datetime

from . import routes
from fetch_data import get_landslide_db_row, get_place_data
from validate_data import place_name_and_type
from luts import valid_kuti_communityIDs

logger = logging.getLogger(__name__)


def validate_community_id(community_id):
    """
    Validate that the community ID is both a valid place ID and one of the supported landslide locations.
    Uses the existing place validation system, then restricts to AK182 and AK91.

    Args:
        community_id (str): The community ID to validate (AK91 for Craig, AK182 for Kasaan)

    Returns:
        str or None: Place name if valid community ID, None if invalid
    """

    if community_id in valid_kuti_communityIDs:
        return valid_kuti_communityIDs[community_id]

    return None


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

    expires_at = data.get("expires_at")
    if expires_at:
        try:
            expires_datetime = datetime.fromisoformat(str(expires_at))
            current_datetime = (
                datetime.now(expires_datetime.tzinfo)
                if expires_datetime.tzinfo
                else datetime.now()
            )

            if expires_datetime < current_datetime:
                di["error_code"] = 409
                di["error_msg"] = "Data is stale"
        except (ValueError, TypeError) as exc:
            raise exc

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

    # Check for errors when fetching landslide data
    try:
        results = get_landslide_db_row(place_name)
    except Exception as exc:
        logger.error(f"Error fetching landslide data for {community_id}: {exc}")
        return render_template("502/upstream_unreachable.html"), 502

    # Check for errors when fetching community data
    # and processing landslide data
    try:
        community_data = get_place_data(community_id)

        landslide_data = package_landslide_data(results, community_data)

        return jsonify(landslide_data)

    except Exception as exc:
        logger.error(f"Error in landslide endpoint for {community_id}: {exc}")
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
