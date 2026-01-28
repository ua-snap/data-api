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

    # Extract forecast blocks at 24, 48, and 72 hours
    forecast_blocks = data.get("forecast_blocks", [])
    block_24hr = None
    block_2days = None
    block_3days = None

    for block in forecast_blocks:
        if block.get("forecast_hour") == 24:
            block_24hr = block
        elif block.get("forecast_hour") == 48:
            block_2days = block
        elif block.get("forecast_hour") == 72:
            block_3days = block

    di = {
        "timestamp": str(data.get("ts", "")),
        "expires_at": str(data.get("expires_at", "")),
        "place_name": data.get("place_name"),
        "place_id": data.get("place_id"),
        "gauge_id": data.get("gauge_id"),
        "realtime_antecedent_mm": data.get("realtime_antecedent_mm"),
        "realtime_rainfall_mm": data.get("realtime_rainfall_mm"),
        "realtime_risk_level": data.get("realtime_risk_level"),
        "realtime_threshold_upper": data.get("realtime_threshold_upper"),
        "block_24hr": block_24hr,
        "block_2days": block_2days,
        "block_3days": block_3days,
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

        expires_at = landslide_data.get("expires_at")
        if expires_at:
            try:
                expires_datetime = datetime.fromisoformat(str(expires_at))
                current_datetime = (
                    datetime.now(expires_datetime.tzinfo)
                    if expires_datetime.tzinfo
                    else datetime.now()
                )

                # data are stale, return the data + HTTP code 409
                if expires_datetime < current_datetime:
                    return jsonify(landslide_data), 409

            except (ValueError, TypeError) as exc:
                raise exc

        return jsonify(landslide_data)

    except Exception as exc:
        logger.error(f"Error in landslide endpoint for {community_id}: {exc}")
        return render_template("500/server_error.html"), 500
