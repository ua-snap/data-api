"""A module to validate fetched data values."""
import asyncio
from config import RAS_BASE_URL, WEB_APP_URL
from generate_requests import *
from generate_urls import *
from luts import place_type_labels
from fetch_data import fetch_data


def place_name_and_type(place_id):
    """
    Determine if provided place_id corresponds to a known place.

    Args:
        place_id (str): place identifier (e.g., AK124)

    Returns:
        Name and type of the place if it was found, otherwise None and None
    """

    if place_id is None:
        return None, None

    # HUC12s, not getting names from them below
    if place_id.isdigit() and len(place_id) == 12:
        return None, "huc12"

    # HUC10s
    if place_id.isdigit() and len(place_id) == 10:
        return None, "huc10"

    place = asyncio.run(
        fetch_data(
            [
                generate_wfs_places_url(
                    "all_boundaries:all_areas", "name,alt_name,type", place_id, "id"
                )
            ]
        )
    )
    if place["numberMatched"] > 0:
        place = place["features"][0]["properties"]
        full_place = place["name"]
        if place["alt_name"] != "":
            full_place += " (" + place["alt_name"] + ")"
        return full_place, place["type"]
    else:
        place = asyncio.run(
            fetch_data(
                [
                    generate_wfs_places_url(
                        "all_boundaries:all_communities",
                        "name,alt_name,type",
                        place_id,
                        "id",
                    )
                ]
            )
        )
        if place["numberMatched"] > 0:
            place = place["features"][0]["properties"]
            full_place = place["name"]
            if place["alt_name"] != "":
                full_place += " (" + place["alt_name"] + ")"
            return full_place, place["type"]

    return None, None
