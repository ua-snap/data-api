"""A module to validate fetched data values."""

import asyncio
from datetime import datetime
from generate_urls import generate_wfs_places_url
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

    place = asyncio.run(
        fetch_data(
            [
                generate_wfs_places_url(
                    "all_boundaries:all_areas",
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


def validate_seaice_timestring(timestring):
    """
    Validate the timestring for the seaice coverage.

    Args:
        timestring (str): the timestring to validate

    Returns:
        datetime object of the timestring
    """
    try:
        parsed_date = datetime.strptime(timestring, "%Y-%m-%dT%H:%M:%S.%fZ")
        return parsed_date
    except ValueError:
        raise ValueError(
            "The timestring is not in the expected format for the seaice coverage: (YYYY-MM-DDTHH:MM:SS.sssZ)"
        )
