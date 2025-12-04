"""A module to validate fetched data values."""

from datetime import datetime
from fetch_data import all_areas_full, all_communities_full


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

    for area in all_areas_full:
        if area["properties"]["id"] == place_id:
            place = area["properties"]
            full_name = place["name"]
            if place.get("alt_name", "") != "":
                full_name += " (" + place["alt_name"] + ")"
            return full_name, place["type"]

    for community in all_communities_full:
        if community["properties"]["id"] == place_id:
            place = community["properties"]
            full_name = place["name"]
            if place.get("alt_name", "") != "":
                full_name += " (" + place["alt_name"] + ")"
            return full_name, place["type"]

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
