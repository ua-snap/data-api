from flask import Blueprint, render_template, Response, request
import asyncio
# import pandas as pd

# local imports
from . import routes
from luts import demographics_fields

from generate_urls import generate_wfs_places_url
from fetch_data import fetch_data
from csv_functions import create_csv


@routes.route("/demographics/<community>")
def get_data_for_community(community):
    """
    Function to pull demographics data as JSON or CSV.
       Args:
           community (string): A community ID from https://earthmaps.io/places/communities.

       Returns:
           JSON-formatted output of demographic data for the requested community,
           with additional contextual data for Alaska and the United States.

       Notes:
           example: http://localhost:5000/demographics/AK15
    """
    # List URLs
    urls = []
    for c in [community, "US0", "AK0"]:
        urls.append(generate_wfs_places_url("demographics:demographics", filter=c, filter_type="id"))

    # Requests the Geoserver WFS URLs and extracts property values to a dict
    results = {}
    for r in asyncio.run(fetch_data(urls)):
        results[r["features"][0]["properties"]["id"]] = r["features"][0]["properties"]

    # Rename keys
    for c in [community, "US0", "AK0"]:
        fields_to_rename = [x for x in list(results[c].keys()) if x in list(demographics_fields.keys())]

        for field in fields_to_rename:
            results[c][demographics_fields[field]] = results[c].pop(field)

    # Return CSV if requested
    if request.args.get("format") == "csv":
         return create_csv(results, endpoint="demographics", place_id=community)

    # Otherwise return results dict
    return results


