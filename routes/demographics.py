from flask import render_template, Response, request
import asyncio
import json
import requests

# local imports
from . import routes
from luts import demographics_fields

from generate_urls import generate_wfs_places_url
from fetch_data import fetch_data
from csv_functions import create_csv

def validate_community_id(community):
    """Function to confirm that the input community ID is valid.
    Args:
           community (string): A community ID from route input
    Returns:
            Tuple of boolean and list: for boolean, True means the community ID is valid and False means it is not valid; list is all valid community IDs
    """
    community_ids = []
    url = generate_wfs_places_url("demographics:demographics", properties="id")
    with requests.get(url) as r:
        for feature in r.json()['features']:
            community_ids.append(feature['properties']['id'])
    if community in community_ids:
        return True, community_ids
    else: 
        return False, community_ids

@routes.route("/demographics/")
def demographics_about():
    return render_template("/documentation/demographics.html")

@routes.route("/demographics/<community>")
def get_data_for_community(community):
    """
    Function to pull demographics data as JSON or CSV.
       Args:
           community (string): A community ID from https://earthmaps.io/places/communities, or "all" to get all available communities

       Returns:
           JSON-formatted output of demographic data for the requested community,
           with additional contextual data for Alaska and the United States.

       Notes:
           example: http://localhost:5000/demographics/AK15
    """
    # Validate community ID; if not valid, return an error
    validation, community_ids = validate_community_id(community)
    if community == 'all': pass
    elif not validation:
        return render_template("400/bad_request.html"), 400
    else:
        community_ids = [community, "US0", "AK0"]

    # List URLs
    urls = []
    for c in community_ids:
        urls.append(generate_wfs_places_url("demographics:demographics", filter=c, filter_type="id"))

    # Requests the Geoserver WFS URLs and extracts property values to a dict
    results = {}
    for r in asyncio.run(fetch_data(urls)):
        results[r["features"][0]["properties"]["id"]] = r["features"][0]["properties"]

    # Rename keys
    for c in community_ids:
        fields_to_rename = [x for x in list(results[c].keys()) if x in list(demographics_fields.keys())]
        for field in fields_to_rename:
            results[c][demographics_fields[field]] = results[c].pop(field)

    # Recreate the dicts in a better order for viewing (drops "id", "GEOID", and "areatype")
    # convert to JSON object to preserve ordered output
    fields = ["name", "comment", "total_population", "pct_under_18", "pct_under_5", "pct_65_plus", 
    "pct_minority", "pct_african_american", "pct_amer_indian_ak_native", "pct_asian", "pct_hawaiian_pacislander", "pct_hispanic_latino", "pct_white", "pct_multi", "pct_other",
    "pct_asthma", "pct_copd", "pct_diabetes", "pct_hd", "pct_kd", "pct_stroke",
    "pct_w_disability", "moe_pct_w_disability", "pct_insured", "moe_pct_insured", "pct_uninsured", "moe_pct_uninsured",
    "pct_no_bband", "pct_no_hsdiploma", "pct_below_150pov",
    ]

    reformatted_results = {}
    for c in community_ids:
        reformatted_results[c] = {}
        for field in fields:
            reformatted_results[c][field] = results[c][field]

    # Return CSV if requested
    if request.args.get("format") == "csv":
        if community != "all":
            return create_csv(reformatted_results, endpoint="demographics", place_id=community)
        else:
            return create_csv(reformatted_results, endpoint="demographics", place_id=None)
    
    # Otherwise return Flask JSON Response
    json_results = json.dumps(reformatted_results, indent = 4)
    return Response(response=json_results, status=200, mimetype="application/json")


