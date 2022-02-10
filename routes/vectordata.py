from flask import Blueprint, render_template, Response

import geopandas as gpd
import json
import os
import pandas as pd
import requests

# local imports
from . import routes
from luts import json_types

data_api = Blueprint("data_api", __name__)


@routes.route("/places/<type>")
def get_json_for_type(type, recurse=False):
    """GET function to pull JSON files
        Args:
            type (string): One of four types:
                [communities, hucs, protected_areas, all]
            recurse (boolean): Defaults to False. Being True
                causes the function to be recursive to allow for
                the same function to collect all the possible JSONs.

        Returns:
            JSON-formatted output of all communities, HUCs,
            and / or protected areas.

        Notes:
            example: http://localhost:5000/places/communities
    """
    if type == "all":
        json_list = []

        # Runs through each of the JSON files
        for curr_type in ["communities", "hucs", "protected_areas"]:

            # Sends a recursive call to this function
            curr_js = get_json_for_type(curr_type, recurse=True)

            # Combines the JSON returned into Python list
            json_list.extend(json.loads(curr_js))

        # Dumps the combined Python list into a single JSON object
        js = json.dumps(json_list)
    else:
        # Generates path to JSON
        jsonpath = json_types[type]

        # If the JSON doesn't exist, it needs be generated.
        if not os.path.exists(jsonpath):
            update_data()

        # Open JSON file and return to requestor
        with open(jsonpath, "r") as infile:
            js = json.dumps(json.load(infile))

    if recurse:
        return js

    # Returns Flask JSON Response
    return Response(response=js, status=200, mimetype="application/json")


@routes.route("/update/")
def update_json_data():
    """GET function for updating underlying CSVs and shapefiles. Creates
       JSON file from CSVs and shapefiles.

        Args:
            None.

        Returns:
            JSON response indicating if a successful update of the data
            took place.

        Notes:
            example: http://localhost:5000/update
    """
    update_data()
    return Response(
        response="{ 'success': True }", status=200, mimetype="application/json"
    )


def update_data():
    """Downloads AOI CSV and shapefiles and converts to JSON format

        Args:
            None.

        Returns:
            Boolean value indicating success or failure to update datasets.
            The underlying code updates all the communities, HUCs, and
            protected areas in Alaska.
    """
    ### Community Locations ###

    # Ensure the path to store CSVs is created
    path = "data/csvs/"
    if not os.path.exists(path):
        os.makedirs(path)

    # Ensure the path to store JSONs is created
    jsonpath = "data/jsons/"
    if not os.path.exists(jsonpath):
        os.makedirs(jsonpath)

    # Download CSV for all Alaskan communities and write to local CSV file.
    url = "https://github.com/ua-snap/geospatial-vector-veracity/raw/main/vector_data/point/alaska_point_locations.csv"
    r = requests.get(url, allow_redirects=True)
    open(f"{path}ak_communities.csv", "wb").write(r.content)

    # Open CSV file into Pandas data frame
    df = pd.read_csv(f"{path}ak_communities.csv")

    # Add type of community to each community
    df["type"] = "community"

    # Dump data frame to JSON file
    df.to_json(json_types["communities"], orient="records")

    ### HUCs ###

    # Ensure the path to store shapefiles is created
    path = "data/shapefiles/"
    if not os.path.exists(path):
        os.makedirs(path)

    # For each required file of the shapefile, download and store locally.
    for filetype in ["dbf", "prj", "sbn", "sbx", "shp", "shx"]:
        url = (
            f"https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/polygon"
            f"/boundaries/alaska_hucs/hydrologic_units_wbdhu8_a_ak.{filetype}?raw=true "
        )
        r = requests.get(url, allow_redirects=True)
        open(f"{path}hydrologic_units_wbdhu8_a_ak.{filetype}", "wb").write(r.content)

    # Read shapefile into Geopandas data frame
    df = gpd.read_file(f"{path}hydrologic_units_wbdhu8_a_ak.shp")

    # Create a copy of the original data frame
    x = df.copy()

    # Remove all the fields that we don't want in our final JSON.
    for remove_field in [
        "geometry",
        "tnmid",
        "metasource",
        "sourcedata",
        "sourceorig",
        "sourcefeat",
        "loaddate",
        "areasqkm",
        "areaacres",
        "referenceg",
    ]:
        del x[remove_field]

    # Create a new Pandas data frame from modified data.
    z = pd.DataFrame(x)

    # Create JSON data from Pandas data frame.
    hucs_json = json.loads(z.T.to_json(orient="columns"))

    # Create a blank output list for appending JSON fields.
    output = []

    # For each HUC in the JSON, we want to clean up the fields to match
    # the IEM project's JSON and append it to the output list.
    for key in hucs_json:
        # Changes HUC key 'huc8' to 'id'
        hucs_json[key]["id"] = hucs_json[key]["huc8"]
        del hucs_json[key]["huc8"]

        # Adds type to JSON of 'huc'
        hucs_json[key]["type"] = "huc"

        # Append the JSON object to end of list.
        output.append(hucs_json[key])

    # Dump output list into local JSON file
    with open(json_types["hucs"], "w") as outfile:
        json.dump(output, outfile)

    ### Alaska Protected Areas ###

    # For each required file of the shapefile, download and store locally.
    for filetype in ["cpg", "dbf", "prj", "shp", "shx"]:
        url = (
            f"https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/polygon/boundaries"
            f"/protected_areas/ak_protected_areas/ak_protected_areas.{filetype}?raw=true "
        )
        r = requests.get(url, allow_redirects=True)
        open(f"{path}ak_protected_areas.{filetype}", "wb").write(r.content)

    # Read shapefile into Geopandas data frame
    df = gpd.read_file(f"{path}ak_protected_areas.shp")

    # Create a copy of the original data frame
    x = df.copy()

    # Remove all the fields that we don't want in our final JSON.
    for remove_field in ["geometry", "country", "region"]:
        del x[remove_field]

    # Create a new Pandas data frame from modified data.
    z = pd.DataFrame(x)

    # Create JSON data from Pandas data frame.
    pa_json = json.loads(z.T.to_json(orient="columns"))

    # Create a blank output list for appending JSON fields.
    output = []

    # For each protected area in the PA JSON, add the type protected_area
    # and append it to the output list.
    for key in pa_json:
        # Adds key 'type' and value 'protected_area'
        pa_json[key]["type"] = "protected_area"

        # Append JSON to output list.
        output.append(pa_json[key])

    # Dump JSON object to local JSON file
    with open(json_types["protected_areas"], "w") as outfile:
        json.dump(output, outfile)
