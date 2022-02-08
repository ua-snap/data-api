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
def get_json_for_type(type):
    """GET function to pull JSON files
        Args:
            type (string): One of three types:
                [communities, hucs, protected_areas]

        Returns:
            JSON-formatted output of all communities, HUCs,
            or protected areas.

        Notes:
            example: http://localhost:5000/places/communities
    """
    # Generates path to JSON
    jsonpath = f"data/jsons/{json_types[type]}"

    # If the JSON doesn't exist, it needs be generated.
    if not os.path.exists(jsonpath):
        update_data()

    # Open JSON file and return to requestor
    df = pd.read_json(jsonpath)
    return Response(
        response=df.to_json(orient="records"), status=200, mimetype="application/json"
    )


@routes.route("/update/")
def update_json_data():
    """GET function for updating underlying CSVs and shapefiles. Creates
       JSON file from CSVs and shapefiles.

        Args:
            None.

        Returns:
            Web page response indicating if a successful update of the data
            took place.

        Notes:
            example: http://localhost:5000/update
    """
    if update_data():
        return render_template("vectordata/updated.html")
    else:
        return render_template("vectordata/failed_update.html")


def update_data():
    """Downloads AOI CSV and shapefiles and converts to JSON format

        Args:
            None.

        Returns:
            Boolean value indicating success or failure to update datasets.
            The underlying code updates all the communities, HUCs, and
            protected areas in Alaska.
    """
    try:
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

        # Dump data frame to JSON file
        df.to_json(f"{jsonpath}ak_communities.json", orient="records")

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
            open(f"{path}hydrologic_units_wbdhu8_a_ak.{filetype}", "wb").write(
                r.content
            )

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
        with open(f"{jsonpath}ak_hucs.json", "w") as outfile:
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
        with open(f"{jsonpath}ak_protected_areas.json", "w") as outfile:
            json.dump(output, outfile)

        # If we have made it this far without issue, we return True
        return True
    except:
        # If anything goes wrong during execution, return False
        # TODO: Collect error and ensure team is alerted to issue.
        return False
