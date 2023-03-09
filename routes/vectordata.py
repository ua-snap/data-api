from flask import Blueprint, render_template, Response

import geopandas as gpd
import json
import os
import shutil
import numpy as np
import pandas as pd
import requests
from shapely.geometry import Point, box

# local imports
from . import routes
from luts import (
    json_types,
    shp_di,
    all_jsons,
    areas_near,
)
from config import GS_BASE_URL, EAST_BBOX, WEST_BBOX
from validate_request import validate_latlon
from generate_urls import generate_wfs_search_url, generate_wfs_places_url

data_api = Blueprint("data_api", __name__)


@routes.route("/places/search/<lat>/<lon>")
def find_via_gs(lat, lon):
    """
     GET function to search for nearby communities and polygon areas
     by a supplied latitude and longitude.

    Args:
        lat (float): latitude of requested point
        lon (float): longitude of requested point

    Returns:
        JSON-output of all nearby communities and polygon areas.

    Notes:
        example: http://localhost:5000/places/search/64.28/-144.28
    """

    # Validate the latitude and longitude are valid and within the bounding
    # box of our area of interest.
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    # WFS request to Geoserver for all communities.
    communities_resp = requests.get(
        generate_wfs_search_url("all_boundaries:all_communities", lat, lon),
        allow_redirects=True,
    )
    communities_json = json.loads(communities_resp.content)
    nearby_communities = communities_json["features"]

    # Dictionary containing all the communities and
    # polygon areas by the end of this function.
    proximal_di = dict()
    proximal_di["communities"] = dict()

    # For each returned community, grab its name,
    # alternate name, id, lat, lon, and type. They are all
    # found within the properties of the returned JSON.
    for i in range(len(nearby_communities)):
        proximal_di["communities"][i] = nearby_communities[i]["properties"]

    # WFS request to Geoserver for all polygon areas.
    areas_resp = requests.get(
        generate_wfs_search_url("all_boundaries:all_areas", lat, lon),
        allow_redirects=True,
    )
    areas_json = json.loads(areas_resp.content)
    nearby_areas = areas_json["features"]

    # Create the JSON section for each of the area types.
    for area_type in areas_near.values():
        proximal_di[area_type] = dict()

    # For each returned area, place it inside the correct area type.
    # We want to collect the area's geometry, id, name, and type.
    for ai in range(len(nearby_areas)):
        current_area_type = areas_near[nearby_areas[ai]["properties"]["type"]]
        current_index = len(proximal_di[current_area_type])
        proximal_di[current_area_type][current_index] = gather_nearby_area(
            nearby_areas[ai]
        )

    # Check to see if any communities were found around the point chosen
    communities_found = (
        nearby_communities if communities_json["numberMatched"] > 0 else False
    )

    # Get the total bounds for the communities, HUCs, and protected areas only
    total_bounds = get_total_bounds(lat, lon, communities_found)

    # Bounding box keys
    bbox_ids = ["xmin", "ymin", "xmax", "ymax"]

    # Generates bounding box from keys above and the values of the total_bounds
    proximal_di["total_bounds"] = dict(zip(bbox_ids, list(total_bounds)))

    return Response(
        response=json.dumps(proximal_di), status=200, mimetype="application/json"
    )


def get_total_bounds(lat, lon, communities=False):
    """
    Generates the total bounds of the returned data from a search, but only for
    communities, HUC8s, and protected areas.

    Args:
        lat (float): latitude of requested point
        lon (float): longitude of requested point
        communities: Either the JSON response containing all communities nearby or False

    Returns:
        Bounding box for AOI for all communities, HUC8s, and protected areas nearby the
        selected latitude and longitude.

        Returns as Python list with order [xmin, ymin, xmax, ymax]
    """

    # Request the nearby HUCs and protected areas only
    hucs_pa_resp = requests.get(
        generate_wfs_search_url("all_boundaries:all_areas", lat, lon, True),
        allow_redirects=True,
    )

    # From the JSON returned, pull out the features
    hucs_pa_json = json.loads(hucs_pa_resp.content)
    nearby_hucs_pa = hucs_pa_json["features"]

    # Create a GeoPandas GeoDataFrame from the nearby HUCs and protected areas
    areas_gdf = gpd.GeoDataFrame.from_features(nearby_hucs_pa)

    # If there were any nearby communities, we want to ensure our
    # bounding box includes them.
    if communities:
        # Create a GeoPandas GeoDataFrame from the communities
        communities_gdf = gpd.GeoDataFrame.from_features(communities)

        # Gather the maximum of the total bounds from communities, HUCs, and protected areas
        total_bounds = np.maximum(communities_gdf.total_bounds, areas_gdf.total_bounds)

        # The most western longitudinal coordinate and the most southern
        # latitudinal coordinate must be the minimums.
        total_bounds[0] = np.minimum(
            communities_gdf.total_bounds[0], areas_gdf.total_bounds[0]
        )
        total_bounds[1] = np.minimum(
            communities_gdf.total_bounds[1], areas_gdf.total_bounds[1]
        )
    else:
        # If no communities are returned from the search, the HUCs and protected areas
        # bounding box should be used.
        total_bounds = areas_gdf.total_bounds

    return total_bounds


def gather_nearby_area(nearby_area):
    """
    Gather data from the nearby area to be returned for the search interface.

        Args:
            nearby_area (JSON object): JSON containing metadata about current community.

        Returns:
            Python dictionary containing the geometry, ID, name and type of the area
    """
    curr_di = dict()
    curr_di["geojson"] = nearby_area["geometry"]
    curr_di["id"] = nearby_area["properties"]["id"]
    curr_di["name"] = nearby_area["properties"]["name"]
    curr_di["type"] = nearby_area["properties"]["type"]
    return curr_di


@routes.route("/places/<type>")
def get_json_for_type(type, recurse=False):
    """
    GET function to pull JSON files
       Args:
           type (string): Any of the below types:
               [communities, hucs, corporations, climate_divisions,
                ethnolinguistic_regions, game_management_units, fire_zones,
                first_nations, boroughs, census_areas, protected_areas, all]
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
        json_list = list()

        # Loops through all the different types for search field
        for curr_type in all_jsons:

            # Gets the JSON for the current type
            curr_js = get_json_for_type(curr_type, recurse=True)

            # Adds the returned JSON to a list
            json_list.extend(json.loads(curr_js))

        # Dumps the list of JSON into the returned js object
        js = json.dumps(json_list)

    else:
        js_list = list()
        if type == "communities":
            # Requests the Geoserver WFS URL for gathering all the communities
            communities_resp = requests.get(
                generate_wfs_places_url(
                    "all_boundaries:all_communities",
                    "name,alt_name,id,type,latitude,longitude",
                ),
                allow_redirects=True,
            )
            communities_json = json.loads(communities_resp.content)

            # Pulls out only the "Features" field, containing all the
            # properties for the communities
            all_communities = communities_json["features"]

            # For each feature, put the properties (name, id, etc.) into the
            # list for creation of a JSON object to be returned.
            for i in range(len(all_communities)):
                js_list.append(all_communities[i]["properties"])
        else:
            # Remove the 's' at the end of the type
            type = type[:-1]

            # Requests the Geoserver WFS URL for gathering all the polygon areas
            areas_resp = requests.get(
                generate_wfs_places_url(
                    "all_boundaries:all_areas", "id,name,type", type
                ),
                allow_redirects=True,
            )
            areas_json = json.loads(areas_resp.content)

            # Pulls out only the "Features" field, containing all the
            # properties for the areas
            all_areas = areas_json["features"]

            # For each feature, put the properties (name, id, type) into the
            # list for creation of a JSON object to be returned.
            for ai in range(len(all_areas)):
                js_list.append(all_areas[ai]["properties"])

        # Creates JSON object from created list
        js = json.dumps(js_list)

    if recurse:
        return js

    # Returns Flask JSON Response
    return Response(response=js, status=200, mimetype="application/json")


@routes.route("/update")
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
        response='{ "success": "True" }', status=200, mimetype="application/json"
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
    else:
        shutil.rmtree(path)
        os.makedirs(path)

    # Ensure the path to store JSONs is created
    jsonpath = "data/jsons/"
    if not os.path.exists(jsonpath):
        os.makedirs(jsonpath)
    else:
        shutil.rmtree(jsonpath)
        os.makedirs(jsonpath)

    # Ensure the path to store shapefiles is created
    shppath = "data/shapefiles/"
    if not os.path.exists(shppath):
        os.makedirs(shppath)
    else:
        shutil.rmtree(shppath)
        os.makedirs(shppath)

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

    for k in shp_di.keys():
        download_shapefiles_from_repo(shp_di[k]["src_dir"], shp_di[k]["prefix"])
        generate_minimal_json_from_shapefile(
            shp_di[k]["prefix"], shp_di[k]["poly_type"], shp_di[k]["retain"]
        )


def download_shapefiles_from_repo(target_dir, file_prefix):
    path = "data/shapefiles/"
    # For each required file of the shapefile, download and store locally.
    for filetype in ["dbf", "prj", "sbn", "sbx", "shp", "shx"]:
        try:
            url = f"https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/polygon/boundaries/{target_dir}/{file_prefix}.{filetype}?raw=true"
            r = requests.get(url, allow_redirects=True)
            open(f"{path}{file_prefix}.{filetype}", "wb").write(r.content)
        except:
            return 404


def generate_minimal_json_from_shapefile(file_prefix, poly_type, fields_retained):
    path = "data/shapefiles/"
    # Read shapefile into Geopandas data frame
    df = gpd.read_file(f"{path}{file_prefix}.shp")

    # Create a copy of the original data frame
    to_retain = ["id", "name"] + fields_retained

    x = df[to_retain].copy()

    # Create a new Pandas data frame from modified data.
    z = pd.DataFrame(x)

    # Create JSON data from Pandas data frame.
    shp_json = json.loads(z.T.to_json(orient="columns"))

    # Create a blank output list for appending JSON fields.
    output = []

    # For each feature in the JSON, add the "type" of the feature e.g. protected_area and append it to the output list.
    for key in shp_json:
        shp_json[key]["type"] = poly_type
        output.append(shp_json[key])

    # Dump JSON object to local JSON file, append to the file if it exists
    with open(json_types[poly_type + "s"], "w") as outfile:
        json.dump(output, outfile)
