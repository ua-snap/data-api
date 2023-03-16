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
    shp_types,
    shp_di,
    all_jsons,
    areas_near,
    type_di,
    valid_huc_ids,
    load_gdfs,
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


@routes.route("/places/<area_type>")
def get_json_for_type(area_type, recurse=False):
    """
    GET function to pull JSON files
       Args:
           area_type (string): Any of the below types:
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
    if area_type == "all":
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
        if area_type == "communities":

            # Create a copy of the community GDF and delete the
            # geometry column.
            comm_gdf = type_di["community"].copy()
            comm_gdf = comm_gdf.drop(columns=["geometry"])

            # Iterate over all communities and add to list
            for index, community in comm_gdf.iterrows():
                js_list.append(community.to_dict())
        else:
            # Remove the 's' at the end of the type
            area_type = area_type[:-1]

            # Create a copy of the current area type's GDF and
            # delete the geometry column.
            area_gdf = type_di[area_type].copy()
            area_gdf = area_gdf.drop(columns=["geometry"])

            # Iterate over all the current area type's AOIs
            # and add to list.
            for id, area in area_gdf.iterrows():
                area_dict = area.to_dict()
                # Add the ID to the dictionary
                area_dict["id"] = id
                js_list.append(area_dict)

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
    type_di, valid_huc_ids = load_gdfs()
    return Response(
        response='{ "success": "True" }', status=200, mimetype="application/json"
    )


def update_data():
    """Downloads AOI shapefiles from Geoserver WFS request for all areas

    Args:
        None.

    Returns:
        Boolean value indicating success or failure to update datasets.
        The underlying code updates all the polygonal areas as local shapefiles
        for rapid start-up of API and generation of GeoPanda GeoDataFrames.
    """

    # Ensure the path to store shapefiles is created
    shppath = "data/shapefiles/"
    if not os.path.exists(shppath):
        os.makedirs(shppath)
    else:
        shutil.rmtree(shppath)
        os.makedirs(shppath)

    crs = "EPSG:4326"

    # Requests all point locations for communities
    comms_resp = requests.get(
        generate_wfs_places_url("all_boundaries:all_communities"),
        allow_redirects=True,
    )
    all_comms = json.loads(comms_resp.content)["features"]

    # Creates a GeoDataFrame from all the communities returned from GeoServer
    comms_gdf = gpd.GeoDataFrame.from_features(all_comms)
    comms_gdf = comms_gdf.drop(columns=["km_distanc"])

    # Writes a cached copy of the community point locations to a point shapefile
    comms_gdf.to_file(
        shp_types["communities"],
        driver="ESRI Shapefile",
        crs=crs,
        encoding="utf-8",
    )

    # Requests all polygons that make up all areas in the state of Alaska
    areas_resp = requests.get(
        generate_wfs_places_url("all_boundaries:all_areas"),
        allow_redirects=True,
    )
    all_areas = json.loads(areas_resp.content)["features"]

    # Creates a GeoDataFrame from all the features returned from GeoServer
    areas_gdf = gpd.GeoDataFrame.from_features(all_areas)

    for k in shp_di.keys():
        # If the key is for Alaska HUC12 polygons, we need to download the
        # remote shapefile as it has not been imported into GS.
        if k == "akhuc12s":
            download_shapefiles_from_repo(
                shp_di["akhuc12s"]["src_dir"], shp_di["akhuc12s"]["prefix"]
            )
            continue

        # Pulls out any fields from the GeoDataFrame that are not relevant to
        # the 'type' of area in the data.
        curr_gdf = areas_gdf.loc[areas_gdf["type"] == shp_di[k]["poly_type"]]
        remove_columns = ["alt_name", "area_type"]

        # If the shapefile dictionary has a retain section, it removes the
        # field not to be deleted from the 'remove_columns' list.
        if "retain" in shp_di[k]:
            retain_index = remove_columns.index(shp_di[k]["retain"])
            remove_columns.pop(retain_index)
        curr_gdf = curr_gdf.drop(columns=remove_columns)

        # Writes the GDF to a local shapefile for rapid restarts of the
        # server when we don't need to update from GeoServer.
        curr_gdf.to_file(
            shp_types[shp_di[k]["poly_type"] + "s"],
            driver="ESRI Shapefile",
            crs=crs,
            encoding="utf-8",
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
