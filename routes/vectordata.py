from flask import Blueprint, render_template, Response, request
import asyncio
import geopandas as gpd
import json
import pandas as pd
import os
from shapely.geometry import shape, Point
import jaro

# local imports
from . import routes
from luts import (
    all_jsons,
    areas_near,
)
from config import EAST_BBOX, WEST_BBOX, geojson_names
from validate_request import validate_latlon
from generate_urls import generate_wfs_search_url, generate_wfs_places_url
from fetch_data import fetch_data
from csv_functions import create_csv

data_api = Blueprint("data_api", __name__)

extent_filtered_communities = {}

all_communities_full = asyncio.run(
    fetch_data(
        [
            generate_wfs_places_url(
                "all_boundaries:all_communities",
                "name,alt_name,id,region,country,type,latitude,longitude,tags,is_coastal,ocean_lat1,ocean_lon1",
            )
        ]
    )
)["features"]

for extent in geojson_names:
    geojson_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "geojsons", f"{extent}.geojson"
    )
    gdf_extent = gpd.read_file(geojson_path)
    gdf_extent = gdf_extent.set_crs(epsg=4326, allow_override=True)
    region_geom = gdf_extent.unary_union
    filtered = []
    for community in all_communities_full:
        lat = float(community["properties"].get("latitude", 0))
        lon = float(community["properties"].get("longitude", 0))
        pt = Point(lon, lat)
        if region_geom.contains(pt):
            filtered.append(community)
    extent_filtered_communities[extent] = filtered


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
    communities_json = asyncio.run(
        fetch_data(
            [generate_wfs_search_url("all_boundaries:all_communities", lat, lon)]
        )
    )

    nearby_communities = communities_json["features"]
    filtered_communities = filter_by_tag(nearby_communities)

    # Dictionary containing all the communities and
    # polygon areas by the end of this function.
    proximal_di = dict()
    proximal_di["communities"] = dict()

    # For each returned community, grab its name,
    # alternate name, id, lat, lon, and type. They are all
    # found within the properties of the returned JSON.
    for i in range(len(filtered_communities)):
        proximal_di["communities"][i] = filtered_communities[i]["properties"]

    # WFS request to Geoserver for all polygon areas.
    nearby_areas = asyncio.run(
        fetch_data([generate_wfs_search_url("all_boundaries:all_areas", lat, lon)])
    )["features"]

    # Create the JSON section for each of the area types.
    for area_type in areas_near.values():
        proximal_di[area_type] = dict()

    # For each returned area, place it inside the correct area type.
    # We want to collect the area's geometry, id, name, and type.
    for ai in range(len(nearby_areas)):
        # HUC12s do not play well with Northern Climate Reports.
        # Remove them from /places endpoints for now.
        if nearby_areas[ai]["properties"]["area_type"] == "HUC12":
            continue

        current_area_type = areas_near[nearby_areas[ai]["properties"]["type"]]
        current_index = len(proximal_di[current_area_type])
        proximal_di[current_area_type][current_index] = gather_nearby_area(
            nearby_areas[ai]
        )

    # Check to see if any communities were found around the point chosen
    communities_found = (
        filtered_communities if communities_json["numberMatched"] > 0 else None
    )

    # Get the total bounds for the communities, HUCs, and protected areas only
    total_bounds = get_total_bounds(nearby_areas, communities_found)

    # Bounding box keys
    bbox_ids = ["xmin", "ymin", "xmax", "ymax"]

    # Generates bounding box from keys above and the values of the total_bounds
    proximal_di["total_bounds"] = dict(zip(bbox_ids, list(total_bounds)))

    return Response(
        response=json.dumps(proximal_di), status=200, mimetype="application/json"
    )


def get_total_bounds(nearby_areas, communities=None):
    """
    Generates the total bounds of the returned data from a search, but only for
    communities, HUC8s, and protected areas.

    Args:
        nearby_areas: A JSON response containing all AOI polygons from GeoServer.
        communities: Either the JSON response containing all communities nearby or False

    Returns:
        Bounding box for AOI for all communities, HUC8s, and protected areas nearby the
        selected latitude and longitude.

        Returns as Python list with order [xmin, ymin, xmax, ymax]
    """

    # Create a GeoPandas GeoDataFrame from all of the nearby areas GeoJSON
    areas_gdf = gpd.GeoDataFrame.from_features(nearby_areas)

    # Make a new GeoPandas GeoDataFrome which contains only the HUCs and protected areas
    huc_pa_gdf = areas_gdf[
        areas_gdf["type"].isin(
            ["huc", "protected_area", "yt_watershed", "yt_game_management_zone"]
        )
    ].copy()

    # If there were any nearby communities, we want to ensure our
    # bounding box includes them.
    if communities is not None:
        # Create a GeoPandas GeoDataFrame from the communities
        communities_gdf = gpd.GeoDataFrame.from_features(communities)

        # Combines the communities and HUC / PA GDFs
        combined_gdf = pd.concat([communities_gdf, huc_pa_gdf], ignore_index=True)

        # Gets total bounds of combined GDF
        total_bounds = combined_gdf.total_bounds
    else:
        # If no communities are returned from the search, the HUCs and protected areas
        # bounding box should be used.
        total_bounds = huc_pa_gdf.total_bounds

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


def filter_by_tag(communities):
    """
    Filters communities by tags if tags are provided in the request.

    Args:
        communities: All communities returned from the WFS request.

    Returns:
        Communities with the tags provided in the request, with the tags removed
        from the output after filtering.
    """
    if request.args.get("tags"):
        tags = request.args.get("tags").split(",")
        filtered_communities = []
        for community in communities:
            community_added = False
            for tag in tags:
                if not community_added:
                    community_tags = community["properties"]["tags"].split(",")
                    if tag in community_tags:
                        # Remove tags property from output
                        del community["properties"]["tags"]

                        filtered_communities.append(community)
                        community_added = True
        return filtered_communities
    else:
        return communities


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
            all_communities = asyncio.run(
                fetch_data(
                    [
                        generate_wfs_places_url(
                            "all_boundaries:all_communities",
                            "name,alt_name,id,region,country,type,latitude,longitude,tags,is_coastal,ocean_lat1,ocean_lon1",
                        )
                    ]
                )
            )["features"]

            filtered_communities = filter_by_tag(all_communities)

            # For each feature, put the properties (name, id, etc.) into the
            # list for creation of a JSON object to be returned.
            for i in range(len(filtered_communities)):
                js_list.append(filtered_communities[i]["properties"])
        else:
            # Remove the 's' at the end of the type
            type = type[:-1]

            # Requests the Geoserver WFS URL for gathering all the polygon areas
            all_areas = asyncio.run(
                fetch_data(
                    [
                        generate_wfs_places_url(
                            "all_boundaries:all_areas",
                            "id,name,type,area_type",
                            type,
                        )
                    ]
                )
            )["features"]

            # For each feature, put the properties (name, id, type) into the
            # list for creation of a JSON object to be returned.
            for ai in range(len(all_areas)):
                # HUC12s do not play well with Northern Climate Reports.
                # Remove them from /places endpoints for now.
                if all_areas[ai]["properties"]["area_type"] == "HUC12":
                    continue

                # If this area is a protected_area, keep area_type in
                # returned output.
                if all_areas[ai]["properties"]["area_type"] != "":
                    js_list.append(all_areas[ai]["properties"])
                else:
                    del all_areas[ai]["properties"]["area_type"]
                    js_list.append(all_areas[ai]["properties"])

        # Creates JSON object from created list
        js = json.dumps(js_list)

    if recurse:
        return js

    if request.args.get("format") == "csv":
        return create_csv(json.loads(js), "places_" + type)

    # Returns Flask JSON Response
    return Response(response=js, status=200, mimetype="application/json")


@routes.route("/places/search/communities")
def get_communities():
    """
    GET function to return communities filtered by extent and substring.
    Query params:
        extent: alaska, blockyAlaska, elevation, mizukami, or slie (GeoJSON region)
        substring: substring to match in name or alt_name. Also performs fuzzy matching.
    Returns:
        JSON of filtered communities.
    """

    # Filter by precomputed extent if provided
    extent = request.args.get("extent")
    if extent in geojson_names:
        all_communities = extent_filtered_communities[extent]
    else:
        all_communities = all_communities_full

    # Filter by substring if provided
    substring = request.args.get("substring")
    if substring:
        substring = substring.lower()
        filtered_exact = []
        filtered_fuzzy_with_scores = []
        seen_ids = set()
        for community in all_communities:
            name = community["properties"].get("name", "").lower()
            alt_name = community["properties"].get("alt_name", "").lower()
            community_id = community["properties"].get("id")
            if substring in name or substring in alt_name:
                filtered_exact.append(community)
                # Add community ID to the seen_ids set to avoid duplicates
                seen_ids.add(community_id)

            # Runs fuzzy matching for names and alt_names that are
            # one character off or very similar to the substring.
            ratio_name = jaro.jaro_winkler_metric(substring, name)
            ratio_alt = jaro.jaro_winkler_metric(substring, alt_name) if alt_name else 0
            max_ratio = max(ratio_name, ratio_alt)

            if max_ratio >= 0.85 and community_id not in seen_ids:
                filtered_fuzzy_with_scores.append((community, max_ratio))
                seen_ids.add(community_id)

        # Sort the fuzzy matches by their Jaro-Winkler score in descending order
        filtered_fuzzy_with_scores.sort(key=lambda x: x[1], reverse=True)
        filtered_fuzzy = [community for community, score in filtered_fuzzy_with_scores]

        all_communities = filtered_exact + filtered_fuzzy

    output = [c["properties"] for c in all_communities]

    return Response(
        response=json.dumps(output), status=200, mimetype="application/json"
    )
