from flask import Blueprint, render_template, Response

import geopandas as gpd
import json
import os
import pandas as pd
import requests
from shapely.geometry import Point, box

# local imports
from . import routes
from luts import (
    json_types,
    huc8_gdf,
    akpa_gdf,
    akco_gdf,
    aketh_gdf,
    akclim_gdf,
    akfire_gdf,
    proximity_search_radius_m,
)
from config import EAST_BBOX, WEST_BBOX
from validate_request import validate_latlon
from validate_data import is_di_empty

data_api = Blueprint("data_api", __name__)


@routes.route("/places/search/<lat>/<lon>")
def find_containing_polygons(lat, lon):
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
    p = create_point_gdf(float(lat), float(lon))
    p_buff = create_buffered_point_gdf(p)

    geo_suggestions = {}

    within_di = {}
    huc_di = fetch_huc_containing_point(p)
    akpa_di = fetch_akpa_containing_point(p)
    within_di.update(huc_di)
    within_di.update(akpa_di)

    proximal_di = {}
    near_huc_di, huc_tb = fetch_huc_near_point(p_buff)
    near_akpa_di, pa_tb = fetch_akpa_near_point(p_buff)
    bbox_ids = ["xmin", "ymin", "xmax", "ymax"]
    huc_bb = box(*huc_tb)
    pa_bb = box(*pa_tb)

    proximal_di.update(near_huc_di)
    proximal_di.update(near_akpa_di)

    geo_suggestions.update(within_di)
    geo_suggestions.update(proximal_di)

    empty_di_validation = is_di_empty(geo_suggestions)
    if empty_di_validation == 204:
        return geo_suggestions, 204

    if huc_bb.area >= pa_bb.area:
        geo_suggestions["total_bounds"] = dict(zip(bbox_ids, list(huc_tb)))
    else:
        geo_suggestions["total_bounds"] = dict(zip(bbox_ids, list(pa_tb)))
    return geo_suggestions


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


def create_point_gdf(lat, lon):
    p = Point(lon, lat)
    p_gdf = gpd.GeoDataFrame({"geometry": [p]}, crs=4326)
    return p_gdf


def create_buffered_point_gdf(pt):
    p_buff = pt.to_crs(3338)
    p_buff.geometry = p_buff.buffer(proximity_search_radius_m)
    return p_buff


def execute_spatial_join(left, right, predicate):
    joined = gpd.sjoin(left, right, how="left", predicate=predicate)
    return joined


def package_polys(poly_key, join, poly_type, gdf):

    di = {}
    di[poly_key] = {}

    if join.isna().any().any():
        return di
    else:
        for k in range(len(join)):
            di[poly_key][k] = {}
            di[poly_key][k]["name"] = join.name.values[k]
            di[poly_key][k]["type"] = poly_type
            f_id = join.id.values[k]
            di[poly_key][k]["geojson"] = gpd.GeoSeries(gdf.loc[f_id].geometry).to_json()
            di[poly_key][k]["id"] = f_id
    return di


def fetch_huc_near_point(pt):
    join = execute_spatial_join(pt, huc8_gdf.reset_index(), "intersects")
    tb = join.to_crs(4326).total_bounds
    di = package_polys("hucs_near", join, "huc", huc8_gdf)
    return di, tb


def fetch_akpa_near_point(pt):
    join = execute_spatial_join(pt, akpa_gdf.reset_index(), "intersects")
    tb = join.to_crs(4326).total_bounds
    di = package_polys("protected_areas_near", join, "protected_area", akpa_gdf)
    return di, tb


def fetch_huc_containing_point(pt):

    join = execute_spatial_join(pt, huc8_gdf.reset_index().to_crs(4326), "within")
    di = {}
    di["hucs"] = {}
    for k in range(len(join)):
        di["hucs"][k] = {}
        di["hucs"][k]["name"] = join.name.values[k]
        di["hucs"][k]["type"] = "huc"
        huc_code = join.id.values[k]
        di["hucs"][k]["geojson"] = gpd.GeoSeries(
            huc8_gdf.to_crs(4326).loc[huc_code].geometry
        ).to_json()
        di["hucs"][k]["id"] = huc_code
    return di


def fetch_akpa_containing_point(pt):

    join = execute_spatial_join(pt, akpa_gdf.reset_index().to_crs(4326), "within")

    di = {}
    di["protected_areas"] = {}
    if join.isna().any().any():
        return di
    else:
        for k in range(len(join)):
            di["protected_areas"][k] = {}
            di["protected_areas"][k]["name"] = join.name.values[k]
            di["protected_areas"][k]["type"] = "protected_area"
            di["protected_areas"][k]["area_type"] = join.area_type.values[k]
            akpa_id = join.id.values[k]
            di["protected_areas"][k]["geojson"] = gpd.GeoSeries(
                akpa_gdf.to_crs(4326).loc[akpa_id].geometry
            ).to_json()
            di["protected_areas"][k]["id"] = akpa_id
        return di
