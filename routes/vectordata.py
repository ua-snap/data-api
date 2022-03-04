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
    huc8_search_gdf,
    akpa_gdf,
    # akco_gdf,
    # aketh_gdf,
    # akclim_gdf,
    # akfire_gdf,
    proximity_search_radius_m,
    community_search_radius_m,
)
from config import EAST_BBOX, WEST_BBOX
from validate_request import validate_latlon
from validate_data import is_di_empty, recursive_rounding

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
    p_buff = create_buffered_point_gdf(p, proximity_search_radius_m)
    p_buff_community = create_buffered_point_gdf(p, community_search_radius_m)

    geo_suggestions = {}

    proximal_di = {}
    try:
        near_huc_di, huc_tb = fetch_huc_near_point(p_buff)
        huc_bb = box(*huc_tb)
    except ValueError:
        near_huc_di, huc_bb = {}, box(*[1, 1, 1, 1])
    try:
        near_akpa_di, pa_tb = fetch_akpa_near_point(p_buff)
        pa_bb = box(*pa_tb)
    except ValueError:
        near_akpa_di, pa_bb = {}, box(*[1, 1, 1, 1])

    df = csv_to4326_gdf("data/csvs/ak_communities.csv")
    nearby_points_di = package_nearby_points(
        find_nearest_communities(p_buff_community, df).fillna("None")
    )

    proximal_di.update(near_huc_di)
    proximal_di.update(near_akpa_di)
    proximal_di.update(nearby_points_di)

    geo_suggestions.update(proximal_di)

    empty_di_validation = is_di_empty(geo_suggestions)
    if empty_di_validation == 404:
        return geo_suggestions, 404

    bbox_ids = ["xmin", "ymin", "xmax", "ymax"]
    if huc_bb.area >= pa_bb.area:
        geo_suggestions["total_bounds"] = dict(zip(bbox_ids, list(huc_tb)))
    else:
        geo_suggestions["total_bounds"] = dict(zip(bbox_ids, list(pa_tb)))
    return recursive_rounding(geo_suggestions.keys(), geo_suggestions.values())


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


def create_buffered_point_gdf(pt, radius):
    p_buff = pt.to_crs(3338)
    p_buff.geometry = p_buff.buffer(radius)
    return p_buff


def execute_spatial_join(left, right, predicate):
    joined = gpd.sjoin(left, right, how="left", predicate=predicate)
    return joined


def package_polys(poly_key, join, poly_type, gdf, to_wgs=False):
    di = {}
    di[poly_key] = {}

    if join.isna().any().any():
        return di
    else:
        f_ids = []
        for k in range(len(join)):
            di[poly_key][k] = {}
            di[poly_key][k]["name"] = join.name.values[k]
            di[poly_key][k]["type"] = poly_type
            f_id = join.id.values[k]
            if to_wgs:
                geojson = gpd.GeoSeries(gdf.to_crs(4326).loc[f_id].geometry).to_json()
            else:
                geojson = gpd.GeoSeries(gdf.loc[f_id].geometry).to_json()
            di[poly_key][k]["geojson"] = json.loads(geojson)["features"][0]["geometry"]
            di[poly_key][k]["id"] = f_id
            f_ids.append(f_id)
        new_gdf = gdf.loc[gdf.index.isin(f_ids)]
        tb = new_gdf.to_crs(4326).total_bounds.round(4)
    return di, tb


def package_nearby_points(nearby):
    di = {}
    di["communities"] = {}
    if nearby.isna().all(axis=1).all():
        return di
    else:
        for k in range(len(nearby)):
            comm_di = nearby.iloc[k].to_dict()
            comm_di["type"] = "community"
            di["communities"][k] = comm_di
    return di


def fetch_huc_near_point(pt):
    join = execute_spatial_join(pt, huc8_search_gdf.reset_index(), "intersects")
    di, tb = package_polys("hucs_near", join, "huc", huc8_search_gdf, to_wgs=True)
    return di, tb


def fetch_akpa_near_point(pt):
    join = execute_spatial_join(pt, akpa_gdf.reset_index(), "intersects")
    di, tb = package_polys(
        "protected_areas_near", join, "protected_area", akpa_gdf, to_wgs=True
    )
    return di, tb


def read_tabular(raw_file, header_row="infer"):
    """Read data (*. xls, *.dat, *.csv, etc.) to DataFrame"""
    if raw_file.split(".")[-1][:2] == "xl":
        raw_df = pd.read_excel(raw_file, header=header_row)
    else:
        raw_df = pd.read_csv(raw_file, header=header_row)
    return raw_df


def create_geometry(df):
    """Add Geometry column to specify that these are spatial coordinates for
    point vector data"""
    df["geometry"] = df.apply(
        lambda x: Point((float(x["longitude"]), float(x["latitude"]))), axis=1
    )
    return df


def create_geodataframe(df):
    """Create GeoDataFrame with WGS 84 Spatial Reference"""
    gdf = gpd.GeoDataFrame(df, geometry="geometry")
    gdf.crs = "epsg:4326"
    return gdf


def csv_to4326_gdf(fp):
    df = create_geodataframe(create_geometry(read_tabular(fp)))
    return df


def find_nearest_communities(pt, df):
    nearby = gpd.sjoin_nearest(
        pt.to_crs(3338), df.to_crs(3338), how="inner", max_distance=50000
    )
    return nearby[["name", "alt_name", "id", "latitude", "longitude"]]
