from flask import render_template, Response, request
import asyncio
import json
import requests
import pandas as pd

# local imports
from . import routes
from luts import species_crosswalk
from config import WEST_BBOX, EAST_BBOX

from validate_request import validate_latlon
from generate_urls import generate_wfs_places_url, generate_wfs_species_huc12_intersection_url
from fetch_data import fetch_data
from csv_functions import create_csv


@routes.route("/species/<lat>/<lon>")
def fetch_species_data_by_lat_lon(lat, lon):
    """Get species data by querying for HUC12 that intersects a given lat/lon coordinate. 
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        JSON-like dict of species by type. The dictionary does not include reference
        to outdated HUC12 ID that was used to query species dataset."""

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
    
    with requests.get(generate_wfs_species_huc12_intersection_url(lat, lon)) as r:
        if len(r.json()['features']) < 1:
            return render_template("404/no_data.html"), 404
        else:
            huc12 = r.json()['features'][0]['properties']['HUC_12']

    df = pd.DataFrame(columns=["HUC_12", "type", "species_id", "common_name", "scientific_name"])
    
    with requests.get(generate_wfs_places_url("species:huc12_species_lookup", filter_type="HUC_12", filter=huc12)) as r:
        for feature in r.json()["features"]:
            row_dict = {"HUC_12" : [feature["properties"]["HUC_12"]],
                        "type" : [feature["properties"]["type"]], 
                        "species_id" : [feature["properties"]["species_ID"]],
                        "common_name" : [species_crosswalk[feature["properties"]["species_ID"]]["common_name"]],
                        "scientific_name" : [species_crosswalk[feature["properties"]["species_ID"]]["scientific_name"]],
            }
            df = pd.concat([df, pd.DataFrame(row_dict)], ignore_index=True)

    birds_list = []
    mammals_list = []
    amphibians_list = []

    for index_, row in df.iterrows():
        if row.type == "birds":
            birds_list.append([row['common_name'], row['scientific_name']])
        if row.type == "mammals":
            mammals_list.append([row['common_name'], row['scientific_name']])
        if row.type == "amphibians":
            amphibians_list.append([row['common_name'], row['scientific_name']])

    results = {
            "total_sgcn_species" : str(len(df)),
            "birds" : birds_list,
            "mammals" : mammals_list,
            "amphibians" : amphibians_list
        }
    
    # TODO: add CSV explort
    # if request.args.get("format") == "csv":
    #     return create_csv(......)

    return results


@routes.route("/species/")
def species_about():
    return render_template("/documentation/species.html")



