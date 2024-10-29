import requests
import io
import numpy as np
import xarray as xr
import json
import geopandas as gpd
import xml.etree.ElementTree as ET
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)

from generate_requests import generate_conus_hydrology_wcs_str
from generate_urls import generate_wfs_conus_hydrology_url
from config import RAS_BASE_URL
from . import routes

cov_id = "conus_hydro_segments_crstephenson"
# TODO: change to "Rasdaman Encoding" to disambiguate from the encoding attribute in the netCDF file
encoding_attr = "Encoding"


def fetch_hydrology_data(cov_id, geom_id):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one geometry ID at a time!
    Args:
        coverage_id (str): Coverage ID for the hydrology data
        geom_id (str): Geometry ID for the hydrology data

    Returns:
        Xarray dataset with hydrological stats for the all var/lc/model/scenario/era combinations for the requested geom ID.
    """

    url = RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, geom_id)
    print(url)

    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        ds = xr.open_dataset(io.BytesIO(r.content))

    return ds


def build_decode_dicts(ds, encoding_attr):
    """
    Function to build decoding dictionaries.
    Searches the XML response from the DescribeCoverage request for the encodings metadata and
    returns the dictionary of encodings. Reverses the dictionary of encodingsd so we can decodes
    and return dimensions as strings.
    Args:
        ds (xarray dataset): Dataset with hydrological stats for the geom ID
        encoding_attr (str): Attribute name for the encoding dictionary in the XML response
    Returns:
        Decoded data dictionary with human-readable keys."""

    url = (
        RAS_BASE_URL
        + f"ows?&SERVICE=WCS&VERSION=2.1.0&REQUEST=DescribeCoverage&COVERAGEID={cov_id}&outputType=GeneralGridCoverage"
    )
    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        tree = ET.ElementTree(ET.fromstring(r.content))

    xml_search_string = str(".//{http://www.rasdaman.org}" + encoding_attr)
    encoding_dict_str = tree.findall(xml_search_string)[0].text
    encoding_dict = eval(encoding_dict_str)

    # for each dimension, reverse the encoding dictionary to decode the keys
    # also convert to float, since this is the dtype of the encoded values in the datacube
    # TODO: fix netCDF encoding to use integers instead of floats!
    lc_dict = {float(v): k for k, v in encoding_dict["lc"].items()}
    model_dict = {float(v): k for k, v in encoding_dict["model"].items()}
    scenario_dict = {float(v): k for k, v in encoding_dict["scenario"].items()}
    era_dict = {float(v): k for k, v in encoding_dict["era"].items()}

    return lc_dict, model_dict, scenario_dict, era_dict


def build_dict_and_populate_stats(geom_id, ds):
    """
    Function to populate the stats in the data dictionary with the hydrology statistics.
    The levels of the stats data dictionary are as follows: landcover, model, scenario, era, variable.
    Args:
        geom_id (str): Geometry ID for the hydrology data
        ds (xarray dataset): Dataset with hydrological stats for the geom ID
        data_dict (dict): Data dictionary to populate with the hydrology stats
    Returns:
        Data dictionary with the hydrology stats populated.
    """

    lc_dict, model_dict, scenario_dict, era_dict = build_decode_dicts(ds, encoding_attr)

    data_dict = {
        geom_id: {"name": None, "latitude": None, "longitude": None, "stats": {}}
    }

    # get the stats from the dataset for each landcover, model, scenario, era, and variable.
    vars = list(ds.data_vars)
    for lc in ds.lc.values:
        data_dict[geom_id]["stats"][lc_dict[lc]] = {}
        for model in ds.model.values:
            data_dict[geom_id]["stats"][lc_dict[lc]][model_dict[model]] = {}
            for scenario in ds.scenario.values:
                data_dict[geom_id]["stats"][lc_dict[lc]][model_dict[model]][
                    scenario_dict[scenario]
                ] = {}
                # if scenario is historical, get only the first era values (all others are null)
                if scenario_dict[scenario] == "historical":
                    for era in ds.era.values[:1]:
                        data_dict[geom_id]["stats"][lc_dict[lc]][model_dict[model]][
                            scenario_dict[scenario]
                        ][era_dict[era]] = {}
                        stats_dict = {}
                        for var in vars:
                            stat_value = float(
                                ds[var].sel(
                                    lc=lc, model=model, scenario=scenario, era=era
                                )
                            )

                            if np.isnan(stat_value):
                                stat_value = None

                            stats_dict[var] = stat_value

                            data_dict[geom_id]["stats"][lc_dict[lc]][model_dict[model]][
                                scenario_dict[scenario]
                            ][era_dict[era]] = stats_dict
                # if scenario is not historical, get all era values except the first (which is null)
                else:
                    for era in ds.era.values[1:]:
                        data_dict[geom_id]["stats"][lc_dict[lc]][model_dict[model]][
                            scenario_dict[scenario]
                        ][era_dict[era]] = {}
                        stats_dict = {}
                        for var in vars:
                            stat_value = float(
                                ds[var].sel(
                                    lc=lc, model=model, scenario=scenario, era=era
                                )
                            )

                            if np.isnan(stat_value):
                                stat_value = None

                            stats_dict[var] = stat_value
                            data_dict[geom_id]["stats"][lc_dict[lc]][model_dict[model]][
                                scenario_dict[scenario]
                            ][era_dict[era]] = stats_dict

    return data_dict


def get_features_and_populate_attributes(data_dict):
    """Function to populate the data dictionary with the attributes from the vector data.
    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
    Returns:
        Data dictionary with the vector attributes populated."""
    for geom_id in data_dict.keys():
        url = generate_wfs_conus_hydrology_url(geom_id)

        # get the features
        with requests.get(
            url, verify=False
        ) as r:  # verify=False is necessary for dev version of Geoserver
            if r.status_code != 200:
                return render_template("500/server_error.html"), 500
            else:
                try:
                    r_json = r.json()
                except:
                    print("Unable to decode as JSON, got raw text:\n", r.text)
                    return render_template("500/server_error.html"), 500

        # save json to test size of return
        with open("/home/jdpaul3/segments.json", "w", encoding="utf-8") as f:
            json.dump(r_json, f, ensure_ascii=False, indent=4)

        # create a valid geodataframe from the features and find a representation point on the line segment
        # CRS is hardcoded to EPSG:5070!
        seg_gdf = gpd.GeoDataFrame.from_features(r_json["features"], crs="EPSG:5070")
        seg_gdf["geometry"] = seg_gdf["geometry"].make_valid()

        rep_x_coord = seg_gdf.loc[0].geometry.representative_point().x
        rep_y_coord = seg_gdf.loc[0].geometry.representative_point().y

        data_dict[geom_id]["name"] = seg_gdf.loc[0].GNIS_NAME
        data_dict[geom_id]["latitude"] = rep_y_coord
        data_dict[geom_id]["longitude"] = rep_x_coord

    return data_dict


@routes.route("/conus_hydrology/")
def conus_hydrology_about():
    return render_template("/documentation/conus_hydrology.html")


@routes.route("/conus_hydrology/<geom_id>")
def run_get_conus_hydrology_point_data(geom_id):
    """
    Function to fetch hydrology data from Rasdaman for a single geometry ID.
    Example URL: http://localhost:5000/conus_hydrology/1000
    Args:
        geom_id (str): Geometry ID for the hydrology data
    Returns:
        JSON response with hydrological stats for the requested geom ID.
    """

    ds = fetch_hydrology_data(cov_id, geom_id)
    # save nc to test size of return
    ds.to_netcdf("/home/jdpaul3/stats_from_geom_id.nc", engine="h5netcdf")

    # build the data dictionary and populate with the hydrology statistics
    data_dict = build_dict_and_populate_stats(geom_id, ds)

    # populate attributes from vector data
    data_dict = get_features_and_populate_attributes(data_dict)

    # convert to JSON
    json_results = json.dumps(data_dict, indent=4)

    # save json to test size of return
    with open("/home/jdpaul3/result.json", "w", encoding="utf-8") as f:
        json.dump(json_results, f)

    return Response(json_results, mimetype="application/json")
