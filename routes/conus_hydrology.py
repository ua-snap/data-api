import requests
import asyncio
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
from fetch_data import fetch_data, describe_via_wcps
from validate_request import get_axis_encodings, get_coverage_encodings
from postprocessing import prune_nulls_with_max_intensity
from config import RAS_BASE_URL
from . import routes

# TODO: Add validation of stream ID
# TODO: Improve error handling throughout

seg_cov_id = "conus_hydro_segments_all_stats"

hydrograph_hist_cov_id = "conus_hydro_segments_doy_mmm_maurer_historical_test_2"
hydrograph_proj_cov_id = "conus_hydro_segments_doy_mmm_maurer_projected_test"


def fetch_hydro_data(cov_id, stream_id, type):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one stream ID at a time!
    Args:
        coverage_id (str): Coverage ID for the hydrology data
        stream_id (str): Stream ID for the hydrology data

    Returns:
        Xarray dataset with hydrological stats for the all var/lc/model/scenario/era combinations for the requested stream ID.
    """
    url = RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, stream_id, type)
    response = asyncio.run(fetch_data([url]))
    ds = xr.open_dataset(io.BytesIO(response))

    return ds


async def get_decode_dicts_from_axis_attributes(cov_id):
    metadata = await describe_via_wcps(cov_id)
    return get_axis_encodings(metadata)


async def get_decode_dicts_from_coverage_attributes(cov_id):
    metadata = await describe_via_wcps(cov_id)
    return get_coverage_encodings(metadata)


# TODO: replace this function with get_decode_dicts_from_axis_attributes()
def build_decode_dicts(seg_encoding_attr):
    """
    Function to build decoding dictionaries.
    Searches the XML response from the DescribeCoverage request for the encodings metadata and
    returns the dictionary of encodings. Reverses the dictionary of encodingsd so we can decodes
    and return dimensions as strings.
    Args:
        ds (xarray dataset): Dataset with hydrological stats for the stream ID
        seg_encoding_attr (str): Attribute name for the encoding dictionary in the XML response
    Returns:
        Decoded data dictionary with human-readable keys."""

    url = (
        RAS_BASE_URL
        + f"ows?&SERVICE=WCS&VERSION=2.1.0&REQUEST=DescribeCoverage&COVERAGEID={seg_cov_id}&outputType=GeneralGridCoverage"
    )
    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        tree = ET.ElementTree(ET.fromstring(r.content))

    xml_search_string = str(".//{http://www.rasdaman.org}" + seg_encoding_attr)
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


# TODO: condense into a package_stats_data() function
def build_dict_and_populate_stats(stream_id, ds):
    """
    Function to populate the stats in the data dictionary with the hydrology statistics.
    The levels of the stats data dictionary are as follows: landcover, model, scenario, era, variable.
    Args:
        stream_id (str): Stream ID for the hydrology data
        ds (xarray dataset): Dataset with hydrological stats for the stream ID
        data_dict (dict): Data dictionary to populate with the hydrology stats
    Returns:
        Data dictionary with the hydrology stats populated.
    """

    lc_dict, model_dict, scenario_dict, era_dict = build_decode_dicts(seg_encoding_attr)

    data_dict = {
        stream_id: {"name": None, "latitude": None, "longitude": None, "stats": {}}
    }

    # get the stats from the dataset for each landcover, model, scenario, era, and variable.
    vars = list(ds.data_vars)
    for lc in ds.lc.values:
        data_dict[stream_id]["stats"][lc_dict[lc]] = {}
        for model in ds.model.values:
            data_dict[stream_id]["stats"][lc_dict[lc]][model_dict[model]] = {}
            for scenario in ds.scenario.values:
                data_dict[stream_id]["stats"][lc_dict[lc]][model_dict[model]][
                    scenario_dict[scenario]
                ] = {}
                # if scenario is historical, get only the first era values (all others are null)
                if scenario_dict[scenario] == "historical":
                    for era in ds.era.values[:1]:
                        data_dict[stream_id]["stats"][lc_dict[lc]][model_dict[model]][
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

                            data_dict[stream_id]["stats"][lc_dict[lc]][
                                model_dict[model]
                            ][scenario_dict[scenario]][era_dict[era]] = stats_dict
                # if scenario is not historical, get all era values except the first (which is null)
                else:
                    for era in ds.era.values[1:]:
                        data_dict[stream_id]["stats"][lc_dict[lc]][model_dict[model]][
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
                            data_dict[stream_id]["stats"][lc_dict[lc]][
                                model_dict[model]
                            ][scenario_dict[scenario]][era_dict[era]] = stats_dict

    return data_dict


# TODO: condense into a package_stats_data() function
# TODO: can this be done via existing data fetching function?
def get_features_and_populate_attributes(data_dict):
    """Function to populate the data dictionary with the attributes from the vector data.
    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
    Returns:
        Data dictionary with the vector attributes populated."""
    for stream_id in data_dict.keys():
        url = generate_wfs_conus_hydrology_url(stream_id)

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
        with open("/tmp/segments.json", "w", encoding="utf-8") as f:
            json.dump(r_json, f, ensure_ascii=False, indent=4)

        # create a valid geodataframe from the features and find a representation point on the line segment
        # CRS is hardcoded to EPSG:5070!
        seg_gdf = gpd.GeoDataFrame.from_features(r_json["features"], crs="EPSG:5070")
        seg_gdf["geometry"] = seg_gdf["geometry"].make_valid()

        rep_x_coord = seg_gdf.loc[0].geometry.representative_point().x
        rep_y_coord = seg_gdf.loc[0].geometry.representative_point().y

        data_dict[stream_id]["name"] = seg_gdf.loc[0].GNIS_NAME
        data_dict[stream_id]["latitude"] = rep_y_coord
        data_dict[stream_id]["longitude"] = rep_x_coord

    return data_dict


def package_hydrograph_data(stream_id, ds_hist, ds_proj):
    """
    Function to package the hydrograph data into a dictionary for JSON serialization.
    The levels of the hydrograph data dictionary are as follows: landcover, model, scenario, era, variable.
    Streamflow values (cfs) are rounded to integers.
    Args:
        stream_id (str): Stream ID for the hydrology data
        ds_proj (xarray dataset): Dataset with historical era hydrograph data for the stream ID
        ds_hist (xarray dataset): Dataset with projected era hydrograph data for the stream ID
    Returns:
        Data dictionary with the hydrograph data packaged for JSON serialization.
    """
    hydrograph_dict = {
        stream_id: {"name": None, "latitude": None, "longitude": None, "data": {}}
    }

    for ds in [ds_hist, ds_proj]:
        for landcover in ds.landcover.values:
            if landcover not in hydrograph_dict[stream_id]["data"]:
                hydrograph_dict[stream_id]["data"][landcover] = {}
            for model in ds.model.values:
                if model not in hydrograph_dict[stream_id]["data"][landcover]:
                    hydrograph_dict[stream_id]["data"][landcover][model] = {}
                for scenario in ds.scenario.values:
                    if (
                        scenario
                        not in hydrograph_dict[stream_id]["data"][landcover][model]
                    ):
                        hydrograph_dict[stream_id]["data"][landcover][model][
                            scenario
                        ] = {}
                    for era in ds.era.values:
                        if (
                            era
                            not in hydrograph_dict[stream_id]["data"][landcover][model][
                                scenario
                            ]
                        ):
                            hydrograph_dict[stream_id]["data"][landcover][model][
                                scenario
                            ][era] = {}
                        # iterate over the doy dimension to get the hydrograph data points
                        # populate a dict with integer doy as key and a dict of variable values (min, mean, max) as values
                        for doy in ds.doy.values:
                            var_dict = {}
                            for var in list(ds.data_vars):
                                streamflow_value = (
                                    ds[var]
                                    .sel(
                                        landcover=landcover,
                                        model=model,
                                        scenario=scenario,
                                        era=era,
                                        doy=doy,
                                    )
                                    .values
                                )
                                if np.isnan(streamflow_value):
                                    streamflow_value = None
                                else:
                                    streamflow_value = int(streamflow_value)
                                var_dict[var] = streamflow_value
                            hydrograph_dict[stream_id]["data"][landcover][model][
                                scenario
                            ][era][int(doy)] = var_dict

    return hydrograph_dict


@routes.route("/conus_hydrology/")
def conus_hydrology_about():
    return render_template("/documentation/conus_hydrology.html")


@routes.route("/conus_hydrology/<stream_id>")
def run_get_conus_hydrology_stats_data(stream_id):
    """
    Function to fetch hydrology data from Rasdaman for a single stream ID.
    Example URL: http://localhost:5000/conus_hydrology/1000
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with hydrological stats for the requested stream ID.
    """

    # testing
    # print("ORIGINAL ENCODINGS OUTPUT:")
    # print(build_decode_dicts(seg_encoding_attr), "\n\n")
    decode_dict = asyncio.run(get_decode_dicts_from_axis_attributes(seg_cov_id))
    print("NEW ENCODINGS OUTPUT:")
    print(decode_dict)

    ds = fetch_hydro_data(seg_cov_id, stream_id, type="stats")

    # build the data dictionary and populate with the hydrology statistics
    data_dict = build_dict_and_populate_stats(stream_id, ds)

    # populate attributes from vector data
    data_dict = get_features_and_populate_attributes(data_dict)

    # convert to JSON
    json_results = json.dumps(data_dict, indent=4)

    return Response(json_results, mimetype="application/json")


@routes.route("/conus_hydrology/hydrograph/<stream_id>")
def run_get_conus_hydrology_hydrograph(stream_id):
    """
    Function to fetch hydrograph data from Rasdaman for a single stream ID.
    Example URL: http://localhost:5000/conus_hydrology/hydrograph/1000
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with hydrograph data for the requested stream ID.
    """
    decode_dict_hist = asyncio.run(
        get_decode_dicts_from_axis_attributes(hydrograph_hist_cov_id)
    )
    ds_hist = fetch_hydro_data(
        hydrograph_hist_cov_id, stream_id, type="hydrograph_hist"
    )
    decode_dict_proj = asyncio.run(
        get_decode_dicts_from_axis_attributes(hydrograph_proj_cov_id)
    )
    ds_proj = fetch_hydro_data(
        hydrograph_proj_cov_id, stream_id, type="hydrograph_proj"
    )

    # Decode dimensions in the dataset to strings using the decode dictionaries
    for dim in decode_dict_hist.keys():
        decoded_vals = [decode_dict_hist[dim][float(v)] for v in ds_hist[dim].values]
        ds_hist[dim] = decoded_vals

    for dim in decode_dict_proj.keys():
        decoded_vals = [decode_dict_proj[dim][float(v)] for v in ds_proj[dim].values]
        ds_proj[dim] = decoded_vals

    # Print for debugging
    print(ds_hist)
    print(ds_proj)

    # Package the hydrograph data into a dictionary for JSON serialization
    data_dict = package_hydrograph_data(stream_id, ds_hist, ds_proj)

    # TODO: this pruning only works for lowest dict level - how to remove empty scenario/era combinations?
    # prune twice does not work to remove all the missing scenario / era combinations
    pruned_data_dict = prune_nulls_with_max_intensity(data_dict)
    pruned_data_dict = prune_nulls_with_max_intensity(pruned_data_dict)

    # Convert to JSON
    json_results = json.dumps(pruned_data_dict, indent=4)

    return Response(json_results, mimetype="application/json")
