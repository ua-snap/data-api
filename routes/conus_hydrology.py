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

seg_cov_id = "conus_hydro_segments"
hydrograph_hist_cov_id = "conus_hydro_segments_doy_mmm_maurer_historical_test_2"
hydrograph_proj_cov_id = "conus_hydro_segments_doy_mmm_maurer_projected_test"


async def get_decode_dicts_from_axis_attributes(cov_id):
    metadata = await describe_via_wcps(cov_id)
    return get_axis_encodings(metadata)


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


def package_stats_data(stream_id, ds):
    stats_dict = {
        stream_id: {"name": None, "latitude": None, "longitude": None, "data": {}}
    }

    for landcover in ds.landcover.values:
        if landcover not in stats_dict[stream_id]["data"]:
            stats_dict[stream_id]["data"][landcover] = {}
        for model in ds.model.values:
            if model not in stats_dict[stream_id]["data"][landcover]:
                stats_dict[stream_id]["data"][landcover][model] = {}
            for scenario in ds.scenario.values:
                if scenario not in stats_dict[stream_id]["data"][landcover][model]:
                    stats_dict[stream_id]["data"][landcover][model][scenario] = {}
                for era in ds.era.values:

                    var_dict = {}
                    for var in list(ds.data_vars):
                        stat_value = (
                            ds[var]
                            .sel(
                                landcover=landcover,
                                model=model,
                                scenario=scenario,
                                era=era,
                            )
                            .values
                        ).item()

                        if np.isnan(stat_value):
                            stat_value = None
                        else:
                            stat_value = round(stat_value, 2)

                        var_dict[var] = {
                            "value": stat_value,
                            "units": ds[var].attrs.get("units", ""),
                            # "description": ds[var].attrs.get("description", ""),
                        }

                    stats_dict[stream_id]["data"][landcover][model][scenario][
                        era
                    ] = var_dict

    return stats_dict


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
    # fetch data and metadata
    decode_dict = asyncio.run(get_decode_dicts_from_axis_attributes(seg_cov_id))
    ds = fetch_hydro_data(seg_cov_id, stream_id, type="stats")
    # decode the dimension values
    for dim in decode_dict.keys():
        decoded_vals = [decode_dict[dim][float(v)] for v in ds[dim].values]
        ds[dim] = decoded_vals
    # package the stats data into a dictionary for JSON serialization
    data_dict = package_stats_data(stream_id, ds)

    # TODO: populate attributes from vector data
    # data_dict = get_features_and_populate_attributes(data_dict)

    # TODO: prune nulls

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
    # fetch data and metadata
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

    # decode the dimension values
    for dim in decode_dict_hist.keys():
        decoded_vals = [decode_dict_hist[dim][float(v)] for v in ds_hist[dim].values]
        ds_hist[dim] = decoded_vals
    for dim in decode_dict_proj.keys():
        decoded_vals = [decode_dict_proj[dim][float(v)] for v in ds_proj[dim].values]
        ds_proj[dim] = decoded_vals

    # Package the hydrograph data into a dictionary for JSON serialization
    data_dict = package_hydrograph_data(stream_id, ds_hist, ds_proj)

    # TODO: populate attributes from vector data
    # data_dict = get_features_and_populate_attributes(data_dict)
    # TODO: prune nulls

    # Convert to JSON
    json_results = json.dumps(data_dict, indent=4)

    return Response(json_results, mimetype="application/json")
