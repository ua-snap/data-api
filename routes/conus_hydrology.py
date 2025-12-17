import asyncio
import io
import numpy as np
import xarray as xr
import json
import ast
import geopandas as gpd
from aiohttp import ClientSession
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
from fetch_data import fetch_data, fetch_layer_data, describe_via_wcps
from validate_request import get_axis_encodings
from postprocessing import prune_nulls_with_max_intensity
from config import RAS_BASE_URL
from . import routes

# TODO: Add validation of stream ID
# TODO: Improve error handling throughout

seg_cov_id = "conus_hydro_segments"
hydrograph_hist_cov_id = "conus_hydro_segments_doy_mmm_maurer_historical_test_2"
hydrograph_proj_cov_id = "conus_hydro_segments_doy_mmm_maurer_projected_test"


async def get_decode_dicts_from_axis_attributes(cov_id):
    """
    Function to get the decode dictionaries for all axes from the coverage metadata.
    Args:
        cov_id (str): Coverage ID for the hydrology data
    Returns:
        Dictionary of decode dictionaries for each axis."""
    metadata = await describe_via_wcps(cov_id)
    return get_axis_encodings(metadata)


def fetch_hydro_data(cov_id, stream_id, type):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one stream ID at a time!
    Args:
        coverage_id (str): Coverage ID for the hydrology data
        stream_id (str): Stream ID for the hydrology data

    Returns:
        Xarray dataset with hydrological stats for the requested stream ID.
    """
    url = RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, stream_id, type)
    response = asyncio.run(fetch_data([url]))
    ds = xr.open_dataset(io.BytesIO(response))

    return ds


def package_stats_data(stream_id, ds):
    """
    Function to package the stats data into a dictionary for JSON serialization.
    The levels of the stats data dictionary are as follows: landcover, model, scenario, era, variable.
    Args:
        stream_id (str): Stream ID for the hydrology data
        ds (xarray dataset): Dataset with hydrology data
    Returns:
        Data dictionary with the stats data packaged for JSON serialization."""
    stats_dict = {
        "id": stream_id,
        "name": None,
        "latitude": None,
        "longitude": None,
        "metadata": {},
        "data": {},
    }

    for landcover in ds.landcover.values:
        stats_dict["data"][landcover] = {}
        for model in ds.model.values:
            stats_dict["data"][landcover][model] = {}
            for scenario in ds.scenario.values:
                stats_dict["data"][landcover][model][scenario] = {}
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
                        var_dict[var] = stat_value

                    stats_dict["data"][landcover][model][scenario][era] = var_dict

    return stats_dict


def package_metadata(ds, data_dict):
    """
    Function to package the metadata from the dataset into the data dictionary.
    Args:
        ds (xarray dataset): Dataset with hydrology data
        data_dict (dict): Data dictionary to populate with metadata.
    Returns:
        Data dictionary with the metadata populated."""
    try:
        ds_source_str = ds.attrs["Data_Source"]
        ds_source_dict = ast.literal_eval(ds_source_str)
        citation = ds_source_dict.get("Citation", "")
        data_dict["metadata"]["source"] = {"citation": citation}
    except Exception as e:
        data_dict["metadata"]["source"] = {"citation": ""}
        # for debugging ... can remove and just have empty citation
        print(e)

    data_dict["metadata"]["variables"] = {}
    for var in list(ds.data_vars):
        data_dict["metadata"]["variables"][var] = {}
        data_dict["metadata"]["variables"][var]["units"] = ds[var].attrs.get(
            "units", ""
        )
        data_dict["metadata"]["variables"][var]["description"] = ds[var].attrs.get(
            "description", ""
        )
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
        "id": stream_id,
        "name": None,
        "latitude": None,
        "longitude": None,
        "metadata": {},
        "data": {},
    }

    for ds in [ds_hist, ds_proj]:
        for landcover in ds.landcover.values:
            if landcover not in hydrograph_dict["data"]:
                hydrograph_dict["data"][landcover] = {}
            for model in ds.model.values:
                if model not in hydrograph_dict["data"][landcover]:
                    hydrograph_dict["data"][landcover][model] = {}
                for scenario in ds.scenario.values:
                    if scenario not in hydrograph_dict["data"][landcover][model]:
                        hydrograph_dict["data"][landcover][model][scenario] = {}
                    for era in ds.era.values:
                        if (
                            era
                            not in hydrograph_dict["data"][landcover][model][scenario]
                        ):
                            hydrograph_dict["data"][landcover][model][scenario][
                                era
                            ] = {}
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
                            hydrograph_dict["data"][landcover][model][scenario][era][
                                int(doy)
                            ] = var_dict

    return hydrograph_dict


async def get_features_and_populate_attributes(data_dict, stream_id):
    """Function to populate the data dictionary with the attributes from the vector data.
    Creates a valid geodataframe from the features and finds a representation point on the line segment.
    Populates the name, latitude, and longitude attributes in the data dictionary.

    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
        stream_id (str): Stream ID for the hydrology data
    Returns:
        Data dictionary with the vector attributes populated."""
    url = generate_wfs_conus_hydrology_url(stream_id)

    async with ClientSession() as session:
        layer_data = await fetch_layer_data(url, session)

    gdf = gpd.GeoDataFrame.from_features(
        layer_data["features"], crs="EPSG:5070"
    ).to_crs(epsg=4326)
    gdf["geometry"] = gdf["geometry"].make_valid()

    print(gdf)

    data_dict["name"] = gdf.loc[0].GNIS_NAME
    data_dict["latitude"] = round(gdf.loc[0].geometry.representative_point().x, 4)
    data_dict["longitude"] = round(gdf.loc[0].geometry.representative_point().y, 4)

    return data_dict


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
    # package the stats data + metadata into a dictionary for JSON serialization
    data_dict = package_stats_data(stream_id, ds)
    data_dict = package_metadata(ds, data_dict)
    data_dict = asyncio.run(get_features_and_populate_attributes(data_dict, stream_id))

    # TODO: prune nulls

    return Response(json.dumps(data_dict, indent=4), mimetype="application/json")


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

    # Package the hydrograph data + metadata into a dictionary for JSON serialization
    data_dict = package_hydrograph_data(stream_id, ds_hist, ds_proj)
    data_dict = package_metadata(
        ds_hist, data_dict
    )  # historical dataset should all required metadata
    data_dict = asyncio.run(get_features_and_populate_attributes(data_dict, stream_id))

    # TODO: prune nulls

    return Response(json.dumps(data_dict, indent=4), mimetype="application/json")
