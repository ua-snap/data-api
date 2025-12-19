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
from config import RAS_BASE_URL
from . import routes

coverages = {
    "stats": ["conus_hydro_segments_test_exsitu_reg"],
    "hydrograph": [
        "conus_hydro_segments_doy_climatology_dynamic_historical",
        "conus_hydro_segments_doy_climatology_static_historical",
        "conus_hydro_segments_doy_climatology_dynamic_projected",
        "conus_hydro_segments_doy_climatology_static_projected",
    ],
}


async def get_decode_dicts_from_axis_attributes(cov_ids):
    """
    Function to get the decode dictionaries for all axes from the coverage metadata.
    Args:
        cov_ids (list): coverage IDs to get decode dictionaries for
    Returns:
        list of with an axis decode dictionary for each coverage."""

    async with ClientSession() as session:
        tasks = [describe_via_wcps(cov_id) for cov_id in cov_ids]
        metadata_list = await asyncio.gather(*tasks)
    decode_dicts = [get_axis_encodings(metadata) for metadata in metadata_list]

    return decode_dicts


async def fetch_hydro_data(cov_ids, stream_id):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one stream ID at a time!
    Args:
        cov_ids (list): list of coverage IDs for the hydrology data
        stream_id (str): Stream ID for the hydrology data

    Returns:
        results (list): list of responses from Rasdaman for each coverage ID
    """
    # TODO: consider using fetch_data.fetch_bbox_netcdf_list() function here?

    urls = [
        RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, stream_id)
        for cov_id in cov_ids
    ]

    # if coverages["stats"][0] in cov_ids and len(urls) == 1:
    #     # stats data request - need to handle rasql query bug!
    #     # TODO: investigate this rasda-bug further and see if there's a solution
    #     urls[0] += "&SUBSET=model(0,13)"

    results = await fetch_data(urls)

    # allow for single coverage ID input by wrapping in a list
    if not isinstance(results, list):
        results = [results]

    datasets = [xr.open_dataset(io.BytesIO(result)) for result in results]

    return datasets


async def get_features(stream_id):
    """Function to fetch the vector features from the WFS for a given stream ID.
    Creates a valid geodataframe from the features.
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        geopandas GeoDataFrame with the vector features, or 400."""
    try:
        url = generate_wfs_conus_hydrology_url(stream_id)

        async with ClientSession() as session:
            layer_data = await fetch_layer_data(url, session)
        gdf = gpd.GeoDataFrame.from_features(
            layer_data["features"], crs="EPSG:5070"
        ).to_crs(epsg=4326)
        gdf["geometry"] = gdf["geometry"].make_valid()

        return gdf
    except:
        return render_template("400/bad_request.html"), 400


def package_stats_data(stream_id, ds):
    """
    Function to package the stats data into a dictionary for JSON serialization.
    The levels of the stats data dictionary are as follows: landcover, model, scenario, era, variable.
    Stats values are rounded to 2 decimal places.
    Dataset is read into numpy arrays for faster slicing.
    Args:
        stream_id (str): Stream ID for the hydrology data
        ds (xarray dataset): Dataset with hydrology data
    Returns:
        Data dictionary with the stats data packaged for JSON serialization.
    """
    stats_dict = {
        "id": stream_id,
        "name": None,
        "latitude": None,
        "longitude": None,
        "metadata": {},
        "data": {},
    }

    vars_ = list(ds.data_vars)

    # stack everything except "variable" into one array
    arr = ds[vars_].to_array().values  # (variable, landcover, model, scenario, era)

    landcovers = ds.landcover.values
    models = ds.model.values
    scenarios = ds.scenario.values
    eras = ds.era.values

    for i_lc, landcover in enumerate(landcovers):
        land_dict = stats_dict["data"].setdefault(landcover, {})

        for i_m, model in enumerate(models):
            model_dict = land_dict.setdefault(model, {})

            for i_s, scenario in enumerate(scenarios):
                scen_dict = model_dict.setdefault(scenario, {})

                # slice once per era block
                block = arr[:, i_lc, i_m, i_s, :]  # (variable, era)

                for i_e, era in enumerate(eras):
                    vals = block[:, i_e]

                    # skip if all NaN
                    if np.isnan(vals).all():
                        continue

                    scen_dict[era] = {
                        vars_[i]: round(float(v), 2)
                        for i, v in enumerate(vals)
                        if not np.isnan(v)
                    }

    return stats_dict


def package_hydrograph_data(stream_id, datasets):
    """
    Function to package the hydrograph data into a dictionary for JSON serialization.
    The levels of the hydrograph data dictionary are as follows: landcover, model, scenario, era, variable.
    Streamflow values (cfs) are rounded to integers.
    Each dataset is read into numpy array for faster slicing.
    Args:
        stream_id (str): Stream ID for the hydrology data
        datasets (list of xarray datasets): List of datasets with hydrology data
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

    for ds in datasets:
        vars_ = list(ds.data_vars)

        # stack everything except "variable" into one array
        arr = (
            ds[vars_]
            .to_array()  # (variable, landcover, model, scenario, era, doy)
            .transpose("landcover", "model", "scenario", "era", "doy", "variable")
            .values
        )

        doy_vals = ds.doy.values.astype(int)

        landcovers = ds.landcover.values
        models = ds.model.values
        scenarios = ds.scenario.values
        eras = ds.era.values

        for i_lc, landcover in enumerate(landcovers):
            land_dict = hydrograph_dict["data"].setdefault(landcover, {})

            for i_m, model in enumerate(models):
                model_dict = land_dict.setdefault(model, {})

                for i_s, scenario in enumerate(scenarios):
                    scen_dict = model_dict.setdefault(scenario, {})

                    for i_e, era in enumerate(eras):
                        block = arr[i_lc, i_m, i_s, i_e]  # (doy, variable)

                        # skip empty combos
                        if np.isnan(block).all():
                            continue

                        rows = []
                        for i_doy, row in enumerate(block):
                            if np.isnan(row).all():
                                continue

                            entry = {"doy": int(doy_vals[i_doy])}
                            for i_v, val in enumerate(row):
                                if not np.isnan(val):
                                    entry[vars_[i_v]] = int(val)
                            rows.append(entry)

                        scen_dict[era] = rows

    return hydrograph_dict


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

    data_dict["metadata"]["variables"] = {}
    for var in list(ds.data_vars):
        data_dict["metadata"]["variables"][var] = {}
        data_dict["metadata"]["variables"][var]["units"] = ds[var].attrs.get(
            "units", ""
        )
        data_dict["metadata"]["variables"][var]["description"] = ds[var].attrs.get(
            "description", ""
        )

        # special cases for "doy" vars from hydrograph datasets
        # TODO: add this info to the rasdaman coverage metadata so it can be read automatically
        if var == "doy":
            data_dict["metadata"]["variables"][var]["units"] = "day of year"
            data_dict["metadata"]["variables"][var][
                "description"
            ] = "Day of year (1-366); all years are treated as leap years for consistency."
        elif var in ["doy_min", "doy_mean", "doy_max"]:
            data_dict["metadata"]["variables"][var]["units"] = "cfs"
            if var == "doy_min":
                op = "Minimum"
            elif var == "doy_mean":
                op = "Mean"
            else:
                op = "Maximum"
            data_dict["metadata"]["variables"][var][
                "description"
            ] = f"{op} streamflow value (cfs) on the specified day of year, aggregated over all years in the era."

    return data_dict


def populate_feature_attributes(data_dict, gdf):
    """Function to populate the feature attributes in the data dictionary. Only the first feature is used.
    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
        gdf (geopandas GeoDataFrame): GeoDataFrame with the vector features
    Returns:
        Data dictionary with the vector attributes populated."""

    data_dict["name"] = gdf.loc[0].GNIS_NAME
    data_dict["latitude"] = round(gdf.loc[0].geometry.representative_point().y, 4)
    data_dict["longitude"] = round(gdf.loc[0].geometry.representative_point().x, 4)

    return data_dict


@routes.route("/conus_hydrology/")
def conus_hydrology_about():
    return render_template("/documentation/conus_hydrology.html")


@routes.route("/conus_hydrology/stats/<stream_id>")
def run_get_conus_hydrology_stats_data(stream_id):
    """
    Function to fetch hydrology data from Rasdaman for a single stream ID.
    Example URL: http://localhost:5000/conus_hydrology/stats/1000
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with hydrological stats for the requested stream ID.
    """
    gdf = asyncio.run(get_features(stream_id))
    if isinstance(gdf, tuple):
        return gdf  # return 400 if gdf is a tuple

    try:
        # fetch data and metadata
        decode_dict = asyncio.run(
            get_decode_dicts_from_axis_attributes(coverages["stats"])
        )[0]
        ds = asyncio.run(fetch_hydro_data(coverages["stats"], stream_id))[0]

        # decode the dimension values
        for dim, mapping in decode_dict.items():
            ds = ds.assign_coords({dim: [mapping[int(v)] for v in ds[dim].values]})

        # package the stats data + metadata into a dictionary for JSON serialization
        data_dict = package_stats_data(stream_id, ds)
        data_dict = package_metadata(ds, data_dict)
        data_dict = populate_feature_attributes(data_dict, gdf)

        return jsonify(data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


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
    gdf = asyncio.run(get_features(stream_id))
    if isinstance(gdf, tuple):
        return gdf  # return 400 if gdf is a tuple

    try:
        # fetch data and metadata
        datasets = asyncio.run(fetch_hydro_data(coverages["hydrograph"], stream_id))
        decode_dicts = asyncio.run(
            get_decode_dicts_from_axis_attributes(coverages["hydrograph"])
        )

        # decode the dimension values
        for ds, decode_dict in zip(datasets, decode_dicts):
            for dim, mapping in decode_dict.items():
                ds = ds.assign_coords({dim: [mapping[int(v)] for v in ds[dim].values]})

        # package the hydrograph datasets into a dictionary for JSON serialization
        data_dict = package_hydrograph_data(stream_id, datasets)
        data_dict = package_metadata(
            datasets[0], data_dict
        )  # all datasets should have same metadata, just use the first one
        data_dict = populate_feature_attributes(data_dict, gdf)

        return jsonify(data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
