import asyncio
import io
import numpy as np
import xarray as xr
import ast
import pandas as pd
from datetime import datetime
import geopandas as gpd
import copy
from aiohttp import ClientSession
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)

from generate_requests import generate_conus_hydrology_wcs_str
from fetch_data import fetch_data, fetch_layer_data, describe_via_wcps
from validate_request import get_axis_encodings
from postprocessing import prune_nulls_with_max_intensity
from csv_functions import create_csv
from config import RAS_BASE_URL
from . import routes


coverages = {
    "stats": ["ak_hydro_segments_stats"],
    "doy_climatology": ["ak_hydro_segments_doy_climatology"],
}


async def get_decode_dicts_from_axis_attributes(cov_ids):
    """
    Function to get the decode dictionaries for all axes from the coverage metadata.
    Args:
        cov_ids (list): coverage IDs to get decode dictionaries for
    Returns:
        list of with an axis decode dictionary for each coverage."""

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
    urls = [
        RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, stream_id)
        for cov_id in cov_ids
    ]

    results = await fetch_data(urls)

    # allow for single coverage ID input by wrapping in a list
    if not isinstance(results, list):
        results = [results]

    datasets = [xr.open_dataset(io.BytesIO(result)) for result in results]

    return datasets


# TODO: Implement arctic hydrology feature fetching when WFS layer is available on GeoServer
# async def get_features(stream_id):
#     """Function to fetch the vector features from the WFS for a given stream ID.
#     Creates a valid geodataframe from the features.
#     Args:
#         stream_id (str): Stream ID for the hydrology data
#     Returns:
#         geopandas GeoDataFrame with the vector features, or 400."""
#     try:
#         url = generate_wfs_conus_hydrology_url(stream_id)

#         async with ClientSession() as session:
#             layer_data = await fetch_layer_data(url, session)
#         gdf = gpd.GeoDataFrame.from_features(
#             layer_data["features"], crs="EPSG:5070"
#         ).to_crs(epsg=4326)
#         gdf["geometry"] = gdf["geometry"].make_valid()

#         return gdf
#     except:
#         return render_template("400/bad_request.html"), 400


def package_stats_data(stream_id, ds):
    """
    Function to package the stats data into a dictionary for JSON serialization.
    The levels of the stats data dictionary are as follows: model, era, variable.
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
    arr = ds[vars_].to_array().values  # (variable, model, era)

    models = ds.model.values
    eras = ds.era.values

    for i_m, model in enumerate(models):
        model_dict = stats_dict["data"].setdefault(model, {})

        # slice once per era block
        block = arr[:, i_m, :]  # (variable, era)

        for i_e, era in enumerate(eras):
            vals = block[:, i_e]

            # skip if all NaN
            if np.isnan(vals).all():
                continue

            model_dict[era] = {
                vars_[i]: round(float(v), 2)
                for i, v in enumerate(vals)
                if not np.isnan(v)
            }

    return stats_dict


def package_hydrograph_data(stream_id, datasets):
    """
    Function to package the hydrograph data into a dictionary for JSON serialization.
    The levels of the hydrograph data dictionary are as follows: model, era, doy, variable.
    Streamflow values (cfs) are rounded to 3 decimal places.
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
            .to_array()  # (variable, model, era, doy)
            .transpose("model", "era", "doy", "variable")
            .values
        )

        doy_vals = ds.doy.values.astype(int)
        models = ds.model.values
        eras = ds.era.values

        for i_m, model in enumerate(models):
            model_dict = hydrograph_dict["data"].setdefault(model, {})

            for i_e, era in enumerate(eras):
                block = arr[i_m, i_e]  # (doy, variable)

                # skip empty combos
                if np.isnan(block).all():
                    continue

                rows = []
                for i_doy, row in enumerate(block):
                    if np.isnan(row).all():
                        continue

                    entry = {
                        "doy": int(doy_vals[i_doy]),
                        "water_year_index": convert_doy_to_water_year_index(
                            int(doy_vals[i_doy])
                        ),
                    }
                    for i_v, val in enumerate(row):
                        if not np.isnan(val):
                            entry[vars_[i_v]] = round(float(val), 3)
                    rows.append(entry)

                model_dict[era] = rows

    return hydrograph_dict


def convert_doy_to_water_year_index(doy):
    """
    Function that takes a given day of year (1-366) and converts it to a water year index (1-366).
    The water year is defined as starting on October 1 (DOY 275 in a 366 day year) and ending
    September 30 (DOY 274 in a 366 day year). Note that this function only works for a 366 day year.
    Args:
        doy (int): Day of year (1-366)
    Returns:
        int: Water year index (1-366)"""
    if doy >= 275:
        wy_index = doy - 274
    else:
        wy_index = doy + 92
    return wy_index


def calculate_and_populate_annual_mean_flow(data_dict):
    """
    Function to calculate and populate the annual mean flow (ma99) in the stats data dictionary.
    Args:
        data_dict (dict): Data dictionary with the stats data populated
    Returns:
        Data dictionary with the annual mean flow populated.
    """
    monthly_stats_codes = [
        "ma12",
        "ma13",
        "ma14",
        "ma15",
        "ma16",
        "ma17",
        "ma18",
        "ma19",
        "ma20",
        "ma21",
        "ma22",
        "ma23",
    ]
    for model_, model_dict in data_dict["data"].items():
        for era_, era_dict in model_dict.items():
            # check if all monthly stats are present
            if all(code in era_dict for code in monthly_stats_codes):
                # calculate annual mean flow
                monthly_values = [era_dict[code] for code in monthly_stats_codes]
                annual_mean_flow = sum(monthly_values) / len(monthly_values)
                # populate the annual mean flow in the stats dictionary
                era_dict["ma99"] = round(annual_mean_flow, 2)

    return data_dict


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

        # special cases:

        # add derived "ma99" annual mean flow stat
        # if "ma12" is present (indicating we are dealing with a stats dataset)
        if var == "ma12":
            data_dict["metadata"]["variables"]["ma99"] = {}
            data_dict["metadata"]["variables"]["ma99"]["units"] = "cfs"
            data_dict["metadata"]["variables"]["ma99"][
                "description"
            ] = "Annual mean streamflow (cfs), calculated as the mean of the monthly mean flows."

        # "doy" vars from hydrograph datasets
        if var == "doy":
            data_dict["metadata"]["variables"][var]["units"] = "day of year"
            data_dict["metadata"]["variables"][var][
                "description"
            ] = "Day of year (1-366); all years are treated as leap years for consistency."
        elif var == "water_year_index":
            data_dict["metadata"]["variables"][var]["units"] = "water year day index"
            data_dict["metadata"]["variables"][var][
                "description"
            ] = "Water year day index (1-366), where the water year starts on October 1 (DOY 275) and ends on September 30 (DOY 274)."
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


# TODO: Implement arctic hydrology feature attribute population when WFS layer is available on GeoServer
def populate_feature_attributes(data_dict, gdf):
    """Function to populate the feature attributes in the data dictionary. Only the first feature is used.
    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
        gdf (geopandas GeoDataFrame): GeoDataFrame with the vector features
    Returns:
        Data dictionary with the vector attributes populated."""

    data_dict["name"] = ""
    data_dict["latitude"] = np.nan
    data_dict["longitude"] = np.nan

    if gdf is not None:
        data_dict["name"] = gdf.loc[0].GNIS_NAME
        data_dict["latitude"] = round(gdf.loc[0].geometry.representative_point().y, 4)
        data_dict["longitude"] = round(gdf.loc[0].geometry.representative_point().x, 4)

    return data_dict


@routes.route("/arctic_hydrology/")
def arctic_hydrology_about():
    return render_template("/documentation/arctic_hydrology.html")


@routes.route("/arctic_hydrology/stats/<stream_id>")
def run_get_arctic_hydrology_stats_data(stream_id):
    """
    Function to fetch hydrology data from Rasdaman for a single stream ID.
    Example URL: http://localhost:5000/arctic_hydrology/stats/81000004
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with hydrological stats for the requested stream ID.
    """
    if not stream_id.isdigit():
        return render_template("400/bad_request.html"), 400

    # TODO: Implement arctic hydrology feature validation when WFS layer is available on GeoServer
    # gdf = asyncio.run(get_features(stream_id))
    # if isinstance(gdf, tuple):
    #     return gdf  # return 400 if gdf is a tuple
    gdf = None

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
        try:
            data_dict = package_stats_data(stream_id, ds)
        except Exception as exc:
            print(exc)
            return render_template("500/server_error.html"), 500

        data_dict = calculate_and_populate_annual_mean_flow(data_dict)
        data_dict = package_metadata(ds, data_dict)
        data_dict = populate_feature_attributes(data_dict, gdf=None)
        data_dict = prune_nulls_with_max_intensity(data_dict)

        if request.args.get("format") == "csv":
            try:
                return create_csv(
                    data=data_dict,
                    endpoint="arctic_hydrology",
                    filename_prefix="Hydrologic Statistics",
                    place_id=stream_id,
                    lat=str(data_dict["latitude"]),
                    lon=str(data_dict["longitude"]),
                )
            except Exception as exc:
                return render_template("500/server_error.html"), 500

        return jsonify(data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/arctic_hydrology/modeled_climatology/<stream_id>")
def run_get_arctic_hydrology_modeled_climatology(stream_id):
    """
    Function to fetch hydrograph data from Rasdaman for a single stream ID.
    Example URL: http://localhost:5000/arctic_hydrology/modeled_climatology/81000004
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with modeled daily climatology data for the requested stream ID.
    """
    if not stream_id.isdigit():
        return render_template("400/bad_request.html"), 400

    # TODO: Implement arctic hydrology feature validation when WFS layer is available on GeoServer
    # gdf = asyncio.run(get_features(stream_id))
    # if isinstance(gdf, tuple):
    #     return gdf  # return 400 if gdf is a tuple
    gdf = None

    try:
        # fetch data and metadata
        datasets = asyncio.run(
            fetch_hydro_data(coverages["doy_climatology"], stream_id)
        )
        decode_dicts = asyncio.run(
            get_decode_dicts_from_axis_attributes(coverages["doy_climatology"])
        )

        # decode the dimension values
        decoded_datasets = []
        for ds, decode_dict in zip(datasets, decode_dicts):
            for dim, mapping in decode_dict.items():
                ds = ds.assign_coords({dim: [mapping[int(v)] for v in ds[dim].values]})
            decoded_datasets.append(ds)
        datasets = decoded_datasets

        # package the hydrograph datasets into a dictionary for JSON serialization
        data_dict = package_hydrograph_data(stream_id, datasets)
        data_dict = package_metadata(
            datasets[0], data_dict
        )  # all datasets should have same metadata, just use the first one
        data_dict = populate_feature_attributes(data_dict, gdf=None)
        data_dict = prune_nulls_with_max_intensity(data_dict)

        if request.args.get("format") == "csv":
            try:
                return create_csv(
                    data=data_dict,
                    endpoint="arctic_hydrology",
                    filename_prefix="Modeled Daily Climatologies",
                    place_id=stream_id,
                    lat=str(data_dict["latitude"]),
                    lon=str(data_dict["longitude"]),
                )
            except Exception as exc:
                print(exc)
                return render_template("500/server_error.html"), 500

        return jsonify(data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
