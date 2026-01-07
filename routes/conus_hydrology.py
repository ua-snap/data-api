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
from generate_urls import (
    generate_wfs_conus_hydrology_url,
    generate_usgs_gauge_daily_streamflow_data_url,
    generate_usgs_gauge_metadata_url,
)
from fetch_data import fetch_data, fetch_layer_data, describe_via_wcps
from validate_request import get_axis_encodings
from postprocessing import prune_nulls_with_max_intensity
from config import RAS_BASE_URL
from . import routes

coverages = {
    "stats": ["conus_hydro_segments_stats"],
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


async def get_usgs_gauge_data(gauge_id):

    gauge_data_dict = {
        "id": gauge_id,
        "name": None,
        "latitude": None,
        "longitude": None,
        "metadata": {},
        "data": {},
    }

    start_date = "1976-10-01"
    end_date = "2005-09-30"

    try:
        metadata_url = generate_usgs_gauge_metadata_url(gauge_id)
        data_url = generate_usgs_gauge_daily_streamflow_data_url(
            gauge_id, start_date, end_date
        )
        async with ClientSession() as session:
            gauge_metadata = await fetch_layer_data(metadata_url, session)
            gauge_data = await fetch_layer_data(data_url, session)
    except:
        return render_template("400/bad_request.html"), 400

    # get metadata from JSON and populate dict

    metadata_features = gauge_metadata.get("features")
    if not metadata_features:
        return render_template("400/bad_request.html"), 400

    metadata_feature = metadata_features[0]
    gauge_data_dict["name"] = metadata_feature["properties"]["monitoring_location_name"]
    coordinates = metadata_feature["geometry"]["coordinates"]
    gauge_data_dict["longitude"] = round(float(coordinates[0]), 4)
    gauge_data_dict["latitude"] = round(float(coordinates[1]), 4)

    # get streamflow data from JSON into dataframe
    date_range = pd.date_range(start_date, end=end_date, freq="D")
    df = pd.DataFrame(date_range, columns=["date"])
    df.set_index("date", inplace=True)
    df["discharge_cfs"] = float("nan")

    data_features = gauge_data.get("features")
    if not data_features:
        return render_template("400/bad_request.html"), 400

    records = []
    for feature in data_features:
        date_str = feature["properties"]["time"][:10]
        value = feature["properties"]["value"]
        records.append((date_str, float(value)))

    if records:
        values_df = pd.DataFrame(records, columns=["date", "discharge_cfs"])
        values_df["date"] = pd.to_datetime(values_df["date"])
        values_df.set_index("date", inplace=True)
        # align on index to fill discharge_cfs for matching dates
        df["discharge_cfs"] = values_df["discharge_cfs"]

    df["DOY"] = df.index.dayofyear

    # calculate percent completeness
    total_days = len(df)
    valid_days = df["discharge_cfs"].count()
    pct_complete = (valid_days / total_days) * 100

    # calculate daily climatology
    df_doy = df.groupby("DOY").mean()
    rows = []
    for doy, row in df_doy.iterrows():
        if np.isnan(row["discharge_cfs"]):
            continue
        entry = {
            "doy": int(doy),
            "discharge": round(float(row["discharge_cfs"]), 2),
        }
        rows.append(entry)

    gauge_data_dict["data"]["actual"] = {}
    gauge_data_dict["data"]["actual"]["usgs"] = {}
    gauge_data_dict["data"]["actual"]["usgs"]["observed"] = {}
    gauge_data_dict["data"]["actual"]["usgs"]["observed"]["1976-2005"] = rows

    # populate metadata
    current_year = datetime.now().year
    current_date = datetime.now().strftime("%Y-%m-%d")

    gauge_data_dict["metadata"]["source"] = {}
    gauge_data_dict["metadata"]["source"][
        "citation"
    ] = f"U.S. Geological Survey, {current_year}, U.S. Geological Survey National Water Information System database, accessed {current_date}, at https://doi.org/10.5066/F7P55KJN. Data download directly accessible at {data_url}"
    gauge_data_dict["metadata"]["variables"] = {}
    gauge_data_dict["metadata"]["variables"]["daily_discharge"] = {}
    gauge_data_dict["metadata"]["variables"]["daily_discharge"]["units"] = "cfs"
    gauge_data_dict["metadata"]["variables"]["daily_discharge"][
        "description"
    ] = f"Daily mean streamflow (cfs), climatology for the period 1976-2005. Calculated as the mean streamflow for each day of year over all years in the period. Data completeness: {pct_complete:.2f}%."

    return gauge_data_dict


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

    return prune_missing_scenarios(stats_dict)


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
    return prune_missing_scenarios(hydrograph_dict)


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
    for landcover_, land_dict in data_dict["data"].items():
        for model_, model_dict in land_dict.items():
            for scenario_, scen_dict in model_dict.items():
                for era_, era_dict in scen_dict.items():
                    # check if all monthly stats are present
                    if all(code in era_dict for code in monthly_stats_codes):
                        # calculate annual mean flow
                        monthly_values = [
                            era_dict[code] for code in monthly_stats_codes
                        ]
                        annual_mean_flow = sum(monthly_values) / len(monthly_values)
                        # populate the annual mean flow in the stats dictionary
                        era_dict["ma99"] = round(annual_mean_flow, 2)

    return data_dict


def prune_missing_scenarios(data_dict):
    """
    Function to prune scenarios with no data from the data dictionary.
    Args:
        data_dict (dict): Data dictionary with the hydrology data populated
    Returns:
        Data dictionary with empty scenarios pruned.
    """
    copy_data_dict = copy.deepcopy(data_dict["data"])
    for landcover, land_dict in copy_data_dict.items():
        for model, model_dict in land_dict.items():
            for scenario, scen_dict in model_dict.items():
                if not scen_dict:
                    del data_dict["data"][landcover][model][scenario]
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
        data_dict = calculate_and_populate_annual_mean_flow(data_dict)
        data_dict = package_metadata(ds, data_dict)
        data_dict = populate_feature_attributes(data_dict, gdf)

        data_dict = prune_nulls_with_max_intensity(data_dict)

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
        data_dict = populate_feature_attributes(data_dict, gdf)
        data_dict = prune_nulls_with_max_intensity(data_dict)

        return jsonify(data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/conus_hydrology/gauge/<stream_id>")
def run_get_conus_hydrology_gauge_data(stream_id):
    """
    Function to fetch USGS stream gauge data associated with a single stream ID.
    Example URL: http://localhost:5000/conus_hydrology/gauge/50563
    (should fetch associated gauge: USGS-12039500, QUINAULT RIVER AT QUINAULT LAKE, WA)
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with USGS stream gauge data associated with the requested stream ID.
        Results are a daily climatology for the period 1976-2005, packaged identically to the hydrograph data.
        If no gauge is associated with the stream ID, a 404 response is returned.
    """
    gdf = asyncio.run(get_features(stream_id))
    if isinstance(gdf, tuple):
        return gdf  # return 400 if gdf is a tuple

    try:
        gauge_id = gdf.loc[0].GAUGE_ID
        if gauge_id is None or gauge_id == "NA":
            return render_template("404/no_data.html"), 404

        gauge_data_dict = asyncio.run(get_usgs_gauge_data(gauge_id))
        if isinstance(gauge_data_dict, tuple):
            return gauge_data_dict  # return 400 if gauge_data_dict is a tuple
        return jsonify(gauge_data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
