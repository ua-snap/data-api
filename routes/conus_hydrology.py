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
from csv_functions import create_csv
from config import RAS_BASE_URL
from . import routes

coverages = {
    "stats": ["conus_hydro_segments_stats"],
    "doy_climatology": ["conus_hydro_segments_doy_climatology"],
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

    rows = (
        df.groupby("DOY")["discharge_cfs"]
        .agg(doy_min="min", doy_mean="mean", doy_max="max")
        .reset_index()
        .dropna(subset=["doy_mean"])
        .assign(
            doy=lambda x: x["DOY"].astype(int),
            doy_min=lambda x: x["doy_min"].astype(int),
            doy_mean=lambda x: x["doy_mean"].astype(int),
            doy_max=lambda x: x["doy_max"].astype(int),
        )
        .drop(columns="DOY")
        .to_dict("records")
    )

    # in each row, add water_year_index
    for row in rows:
        row["water_year_index"] = convert_doy_to_water_year_index(row["doy"])

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
    gauge_data_dict["metadata"]["variables"] = {
        "water_year_index": {
            "description": "Water year day index (1-366), where the water year starts on October 1 (DOY 275) and ends on September 30 (DOY 274).",
            "units": "dimensionless",
        },
        "doy_max": {
            "description": "Maximum streamflow value (cfs) on the specified day of year, aggregated over all years in the era.",
            "units": "cfs",
        },
        "doy_mean": {
            "description": "Mean streamflow value (cfs) on the specified day of year, aggregated over all years in the era.",
            "units": "cfs",
        },
        "doy_min": {
            "description": "Minimum streamflow value (cfs) on the specified day of year, aggregated over all years in the era.",
            "units": "cfs",
        },
    }
    gauge_data_dict["metadata"]["percent_complete"] = round(pct_complete, 2)

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

            # also add doy and water_year_index metadata:
            # these will be overwritten multiple times, but the values are the same for all three vars and we will only see them once in the final output
            data_dict["metadata"]["variables"]["doy"] = {}
            data_dict["metadata"]["variables"]["doy"]["units"] = "day of year"
            data_dict["metadata"]["variables"]["doy"][
                "description"
            ] = "Day of year (1-366); all years are treated as leap years for consistency."
            data_dict["metadata"]["variables"]["water_year_index"] = {}
            data_dict["metadata"]["variables"]["water_year_index"][
                "units"
            ] = "water year day index"
            data_dict["metadata"]["variables"]["water_year_index"][
                "description"
            ] = "Water year day index (1-366), where the water year starts on October 1 (DOY 275) and ends on September 30 (DOY 274)."

    return data_dict


def populate_feature_name_and_location_attributes(data_dict, gdf):
    """Function to populate the feature attributes in the data dictionary. Only the first feature is used.
    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
        gdf (geopandas GeoDataFrame): GeoDataFrame with the vector features
    Returns:
        Data dictionary with the vector attributes populated."""

    data_dict["name"] = gdf.loc[0].GNIS_NAME
    data_dict["huc8"] = gdf.loc[0].huc8

    huc8_flag = gdf.loc[0].h8_outlet
    if huc8_flag == 1:
        data_dict["huc8_outlet"] = True
    else:
        data_dict["huc8_outlet"] = False

    data_dict["latitude"] = round(gdf.loc[0].geometry.representative_point().y, 4)
    data_dict["longitude"] = round(gdf.loc[0].geometry.representative_point().x, 4)

    return data_dict


def populate_feature_stat_attributes_summary(data_dict, gdf):
    """Function to populate summaries of stats attributes in the data dictionary. Only the first feature is used.
    Args:
        data_dict (dict): Data dictionary with the hydrology data populated
    Returns:
        Data dictionary with the summaries populated."""

    # replace any None values in the gdf with NaN so that we don't get a rounding error
    gdf = gdf.replace({None: np.nan})

    summary_values = {}

    ### MEAN FLOWS:
    # mean annual flow stat variable deltas in geoserver layer: ma99_hist, ma99_min_d, ma99_avg_d, ma99_max_d,

    # historical mean annual flow (ma99_hist) rounded to nearest whole number
    # if greater than 5 cfs, round ma99_hist to nearest 5 cfs, else leave as is

    ma99_hist_value = (
        round(gdf.loc[0].ma99_hist, 0) if not np.isnan(gdf.loc[0].ma99_hist) else None
    )
    if ma99_hist_value is not None and ma99_hist_value > 5:
        ma99_hist_value = round(gdf.loc[0].ma99_hist / 5) * 5
    summary_values["ma99_hist"] = {
        "value": ma99_hist_value,
        "range_low": None,
        "range_high": None,
        "units": "cfs",
        "description": "historical mean annual flow",
    }

    # projected change in mean annual flow (ma99_min_d, ma99_avg_d, ma99_max_d)
    # round to nearest percent change and return as integer
    summary_values["ma99_delta"] = {
        "value": (
            int(round(gdf.loc[0].ma99_avg_d, 0))
            if not np.isnan(gdf.loc[0].ma99_avg_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].ma99_min_d, 0))
            if not np.isnan(gdf.loc[0].ma99_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].ma99_max_d, 0))
            if not np.isnan(gdf.loc[0].ma99_max_d)
            else None
        ),
        "units": "percent",
        "description": "projected change in mean annual flow",
    }

    ### MIN AND MAX FLOWS:
    # min and max 1-day flow stat variable deltas in geoserver layer: dh1_min_d, dh1_max_d, dl1_min_d, dl1_max_d

    # projected change in maximum 1-day flow
    # here the value is max of model maximums, so value = range_high; range_low is minimum of model maximums
    # round to nearest percent change and return as integer
    summary_values["dh1_delta"] = {
        "value": (
            int(round(gdf.loc[0].dh1_max_d, 0))
            if not np.isnan(gdf.loc[0].dh1_max_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].dh1_min_d, 0))
            if not np.isnan(gdf.loc[0].dh1_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].dh1_max_d, 0))
            if not np.isnan(gdf.loc[0].dh1_max_d)
            else None
        ),
        "units": "percent",
        "description": "projected change in maximum 1-day flow",
    }

    # projected minimum 1-day flow delta
    # here the value min of model minimums, so value = range_low; range_high is maximum of model minimums
    # round to nearest percent change and return as integer
    summary_values["dl1_delta"] = {
        "value": (
            int(round(gdf.loc[0].dl1_min_d, 0))
            if not np.isnan(gdf.loc[0].dl1_min_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].dl1_min_d, 0))
            if not np.isnan(gdf.loc[0].dl1_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].dl1_max_d, 0))
            if not np.isnan(gdf.loc[0].dl1_max_d)
            else None
        ),
        "units": "percent",
        "description": "projected change in minimum 1-day flow",
    }

    ### FLOOD DURATION:
    # high/low flood duration stat variable deltas in geoserver layer: dh15_min_d, dh15_avg_d, dh15_max_d, dl16_min_d, dl16_avg_d, dl16_max_d

    # projected change in high flow pulse durations (dh15)
    # round to whole day number and return as integer
    summary_values["dh15_delta"] = {
        "value": (
            int(round(gdf.loc[0].dh15_avg_d, 0))
            if not np.isnan(gdf.loc[0].dh15_avg_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].dh15_min_d, 0))
            if not np.isnan(gdf.loc[0].dh15_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].dh15_max_d, 0))
            if not np.isnan(gdf.loc[0].dh15_max_d)
            else None
        ),
        "units": "days",
        "description": "projected change in high flow pulse duration",
    }

    # projected change in low flow pulse durations (dl16)
    # round to whole day number and return as integer
    summary_values["dl16_delta"] = {
        "value": (
            int(round(gdf.loc[0].dl16_avg_d, 0))
            if not np.isnan(gdf.loc[0].dl16_avg_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].dl16_min_d, 0))
            if not np.isnan(gdf.loc[0].dl16_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].dl16_max_d, 0))
            if not np.isnan(gdf.loc[0].dl16_max_d)
            else None
        ),
        "units": "days",
        "description": "projected change in low flow pulse duration",
    }

    ### FLOOD PULSE COUNT:
    # high/low flood pulse count stat variable deltas in geoserver layer: fh1_min_d, fh1_avg_d, fh1_max_d, fl1_min_d, fl1_avg_d, fl1_max_d
    # projected change in high flood pulse count (fh1)
    # round to whole event number and return as integer
    summary_values["fh1_delta"] = {
        "value": (
            int(round(gdf.loc[0].fh1_avg_d, 0))
            if not np.isnan(gdf.loc[0].fh1_avg_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].fh1_min_d, 0))
            if not np.isnan(gdf.loc[0].fh1_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].fh1_max_d, 0))
            if not np.isnan(gdf.loc[0].fh1_max_d)
            else None
        ),
        "units": "events",
        "description": "projected change in high flood pulse count",
    }

    # projected change in low flood pulse count (fl1)
    # round to whole event number and return as integer
    summary_values["fl1_delta"] = {
        "value": (
            int(round(gdf.loc[0].fl1_avg_d, 0))
            if not np.isnan(gdf.loc[0].fl1_avg_d)
            else None
        ),
        "range_low": (
            int(round(gdf.loc[0].fl1_min_d, 0))
            if not np.isnan(gdf.loc[0].fl1_min_d)
            else None
        ),
        "range_high": (
            int(round(gdf.loc[0].fl1_max_d, 0))
            if not np.isnan(gdf.loc[0].fl1_max_d)
            else None
        ),
        "units": "events",
        "description": "projected change in low flood pulse count",
    }

    data_dict["summary"] = summary_values

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
    if not stream_id.isdigit():
        return render_template("400/bad_request.html"), 400

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
        data_dict = populate_feature_name_and_location_attributes(data_dict, gdf)

        data_dict = prune_nulls_with_max_intensity(data_dict)

        if request.args.get("format") == "csv":
            try:
                return create_csv(
                    data=data_dict,
                    endpoint="conus_hydrology",
                    filename_prefix="Hydrologic Statistics",
                    place_id=stream_id,
                    lat=str(data_dict["latitude"]),
                    lon=str(data_dict["longitude"]),
                )
            except Exception as exc:
                return render_template("500/server_error.html"), 500

        # add stats for data sentences to metadata: this is not included in the CSV output, but should be in JSON response
        data_dict = populate_feature_stat_attributes_summary(data_dict, gdf)

        return jsonify(data_dict)

    except Exception as exc:

        print(exc)

        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/conus_hydrology/modeled_climatology/<stream_id>")
def run_get_conus_hydrology_modeled_climatology(stream_id):
    """
    Function to fetch hydrograph data from Rasdaman for a single stream ID.
    Example URL: http://localhost:5000/conus_hydrology/modeled_climatology/1000
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with modeled daily climatology data for the requested stream ID.
    """
    if not stream_id.isdigit():
        return render_template("400/bad_request.html"), 400

    gdf = asyncio.run(get_features(stream_id))
    if isinstance(gdf, tuple):
        return gdf  # return 400 if gdf is a tuple

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
        data_dict = populate_feature_name_and_location_attributes(data_dict, gdf)
        data_dict = prune_nulls_with_max_intensity(data_dict)

        if request.args.get("format") == "csv":
            try:
                return create_csv(
                    data=data_dict,
                    endpoint="conus_hydrology",
                    filename_prefix="Modeled Daily Climatologies",
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


@routes.route("/conus_hydrology/observed_climatology/<stream_id>")
def run_get_conus_hydrology_gauge_data(stream_id):
    """
    Function to fetch USGS stream gauge data associated with a single stream ID.
    Example URL: http://localhost:5000/conus_hydrology/observed_climatology/50563
    (should fetch associated gauge: USGS-12039500, QUINAULT RIVER AT QUINAULT LAKE, WA)
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with USGS stream gauge data associated with the requested stream ID.
        Results are an observed daily climatology for the period 1976-2005, packaged identically to
        the modeled daily climatology data.
        If no gauge is associated with the stream ID, a 404 response is returned.
    """
    if not stream_id.isdigit():
        return render_template("400/bad_request.html"), 400

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

        if request.args.get("format") == "csv":
            try:
                return create_csv(
                    data=gauge_data_dict,
                    endpoint="conus_hydrology",
                    filename_prefix="Observed Daily Climatology",
                    place_id=stream_id + " (" + gauge_id + ")",
                    lat=str(gauge_data_dict["latitude"]),
                    lon=str(gauge_data_dict["longitude"]),
                    source_metadata={
                        "percent_complete": gauge_data_dict["metadata"][
                            "percent_complete"
                        ]
                    },
                )

            except Exception as exc:
                return render_template("500/server_error.html"), 500

        return jsonify(gauge_data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/conus_hydrology/gauge_info")
def run_get_conus_hydrology_gauge_info():
    """
    Function to fetch all stream segment attributes and create a reference to associate USGS stream gauges.
    Example URL: http://localhost:5000/conus_hydrology/gauge_info
    Args:
        None
    Returns:
        JSON response that lists all stream IDs with associated gauges. Each stream ID key
        maps to a list of dictionaries with the gauge name and USGS gauge ID. A small number of
        stream IDs are associated with more than one gauge. If a stream has no associated gauges,
        it is not included in the response.
    """
    try:
        gdf = asyncio.run(get_features(""))  # omit ID to fetch all stream attributes
        if isinstance(gdf, tuple):
            return gdf  # return 400 if gdf is a tuple
        gauges_gdf = gdf[gdf["GAUGE_ID"].notnull() & (gdf["GAUGE_ID"] != "NA")]
        result = (
            gauges_gdf[["seg_id_nat", "GNIS_NAME", "GAUGE_ID"]]
            .rename(columns={"GNIS_NAME": "name", "GAUGE_ID": "usgs_gauge_id"})
            .groupby("seg_id_nat")
            .apply(
                lambda x: x.drop(columns="seg_id_nat").to_dict(orient="records")
            )  # allows for >1 gauge per stream, e.g. stream ID 29526
            .to_dict()
        )
        return jsonify(result)
    except Exception as exc:
        return render_template("500/server_error.html"), 500
