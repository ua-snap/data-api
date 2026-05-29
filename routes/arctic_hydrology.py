import asyncio
import copy
import io
import numpy as np
import xarray as xr
import ast
import geopandas as gpd
import statistics
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
from generate_urls import generate_wfs_arctic_hydrology_url, generate_wfs_arctic_hydrology_stats_url
from fetch_data import fetch_data, fetch_layer_data, describe_via_wcps
from validate_request import get_axis_encodings
from postprocessing import prune_nulls_with_max_intensity
from csv_functions import create_csv
from config import RAS_BASE_URL
from . import routes

coverages = {
    "stats": ["ak_hydro_segments_mhit_stats_combined"],
    "doy_climatology": ["ak_hydro_segments_doy_climatology"],
}

stat_source_encodings = {
    "original_gcm": 0,
    "gcm_diff": 1,
    "gcm_diff_applied_to_cheng": 2,
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


async def fetch_hydro_data(cov_ids, stream_id, source=None):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one stream ID at a time!
    Args:
        cov_ids (list): list of coverage IDs for the hydrology data
        stream_id (str): Stream ID for the hydrology data
        source (str, optional): Source ID (for stats coverage only)

    Returns:
        results (list): list of responses from Rasdaman for each coverage ID
    """
    if source is not None:
        urls = [
            RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, stream_id, source)
            for cov_id in cov_ids
        ]
    else:
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
        url = generate_wfs_arctic_hydrology_url(stream_id)

        async with ClientSession() as session:
            layer_data = await fetch_layer_data(url, session)
        gdf = gpd.GeoDataFrame.from_features(layer_data["features"], crs="EPSG:3338")
        gdf["geometry"] = gdf["geometry"].make_valid()

        return gdf
    except Exception:
        return render_template("400/bad_request.html"), 400


async def get_stats_features(stream_id):
    """Function to fetch summary stat attributes from the WFS stats layer for a given stream ID.
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        GeoDataFrame with stat attributes, or None if unavailable."""
    try:
        url = generate_wfs_arctic_hydrology_stats_url(stream_id)
        async with ClientSession() as session:
            layer_data = await fetch_layer_data(url, session)
        features = layer_data.get("features", [])
        if not features:
            return None
        gdf = gpd.GeoDataFrame([f["properties"] for f in features])
        return gdf.replace({None: np.nan})
    except Exception:
        return None


def populate_feature_stat_attributes_summary(data_dict, gdf):
    """Function to populate the summary stats from the WFS stats layer into the data dictionary.
    Args:
        data_dict (dict): Data dictionary to populate with summary stats
        gdf (GeoDataFrame or None): GeoDataFrame with stat attributes from the stats layer
    Returns:
        Data dictionary with summary stats populated, or null-valued summary if gdf is unavailable."""
    summary_values = {}

    ### MEAN FLOWS:
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
    # value is max of model maximums (range_high); range_low is minimum of model maximums
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

    # value is min of model minimums (range_low); range_high is maximum of model minimums
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
    summary_values["dh15_hist"] = {
        "value": (
            int(round(gdf.loc[0].dh15_hist, 0))
            if not np.isnan(gdf.loc[0].dh15_hist)
            else None
        ),
        "range_low": None,
        "range_high": None,
        "units": "days",
        "description": "historical high flow pulse duration",
    }

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

    summary_values["dl16_hist"] = {
        "value": (
            int(round(gdf.loc[0].dl16_hist, 0))
            if not np.isnan(gdf.loc[0].dl16_hist)
            else None
        ),
        "range_low": None,
        "range_high": None,
        "units": "days",
        "description": "historical low flow pulse duration",
    }

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
    summary_values["fh1_hist"] = {
        "value": (
            int(round(gdf.loc[0].fh1_hist, 0))
            if not np.isnan(gdf.loc[0].fh1_hist)
            else None
        ),
        "range_low": None,
        "range_high": None,
        "units": "events",
        "description": "historical high flood pulse count",
    }

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

    summary_values["fl1_hist"] = {
        "value": (
            int(round(gdf.loc[0].fl1_hist, 0))
            if not np.isnan(gdf.loc[0].fl1_hist)
            else None
        ),
        "range_low": None,
        "range_high": None,
        "units": "events",
        "description": "historical low flood pulse count",
    }

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

    models = ds.model.values
    eras = ds.era.values

    # stack variables and transpose to guaranteed (variable, model, era) order
    arr = ds[vars_].to_array().transpose("variable", "model", "era").values

    for i_m, model in enumerate(models):
        model_dict = stats_dict["data"].setdefault(model, {})

        # slice once per era block
        block = arr[:, i_m, :]  # (variable, era)

        for i_e, era in enumerate(eras):
            vals = block[:, i_e]

            # skip if all NaN or all zero (zero is sometimes Rasdaman's fill for missing data, and can be inconsistent in the same coverage)
            if np.isnan(vals).all() or (vals == 0).all():
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

                # skip empty combos (all NaN or all zero — zero is Rasdaman's fill for missing data)
                if np.isnan(block).all() or (block == 0).all():
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


def calculate_and_apply_gcm_diffs_to_cheng_climatology(data_dict):
    """
    Function to calculate GCM-projected changes in streamflow and apply them to the historical Cheng climatology.
    Models without a '1990-2021' era (e.g. PGW models with no historical baseline) are silently skipped.
    Args:
        data_dict (dict): Climatology data dict keyed by model then era
    Returns:
        dict: Adjusted data dict with Cheng baseline applied to all eligible models.
    """
    adjusted_data_dict = {}
    for model in data_dict.keys():
        if model == "historical":
            adjusted_data_dict[model] = data_dict[model]
            continue
        if "1990-2021" not in data_dict[model]:
            continue
        adjusted_data_dict[model] = {}
        for era in data_dict[model].keys():
            if era == "1990-2021":
                continue  # historical era is identical across all GCM models; only expose it under "historical"
            adjusted_data_dict[model][era] = []
            for i in range(len(data_dict[model][era])):
                entry = data_dict[model][era][i]
                doy_stats = {
                    "doy": entry["doy"],
                    "water_year_index": entry["water_year_index"],
                }
                for stat in entry.keys():
                    if stat in ("doy", "water_year_index"):
                        continue
                    cheng_historical = data_dict["historical"]["1990-2021"][i][stat]
                    gcm_historical = data_dict[model]["1990-2021"][i][stat]
                    gcm_projected = entry[stat]
                    denominator = gcm_historical
                    if denominator == 0:
                        denominator = 0.0001
                    projected_quotient = gcm_projected / denominator
                    cheng_adjusted = round(cheng_historical * projected_quotient, 3)
                    doy_stats[stat] = cheng_adjusted
                adjusted_data_dict[model][era].append(doy_stats)
    return adjusted_data_dict


def package_metadata(ds, data_dict, source=None):
    """
    Function to package the metadata from the dataset into the data dictionary.
    Args:
        ds (xarray dataset): Dataset with hydrology data
        data_dict (dict): Data dictionary to populate with metadata.
        source (str, optional): Source ID string
    Returns:
        Data dictionary with the metadata populated."""
    try:
        ds_source_str = ds.attrs["Data_Source"]
        ds_source_dict = ast.literal_eval(ds_source_str)
        citation = ds_source_dict.get("Citation", "")
        data_dict["metadata"]["source"] = {"citation": citation}
    except Exception:
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
        source_notes = {
            "original_gcm": "Values are derived from the original GCM or PGW runs.",
            "gcm_diff": "Values are the ratio or absolute difference between the original GCM runs and the historical GCM runs - these are not actual statistic values! Apply these differences to a historical baseline value to approximate future values. PGW runs are not included.",
            "gcm_diff_applied_to_cheng": "Values are derived from applying the GCM-projected changes to the historical Cheng baseline. PGW runs are not included.",
        }

        if source == "original_gcm":
            data_dict["metadata"]["variables"][var][
                "description"
            ] = f"{data_dict['metadata']['variables'][var]['description']} {source_notes['original_gcm']}"

        if source == "gcm_diff":
            data_dict["metadata"]["variables"][var][
                "description"
            ] = f"{data_dict['metadata']['variables'][var]['description']} {source_notes['gcm_diff']}"

        if source == "gcm_diff_applied_to_cheng":
            data_dict["metadata"]["variables"][var][
                "description"
            ] = f"{data_dict['metadata']['variables'][var]['description']} {source_notes['gcm_diff_applied_to_cheng']}"

        # "doy" vars from hydrograph datasets
        if var in ["doy_min", "doy_mean", "doy_max"]:
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
            # add source notes
            if source in source_notes:
                data_dict["metadata"]["variables"][var][
                    "description"
                ] += f" {source_notes[source]}"

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


def populate_feature_attributes(data_dict, gdf):
    """Function to populate the feature attributes in the data dictionary. Only the first feature is used.
    Args:
        data_dict (dict): Data dictionary with the hydrology stats populated
        gdf (geopandas GeoDataFrame): GeoDataFrame with the vector features
    Returns:
        Data dictionary with the vector attributes populated."""

    # data_dict["name"] = "" # arctic rivers segments do not have stream names associated

    # the watershed ID matches the GVV code for HUC8 in Alaska or Yukon watershed in Canada
    # all Yukon watersheds begin with "YTHYDRO" while HUC8s are just numeric
    data_dict["watershed"] = gdf.loc[0].get("ID_1", None)
    data_dict["watershed_outlet"] = gdf.loc[0].get("outlet", None)

    # copy and convert gdf to WGS84 for lat/lon extraction
    gdf_4326 = gdf.to_crs("EPSG:4326")
    data_dict["latitude"] = round(gdf_4326.loc[0].geometry.representative_point().y, 4)
    data_dict["longitude"] = round(gdf_4326.loc[0].geometry.representative_point().x, 4)

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
    # validate get request query parameters
    source = request.args.get("source", None)
    if source is None:
        source = "gcm_diff_applied_to_cheng"

    if not stream_id.isdigit():
        return render_template("400/bad_request.html"), 400

    gdf = asyncio.run(get_features(stream_id))
    if isinstance(gdf, tuple):
        return gdf  # return 400 if gdf is a tuple

    stats_gdf = asyncio.run(get_stats_features(stream_id))

    try:
        # fetch data and metadata
        decode_dict = asyncio.run(
            get_decode_dicts_from_axis_attributes(coverages["stats"])
        )[0]

        ds = asyncio.run(
            fetch_hydro_data(
                coverages["stats"], stream_id, source=stat_source_encodings[source]
            )
        )[0]

        # decode the dimension values, ignoring "source"
        for dim, mapping in decode_dict.items():
            if dim == "source":
                continue
            ds = ds.assign_coords({dim: [mapping[int(v)] for v in ds[dim].values]})

        # package the stats data + metadata into a dictionary for JSON serialization
        try:
            data_dict = package_stats_data(stream_id, ds)
        except Exception:
            return render_template("500/server_error.html"), 500

        data_dict = package_metadata(ds, data_dict, source=source)
        data_dict = populate_feature_attributes(data_dict, gdf)
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
                    source_metadata=source,
                )
            except Exception:
                return render_template("500/server_error.html"), 500

        data_dict = populate_feature_stat_attributes_summary(data_dict, stats_gdf)

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
    # validate get request query parameters
    source = request.args.get("source", None)
    if source is None:
        source = "gcm_diff_applied_to_cheng"
    elif source == "gcm_diff":
        return render_template("400/bad_request.html"), 400

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
            datasets[0], data_dict, source=source
        )  # all datasets should have same metadata, just use the first one
        data_dict = populate_feature_attributes(data_dict, gdf)
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
                    source_metadata=source,
                )
            except Exception:
                return render_template("500/server_error.html"), 500

        # apply GCM-projected changes to Cheng climatology if source is "gcm_diff_applied_to_cheng"
        # PGW models without a 1990-2021 historical era are silently absent from this source;
        # they are only available via source=original_gcm.
        if source == "gcm_diff_applied_to_cheng":
            data_dict["data"] = calculate_and_apply_gcm_diffs_to_cheng_climatology(
                data_dict["data"]
            )

        return jsonify(data_dict)

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/arctic_hydrology/hydroviz/<stream_id>")
def run_get_arctic_hydrology_hydroviz(stream_id):
    """
    Function to fetch all data for the hydroviz webapp for a given arctic stream ID.
    Args:
        stream_id (str): Stream ID for the hydrology data
    Returns:
        JSON response with the following top-level keys:
        {
            "gauge_id": null,
            "huc8": null,
            "huc8_outlet": null,
            "hydrograph": ...,
            "id": ...,
            "monthly_flow": ...,
            "max_flow_dates": ...,
            "name": ...,
            "stats": ...,
            "summary": ...
        }
        Unlike the CONUS hydroviz, projected data is not nested by scenario since the Arctic
        dataset has no scenario dimension. Projected structures are keyed directly by era.
    """
    chart_era = "2034-2065"
    historical_era = "1990-2021"

    # fetch stats with default source (gcm_diff_applied_to_cheng)
    stats_response = run_get_arctic_hydrology_stats_data(stream_id)
    if isinstance(stats_response, tuple):
        return stats_response

    try:
        stats = stats_response.get_json()

        # Fetch original_gcm stats to get PGW models (absent from gcm_diff_applied_to_cheng).
        # The describe call is a duplicate of what run_get_arctic_hydrology_stats_data already did,
        # but keeping the fetch inline here avoids a larger refactor of the route function.
        stats_decode_dict = asyncio.run(
            get_decode_dicts_from_axis_attributes(coverages["stats"])
        )[0]
        pgw_ds = asyncio.run(
            fetch_hydro_data(
                coverages["stats"],
                stream_id,
                source=stat_source_encodings["original_gcm"],
            )
        )[0]
        for dim, mapping in stats_decode_dict.items():
            if dim == "source":
                continue
            pgw_ds = pgw_ds.assign_coords(
                {dim: [mapping[int(v)] for v in pgw_ds[dim].values]}
            )
        pgw_stats = package_stats_data(stream_id, pgw_ds)
        for model, model_data in pgw_stats["data"].items():
            if chart_era not in stats["data"].get(model, {}):
                stats["data"][model] = model_data

        # Fetch and decode climatology once; compute both Cheng-adjusted and original_gcm
        # versions in memory rather than making two network calls (the doy_climatology coverage
        # has no source dimension, so both versions start from the same raw data).
        datasets = asyncio.run(
            fetch_hydro_data(coverages["doy_climatology"], stream_id)
        )
        decode_dicts = asyncio.run(
            get_decode_dicts_from_axis_attributes(coverages["doy_climatology"])
        )

        decoded_datasets = []
        for ds, decode_dict in zip(datasets, decode_dicts):
            for dim, mapping in decode_dict.items():
                ds = ds.assign_coords({dim: [mapping[int(v)] for v in ds[dim].values]})
            decoded_datasets.append(ds)

        raw_data_dict = package_hydrograph_data(stream_id, decoded_datasets)

        # Build Cheng-adjusted version; PGW models missing a 1990-2021 era are silently skipped.
        cheng_adjusted = calculate_and_apply_gcm_diffs_to_cheng_climatology(
            copy.deepcopy(raw_data_dict["data"])
        )

        # Fill in PGW models (absent from cheng_adjusted) using their original_gcm values.
        for model, model_data in raw_data_dict["data"].items():
            if model not in cheng_adjusted:
                cheng_adjusted[model] = model_data

        climatology_data = cheng_adjusted

        historical_climatology = climatology_data["historical"][historical_era]
        historical_stats = stats["data"]["historical"]

        # Non-"historical" models at the future era only.
        projected_climatology = {
            model: model_data
            for model, model_data in climatology_data.items()
            if model != "historical" and chart_era in model_data
        }
        projected_stats = {
            model: model_data
            for model, model_data in stats["data"].items()
            if model != "historical" and chart_era in model_data
        }

        ########## Populate arrays for hydrograph. ##########

        hydrograph = {
            "historical": {
                "doy_min": [x["doy_min"] for x in historical_climatology],
                "doy_mean": [x["doy_mean"] for x in historical_climatology],
                "doy_max": [x["doy_max"] for x in historical_climatology],
            },
            "projected": {
                chart_era: {
                    "doy_min_min": [],
                    "doy_mean_min": [],
                    "doy_mean_mean": [],
                    "doy_mean_max": [],
                    "doy_max_max": [],
                }
            },
        }

        for i in range(366):
            doy_mins = []
            doy_means = []
            doy_maxes = []
            for model_data in projected_climatology.values():
                doy_mins.append(model_data[chart_era][i]["doy_min"])
                doy_means.append(model_data[chart_era][i]["doy_mean"])
                doy_maxes.append(model_data[chart_era][i]["doy_max"])

            hydrograph["projected"][chart_era]["doy_min_min"].append(
                round(min(doy_mins), 3)
            )
            hydrograph["projected"][chart_era]["doy_mean_min"].append(
                round(min(doy_means), 3)
            )
            hydrograph["projected"][chart_era]["doy_mean_mean"].append(
                round(statistics.mean(doy_means), 3)
            )
            hydrograph["projected"][chart_era]["doy_mean_max"].append(
                round(max(doy_means), 3)
            )
            hydrograph["projected"][chart_era]["doy_max_max"].append(
                round(max(doy_maxes), 3)
            )

        ########## Populate arrays for monthly modeled flow rate chart. ##########

        monthly_flow_keys = [
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

        monthly_flow = {
            "historical": {},
            "projected": {chart_era: {}},
        }

        for key in monthly_flow_keys:
            monthly_flow["historical"][key] = historical_stats[historical_era][key]

        for model_stats in projected_stats.values():
            for key in monthly_flow_keys:
                if key not in monthly_flow["projected"][chart_era]:
                    monthly_flow["projected"][chart_era][key] = []
                monthly_flow["projected"][chart_era][key].append(
                    model_stats[chart_era][key]
                )

        ########## Populate arrays for max flow date chart. ##########

        max_flow_dates = {
            "historical": {
                "flow": historical_stats[historical_era].get("dh1"),
                "date": historical_stats[historical_era].get("th1"),
            },
            "projected": {chart_era: {"flow": [], "date": []}},
        }

        for model_stats in projected_stats.values():
            max_flow_dates["projected"][chart_era]["flow"].append(
                model_stats[chart_era].get("dh1")
            )
            max_flow_dates["projected"][chart_era]["date"].append(
                model_stats[chart_era].get("th1")
            )

        ########## Calculate stats for the stats table. ##########

        table_stats = {
            "historical": historical_stats,
            "projected": {chart_era: {}},
        }

        stat_arrays = {}
        for model_stats in projected_stats.values():
            for stat, val in model_stats[chart_era].items():
                if stat not in stat_arrays:
                    stat_arrays[stat] = []
                stat_arrays[stat].append(val)

        for stat, values in stat_arrays.items():
            table_stats["projected"][chart_era][stat] = {
                "min": round(min(values), 3),
                "median": round(statistics.median(values), 3),
                "max": round(max(values), 3),
            }

        response = {
            "gauge_id": None,
            "huc8": None,
            "huc8_outlet": None,
            "hydrograph": hydrograph,
            "id": stats["id"],
            "name": stats["name"],
            "monthly_flow": monthly_flow,
            "max_flow_dates": max_flow_dates,
            "stats": table_stats,
            "summary": stats.get("summary"),
        }

        return jsonify(response)

    except Exception as exc:
        return render_template("500/server_error.html"), 500
