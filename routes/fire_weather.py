import asyncio
import logging
import xarray as xr
import io
import numpy as np
from flask import Blueprint, render_template, request
import datetime
import time

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str, generate_netcdf_wcs_getcov_str
from fetch_data import (
    fetch_data,
    describe_via_wcps,
    ymd_to_cftime_value,
    cftime_value_to_ymd,
    get_encoding_from_axis_attributes,
    get_attributes_from_time_axis,
    get_poly,
    fetch_bbox_netcdf,
)
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    check_geotiffs,
    check_poly_in_geotiffs,
    validate_year,
    validate_var_id,
)
from zonal_stats import (
    get_scale_factor,
    rasterize_polygon,
    interpolate,
    calculate_zonal_means_vectorized,
)
from csv_functions import create_csv
from luts import summer_fire_danger_ratings_dict

from . import routes

logger = logging.getLogger(__name__)

fire_weather_api = Blueprint("fire_weather_api", __name__)


#### SETUP AND METADATA ####

# we have one geotiff to validate all fire weather variables, so its not the same as coverage names
fire_weather_geotiff = "cmip6_all_fire_weather_variables"

fire_weather_coverage_ids = [
    "cmip6_bui",
    "cmip6_dc",
    "cmip6_dmc",
    "cmip6_ffmc",
    "cmip6_fwi",
    "cmip6_isi",
]


# get the basic metadata for all fire weather coverages
async def get_coverage_metadata(coverage_id):
    """Get the coverage metadata."""
    metadata = await describe_via_wcps(coverage_id)
    return metadata


var_coverage_metadata = {}
for coverage_id in fire_weather_coverage_ids:
    coverage_metadata = asyncio.run(get_coverage_metadata(coverage_id))
    base_date, time_min, time_max = get_attributes_from_time_axis(coverage_metadata)
    # below assumes only one variable per coverage
    var_coverage_metadata[list(coverage_metadata["metadata"]["bands"].keys())[0]] = {
        "coverage_id": coverage_id,
        "model_encoding": get_encoding_from_axis_attributes("model", coverage_metadata),
        "start_cf_time": time_min,  # integer days since base date
        "end_cf_time": time_max,  # integer days since base date
        "base_date": base_date,  # datetime.datetime object
        "start_date": cftime_value_to_ymd(
            time_min, base_date
        ),  # (year, month, day) tuple
        "end_date": cftime_value_to_ymd(
            time_max, base_date
        ),  # (year, month, day) tuple
    }

ops_dict = {
    "3_day_rolling_average": 3,
    "5_day_rolling_average": 5,
    "7_day_rolling_average": 7,
    "summer_fire_danger_rating_days": "",
}


#### VALIDATION FUNCTIONS ####


def validate_latlon(lat, lon):
    if latlon_is_numeric_and_in_geodetic_range(lat, lon) == 400:
        return render_template("400/bad_request.html"), 400
    if check_geotiffs(float(lat), float(lon), coverages=[fire_weather_geotiff]) == 404:
        print("no data at lat/lon")
        return render_template("404/no_data.html"), 404
    return True


def validate_vars(requested_vars):
    if requested_vars:
        requested_vars = requested_vars.split(",")
        for var in requested_vars:
            if var not in var_coverage_metadata:
                return render_template("422/invalid_get_parameter.html"), 422
    else:
        requested_vars = [var for var in var_coverage_metadata]
    return requested_vars


def validate_requested_vars_start_and_end_year(requested_vars, start_year, end_year):
    var_time_slices = {}
    for var in requested_vars:
        if None in [start_year, end_year]:
            # use full range if no years requested
            time_slice_cf = (
                var_coverage_metadata[var]["start_cf_time"],
                var_coverage_metadata[var]["end_cf_time"],
            )
        if None not in [start_year, end_year]:
            if not validate_year(start_year, end_year):
                render_template(
                    "422/invalid_year.html",
                    start_year=start_year,
                    end_year=end_year,
                    min_year=coverage_metadata["start_date"][0],
                    max_year=coverage_metadata["end_date"][0],
                ), 422

            start_cf_time = ymd_to_cftime_value(
                start_year, 4, 1, var_coverage_metadata[var]["base_date"]
            )
            end_cf_time = ymd_to_cftime_value(
                end_year, 10, 1, var_coverage_metadata[var]["base_date"]
            )
            time_slice_cf = (start_cf_time, end_cf_time)

        var_time_slices[var] = time_slice_cf

    return var_time_slices


#### DATA FETCHING FUNCTIONS ####


async def fetch_point_data_for_all_vars(requested_vars, lat, lon, var_time_slices):
    """
    Fetch the data for the requested variables at the given lat/lon and time range.
    Args:
        requested_vars (list): list of requested variables
        lat (float): latitude
        lon (float): longitude
        var_time_slices (dict): dict of time slices for each variable
    Returns:
        dict: fetched data as xarray.Datasets, one per variable
    """

    fetched_data = {}
    tasks = []

    for var in requested_vars:
        coverage_id = var_coverage_metadata[var]["coverage_id"]
        time_slice = ("time", f"{var_time_slices[var][0]},{var_time_slices[var][1]}")
        url = generate_wcs_query_url(
            generate_wcs_getcov_str(
                x=lon,
                y=lat,
                cov_id=coverage_id,
                var_coord=None,
                time_slice=time_slice,
                encoding="netcdf",
                projection="EPSG:4326",
            )
        )

        url += f"&RANGESUBSET={var}"

        tasks.append(fetch_data([url]))

    results = await asyncio.gather(*tasks)

    for requested_var, result in zip(requested_vars, results):
        ds = xr.open_dataset(io.BytesIO(result))
        fetched_data[requested_var] = ds

    return fetched_data


async def fetch_polygon_data_for_all_vars(requested_vars, polygon, var_time_slices):
    """
    Fetch the data for the requested variables in the polygon and time range.

    Args:
        requested_vars (list): list of requested variables
        polygon (GeoDataFrame): polygon to fetch data for
        var_time_slices (dict): dict of time slices for each variable
    Returns:
        dict: Dictionary with fetched data as xarray.Datasets, one per variable
    """
    tasks = []
    bbox_bounds = polygon.total_bounds
    x_str = f"{bbox_bounds[0]},{bbox_bounds[2]}"
    y_str = f"{bbox_bounds[1]},{bbox_bounds[3]}"
    for var in requested_vars:
        coverage_id = var_coverage_metadata[var]["coverage_id"]
        time_slice = ("time", f"{var_time_slices[var][0]},{var_time_slices[var][1]}")
        url = generate_wcs_query_url(
            generate_wcs_getcov_str(
                x=x_str,
                y=y_str,
                cov_id=coverage_id,
                var_coord=None,
                time_slice=time_slice,
                encoding="netcdf",
                projection="EPSG:4326",
            )
        )

        url += f"&RANGESUBSET={var}"

        tasks.append(fetch_bbox_netcdf([url]))

    datasets = await asyncio.gather(*tasks)
    datasets_dict = {var_name: ds for var_name, ds in zip(requested_vars, datasets)}
    return datasets_dict


def calculate_zonal_stats(polygon, datasets_dict, variables):
    """Process zonal statistics for variable datasets.

    Args:
        polygon (GeoDataFrame): Target polygon
        datasets_dict (dict): Dictionary with variable names mapped to xarray.Datasets
        variables (list): List of variable names

    Returns:
        dict: Dictionary with variable names mapped to xarray.Datasets of zonal statistics
    """
    logger.info(f"Processing zonal stats for {variables} variables")
    time_start = time.time()

    # convert polygon and all datasets to 3338 so we can do area calculations in meters
    polygon = polygon.to_crs(epsg=3338)

    # we need to split the datasets by model and reproject each one because we cant reproject multi-model datasets directly
    datasets_by_var_model_dict = {}
    for var_name in variables:
        datasets_by_var_model_dict[var_name] = {}
        var_ds = datasets_dict[var_name]
        models = var_ds["model"].values
        for model in models:
            ds_model = var_ds.sel(model=model)
            ds_model = ds_model.rio.write_crs("EPSG:4326", inplace=True)
            ds_model = ds_model.rio.reproject("EPSG:3338")
            datasets_by_var_model_dict[var_name][model] = ds_model

    # use first dataset to get spatial resolution and rasterization
    ds = next(iter(datasets_by_var_model_dict[variables[0]].values()))

    # get scale factor once, not per variable or time slice!
    spatial_resolution = ds.rio.resolution()
    grid_cell_area_m2 = abs(spatial_resolution[0]) * abs(spatial_resolution[1])
    polygon_area_m2 = polygon.area
    scale_factor = get_scale_factor(grid_cell_area_m2, polygon_area_m2)

    # create an initial array for the basis of polygon rasterization
    # why? polygon rasterization bogs down hard when doing it in the loop
    da_i = interpolate(
        ds.isel(time=0), variables[0], "x", "y", scale_factor, method="nearest"
    )

    rasterized_polygon_array = rasterize_polygon(da_i, "x", "y", polygon)

    zonal_results = {}
    for var_name in variables:
        zonal_results[var_name] = {}
        for model in datasets_by_var_model_dict[var_name]:
            ds = datasets_by_var_model_dict[var_name][model]
            # interpolate the entire time series for the variable
            da_i_3d = interpolate(
                ds, var_name, "x", "y", scale_factor, method="nearest"
            )
            # calculate zonal stats for the entire time series
            time_series_means = calculate_zonal_means_vectorized(
                da_i_3d, rasterized_polygon_array, "x", "y"
            )
            zonal_results[var_name][model] = time_series_means

    logger.info(
        f"Zonal stats processed in {round(time.time() - time_start, 2)} seconds"
    )

    # the returned dict is variable -> model -> list of daily zonal means
    # turn the lists into xarray datasets for postprocessing: one dataset per variable, with all models ... no spatial info required here
    zonal_results_datasets = {}
    for var_name in zonal_results:
        model_dataarrays = []
        models = []
        for model in zonal_results[var_name]:
            models.append(model)
            model_da = xr.DataArray(
                zonal_results[var_name][model],
                dims=["time"],
                coords={"time": datasets_dict[var_name]["time"].values},
                name=var_name,
            )
            model_dataarrays.append(model_da)

        # create datasets from the arrays
        var_ds = xr.Dataset(
            {var_name: xr.concat(model_dataarrays, dim="model")},
            coords={"model": models, "time": datasets_dict[var_name]["time"].values},
        )
        zonal_results_datasets[var_name] = var_ds

    return zonal_results_datasets


#### POSTPROCESSING FUNCTIONS ####


def dayofyear_to_mmdd(dayofyear):
    """Convert integer day of year (1-365) to a MM-DD string for a non-leap year."""
    days = float(dayofyear - 1)  # this cant be an int, so we convert to float
    date = datetime.datetime(2001, 1, 1) + datetime.timedelta(days)
    return date.strftime("%m-%d")


def set_dataset_doy_str(ds):
    """Convert the integer dayofyear coordinate to a MM-DD string for better readability."""
    dayofyear_str = [dayofyear_to_mmdd(doy) for doy in ds["dayofyear"].values]
    ds = ds.assign_coords(dayofyear=("dayofyear", dayofyear_str))
    return ds


def build_variable_year_range_str_from_start_and_end_year(var, start_year, end_year):
    """Build a year range string from start and end year."""
    if start_year is None:
        start_year, _start_month, _start_day = cftime_value_to_ymd(
            var_coverage_metadata[var]["start_cf_time"],
            var_coverage_metadata[var]["base_date"],
        )
        end_year, _end_month, _end_day = cftime_value_to_ymd(
            var_coverage_metadata[var]["end_cf_time"],
            var_coverage_metadata[var]["base_date"],
        )
        year_range_str = str(str(start_year) + "-" + str(end_year))
    else:
        year_range_str = str(str(start_year) + "-" + str(end_year))
    return year_range_str


def nday_rolling_average(n, data_dict, var_coverage_metadata, start_year, end_year):
    """
    For each dataset in the dictionary, we will take an n-day rolling average of values (smoothing).
    Then we summarize min, mean, and max of those rolling averages across the entire time range, for each model (including the baseline).
    Return the data as a dictionary with variable, model, time range, and min/mean/max values for each DOY from April 1 to October 31.

    ***NOTE: We skip NAs when calculating min/mean/max.
    This matters for ERA5 model if date range includes both historical and projected (e.g. 2000-2030) because if we skip NA,
    we are averaging the historical values only (e.g. 2000-2020).

    Args:
        n (int): number of days for rolling average
        data_dict (dict): dict of xarray.Datasets, one per variable
        var_coverage_metadata (dict): metadata for each variable, which includes model encoding
        start_year (str): start year of the data
        end_year (str): end year of the data
    Returns:
        dict: postprocessed data
    """
    # highest level of the returned dict is year range
    # use first var in data_dict to determine year range if start_year and end_year are None
    var = list(data_dict.keys())[0]
    year_range_str = build_variable_year_range_str_from_start_and_end_year(
        var, start_year, end_year
    )
    var_nday_summary = {year_range_str: {}}

    # next levels are variable, model, and min/mean/max of 3-day rolling average per DOY
    for var in data_dict:
        var_nday_summary[year_range_str][var] = {}

        ds = data_dict[var]
        # Apply a n-day rolling average along the time dimension
        ds_rolled = ds.rolling(time=int(n), center=True).mean(skipna=True)

        # Group by day of year and model, and calculate min, mean, max
        ds_min_doy = ds_rolled.groupby(["time.dayofyear", "model"]).min(skipna=True)
        ds_mean_doy = ds_rolled.groupby(["time.dayofyear", "model"]).mean(skipna=True)
        ds_max_doy = ds_rolled.groupby(["time.dayofyear", "model"]).max(skipna=True)

        # Replace the integer DOY with dates in format MM-DD for better readability
        ds_min_doy = set_dataset_doy_str(ds_min_doy)
        ds_mean_doy = set_dataset_doy_str(ds_mean_doy)
        ds_max_doy = set_dataset_doy_str(ds_max_doy)

        # for each model in the dataset create a dict of DOYs under that model
        for model in ds_mean_doy["model"].values:
            # use model names from the coverage metadata
            model_name_str = var_coverage_metadata[var]["model_encoding"][int(model)]
            var_nday_summary[year_range_str][var][model_name_str] = {}
            # for each DOY in the dataset extract min/mean/max values and create a dict of them under that DOY
            for doy in ds_mean_doy["dayofyear"].values:
                var_nday_summary[year_range_str][var][model_name_str][doy] = {
                    "min": float(
                        ds_min_doy.sel(dayofyear=doy, model=model)
                        .to_array()
                        .values[0]
                        .round(3)
                    ),
                    "mean": float(
                        ds_mean_doy.sel(dayofyear=doy, model=model)
                        .to_array()
                        .values[0]
                        .round(3)
                    ),
                    "max": float(
                        ds_max_doy.sel(dayofyear=doy, model=model)
                        .to_array()
                        .values[0]
                        .round(3)
                    ),
                }

    return var_nday_summary


def summer_fire_danger_rating_days(
    data_dict, var_coverage_metadata, start_year, end_year
):
    """For the months of June, July, and August, classify each day in the dataset based on the
    summer fire danger rating adjective classes for each fire weather variable. Count the days in each class
    per year, per model, and per variable. Get an average count of days in each class across all years in the dataset,
    rounded to the nearest integer.
    Return the data as a dictionary with variable, model, time range, and average counts for each class.

    Args:
        data_dict (dict): dict of xarray.Datasets, one per variable
        var_coverage_metadata (dict): metadata for each variable, which includes model encoding
        start_year (str): start year of the data
        end_year (str): end year of the data
    Returns:
        dict: postprocessed data

    """

    # highest level of the returned dict is year range
    # use first var in data_dict to determine year range if start_year and end_year are None
    first_var = list(data_dict.keys())[0]
    year_range_str = build_variable_year_range_str_from_start_and_end_year(
        first_var, start_year, end_year
    )
    var_summer_fire_summary = {year_range_str: {}}

    # get the total number of years from the year range string (to compute averages later)
    start_year_int = int(year_range_str.split("-")[0])
    end_year_int = int(year_range_str.split("-")[1])
    num_years = end_year_int - start_year_int + 1

    # next levels are variable, model, and the fire ranger rating average counts
    for var in data_dict:
        var_summer_fire_summary[year_range_str][var] = {}

        ds = data_dict[var]
        # drop any months that arent June, July, or August
        ds = ds.sel(time=ds["time"].dt.month.isin([6, 7, 8]))

        # For each variable, classify each daily value based on the summer fire danger rating classes in summer_fire_danger_ratings_dict
        var_classes = summer_fire_danger_ratings_dict[var].keys()
        for model in ds["model"].values:
            # use model names from the coverage metadata
            model_name_str = var_coverage_metadata[var]["model_encoding"][int(model)]
            var_summer_fire_summary[year_range_str][var][model_name_str] = {}

            for var_class in var_classes:
                # seed count at 0
                var_summer_fire_summary[year_range_str][var][model_name_str][
                    var_class
                ] = 0
                # get class bounds
                class_min, class_max = summer_fire_danger_ratings_dict[var][var_class]

                # classify each value
                # catch era5 case where only years up to 2020 should be counted
                if (
                    model_name_str == "era5"
                    and start_year_int < 2021
                    and end_year_int >= 2021
                ):
                    ds_model = ds.sel(
                        model=model,
                        time=ds["time"].dt.year.isin(list(range(start_year_int, 2021))),
                    )
                    num_years = 2020 - start_year_int + 1
                else:
                    ds_model = ds.sel(model=model)

                values = ds_model[var].values
                for value in values:
                    if class_min <= value < class_max:
                        var_summer_fire_summary[year_range_str][var][model_name_str][
                            var_class
                        ] += 1
            # compute average counts per year, rounded to nearest integer
            # handle era5 case which has a different number of years

            for var_class in var_classes:
                avg_count = (
                    var_summer_fire_summary[year_range_str][var][model_name_str][
                        var_class
                    ]
                    / num_years
                )
                var_summer_fire_summary[year_range_str][var][model_name_str][
                    var_class
                ] = (int(round(avg_count)) if not np.isnan(avg_count) else np.nan)

    return var_summer_fire_summary


def drop_era5(results_dict):
    """Drop the ERA5 model from the results dictionary. Use this function if the start year is 2021 or later (2020 is last year of ERA5 data)."""
    for year_range in results_dict:
        for var in results_dict[year_range]:
            if "era5" in results_dict[year_range][var]:
                del results_dict[year_range][var]["era5"]
    return results_dict


#### FLASK ROUTES ####


@routes.route("/fire_weather/")
def fire_weather_about():
    return render_template("/documentation/fire_weather.html")


@routes.route("/fire_weather/point/<lat>/<lon>")
@routes.route("/fire_weather/point/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_fire_weather_point_data(lat, lon, start_year=None, end_year=None):
    """
    Query the daily fire weather coverage.
    GET parameters:
        vars: comma-separated list of variables to fetch (default: all variables)
            valid variables: bui, dmc, dc, ffmc, fwi, isi
        op: postprocessing operation to perform (required)
            valid operations: 3_day_rolling_average, 5_day_rolling_average, 7_day_rolling_average, summer_fire_danger_rating_days
            only one operation can be performed at a time
            default is 3_day_rolling_average
        format: output format (only supports "csv")

    Args:
        lat (float): latitude
        lon (float): longitude
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested daily fire weather data

    Notes:
        usage examples:
        - 3 day rolling average for all variables, all years: http://localhost:5000/fire_weather/point/65.06/-146.16
        - 5 day rolling average for select variables, all years: http://localhost:5000/fire_weather/point/65.06/-146.16?vars=bui,fwi&op=5_day_rolling_average
        - summer fire danger rating days for select years: http://localhost:5000/fire_weather/point/65.06/-146.16/2000/2030?op=summer_fire_danger_rating_days
        - summer fire danger rating days for select variables, select years: http://localhost:5000/fire_weather/point/65.06/-146.16/2000/2030?vars=bui,fwi&op=summer_fire_danger_rating_days

    """
    latlon_validation = validate_latlon(lat, lon)
    if isinstance(latlon_validation, tuple):
        return latlon_validation

    requested_vars = request.args.get("vars")
    requested_vars = validate_vars(requested_vars)

    start_year = int(start_year) if start_year is not None else None
    end_year = int(end_year) if end_year is not None else None
    var_time_slices = validate_requested_vars_start_and_end_year(
        requested_vars, start_year, end_year
    )

    requested_ops = request.args.get("op")

    fetched_data = asyncio.run(
        fetch_point_data_for_all_vars(
            requested_vars, float(lat), float(lon), var_time_slices
        )
    )

    if ops_dict.get(requested_ops, None) == None:
        n = 3  # default to 3-day rolling average
    else:
        n = ops_dict.get(requested_ops)

    if n in [3, 5, 7]:
        processed_data = nday_rolling_average(
            n,
            fetched_data,
            var_coverage_metadata,
            start_year,
            end_year,
        )

    else:
        processed_data = summer_fire_danger_rating_days(
            fetched_data,
            var_coverage_metadata,
            start_year,
            end_year,
        )

    if start_year is not None and start_year >= 2021:
        processed_data = drop_era5(processed_data)

    if request.args.get("format") == "csv":
        # reformat ops string for filename prefix, e.g "CMIP6 Fire Weather Indices - 3 Day Rolling Average"
        filename_prefix = "CMIP6 Fire Weather Indices - " + " ".join(
            [word.capitalize() for word in requested_ops[0].split("_")]
        )
        return create_csv(
            data=processed_data,
            endpoint="fire_weather",
            lat=lat,
            lon=lon,
            filename_prefix=filename_prefix,
            vars=requested_vars,
            start_year=start_year,
            end_year=end_year,
        )
    else:
        return processed_data


@routes.route("/fire_weather/area/<place_id>")
@routes.route("/fire_weather/area/<place_id>/<start_year>/<end_year>")
def run_fetch_fire_weather_area_data(place_id, start_year=None, end_year=None):
    """
    Query the daily fire weather coverage.
    GET parameters:
        vars: comma-separated list of variables to fetch (default: all variables)
            valid variables: bui, dmc, dc, ffmc, fwi, isi
        op: postprocessing operation to perform (required)
            valid operations: 3_day_rolling_average, 5_day_rolling_average, 7_day_rolling_average, summer_fire_danger_rating_days
            only one operation can be performed at a time
        format: output format (only supports "csv")

    Args:
        place_id (str): place identifier, used to fetch polygon and compute zonal statistics
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested daily fire weather data

    Notes:
        usage examples (HUC10 polygon: 1908030906):
        - 3 day rolling average for all variables, all years: http://localhost:5000/fire_weather/area/1908030906
        - 5 day rolling average for select variables, all years: http://localhost:5000/fire_weather/area/1908030906?vars=bui,fwi&op=5_day_rolling_average
        - summer fire danger rating days for select years: http://localhost:5000/fire_weather/area/1908030906/2000/2030?op=summer_fire_danger_rating_days
        - summer fire danger rating days for select variables, select years: http://localhost:5000/fire_weather/area/1908030906/2000/2030?vars=bui,fwi&op=summer_fire_danger_rating_days
    """

    poly_type = validate_var_id(place_id)
    if type(poly_type) is tuple:
        return poly_type

    try:
        polygon = get_poly(place_id, crs=4326)
    except:
        return render_template("422/invalid_area.html"), 422

    # validate that the polygon is completely within the geotiff that represents the footprint of the data coverage
    if check_poly_in_geotiffs(polygon, coverages=[fire_weather_geotiff]) == 404:
        return render_template("404/no_data.html"), 404

    requested_vars = request.args.get("vars")
    requested_vars = validate_vars(requested_vars)

    start_year = int(start_year) if start_year is not None else None
    end_year = int(end_year) if end_year is not None else None
    var_time_slices = validate_requested_vars_start_and_end_year(
        requested_vars, start_year, end_year
    )

    requested_ops = request.args.get("op")

    try:
        # fetch bbox datasets for requested variables
        datasets_dict = asyncio.run(
            fetch_polygon_data_for_all_vars(requested_vars, polygon, var_time_slices)
        )
        zonal_results = calculate_zonal_stats(polygon, datasets_dict, requested_vars)
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    if ops_dict.get(requested_ops, None) == None:
        n = 3  # default to 3-day rolling average
    else:
        n = ops_dict.get(requested_ops)

    if n in [3, 5, 7]:
        processed_data = nday_rolling_average(
            n,
            zonal_results,
            var_coverage_metadata,
            start_year,
            end_year,
        )

    else:
        processed_data = summer_fire_danger_rating_days(
            zonal_results,
            var_coverage_metadata,
            start_year,
            end_year,
        )

    if start_year is not None and start_year >= 2021:
        processed_data = drop_era5(processed_data)

    if request.args.get("format") == "csv":
        # reformat ops string for filename prefix, e.g "CMIP6 Fire Weather Indices - 3 Day Rolling Average"
        if requested_ops is None:
            # use first key of ops dict for default if no ops requested
            requested_ops = list(ops_dict.keys())[0]
        op_str = " ".join([word.capitalize() for word in requested_ops.split("_")])
        filename_prefix = "CMIP6 Fire Weather Indices - " + op_str
        return create_csv(
            data=processed_data,
            endpoint="fire_weather",
            place_id=place_id,
            filename_prefix=filename_prefix,
            vars=requested_vars,
            start_year=start_year,
            end_year=end_year,
        )
    else:
        return processed_data
