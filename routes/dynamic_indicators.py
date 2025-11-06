import asyncio
import logging
from flask import Blueprint, render_template, request

# local imports
from fetch_data import fetch_data, describe_via_wcps, get_poly, fetch_bbox_netcdf_list
from generate_urls import generate_wcs_query_url
from generate_requests import (
    construct_count_annual_days_above_or_below_threshold_wcps_query_string,
    construct_get_annual_mmm_stat_wcps_query_string,
    generate_netcdf_wcs_getcov_str,
)
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    validate_year,
    project_latlon,
    validate_latlon_in_bboxes,
    construct_latlon_bbox_from_coverage_bounds,
    validate_var_id,
)
from zonal_stats import (
    get_scale_factor,
    rasterize_polygon,
    interpolate,
    calculate_zonal_means_vectorized,
)

# TODO: for additional postprocessing or csv output, uncomment these imports and add code
# from postprocessing import postprocess, prune_nulls_with_max_intensity
# from csv_functions import create_csv

from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("dynamic_indicators_api", __name__)

all_coverages = {
    "pr": [
        "cmip6_downscaled_pr_6ModelAvg_historical_wcs",
        "cmip6_downscaled_pr_6ModelAvg_ssp126_wcs",
        "cmip6_downscaled_pr_6ModelAvg_ssp245_wcs",
        "cmip6_downscaled_pr_6ModelAvg_ssp370_wcs",
        "cmip6_downscaled_pr_6ModelAvg_ssp585_wcs",
    ],
    "tasmin": [
        "cmip6_downscaled_tasmin_6ModelAvg_historical_wcs",
        "cmip6_downscaled_tasmin_6ModelAvg_ssp126_wcs",
        "cmip6_downscaled_tasmin_6ModelAvg_ssp245_wcs",
        "cmip6_downscaled_tasmin_6ModelAvg_ssp370_wcs",
        "cmip6_downscaled_tasmin_6ModelAvg_ssp585_wcs",
    ],
    "tasmax": [
        "cmip6_downscaled_tasmax_6ModelAvg_historical_wcs",
        "cmip6_downscaled_tasmax_6ModelAvg_ssp126_wcs",
        "cmip6_downscaled_tasmax_6ModelAvg_ssp245_wcs",
        "cmip6_downscaled_tasmax_6ModelAvg_ssp370_wcs",
        "cmip6_downscaled_tasmax_6ModelAvg_ssp585_wcs",
    ],
}

time_domains = {
    "historical": (
        1966,
        2014,
    ),  # NOTE: actual data starts at 1965, but we are having an issue with noon time stamps at lower bound! Ask JP for more details
    "projected": (
        2016,
        2100,
    ),  # NOTE: actual data starts at 2015, but we are having an issue with noon time stamps at lower bound! Ask JP for more details
}


async def get_cmip6_metadata(cov_id):
    """Get the coverage metadata and encodings for CMIP6 downscaled daily coverage"""
    metadata = await describe_via_wcps(cov_id)
    return metadata


def validate_latlon_and_reproject_to_epsg_3338(lat, lon, variable):
    """Validate lat/lon, then reproject to EPSG:3338"""
    lat = float(lat)
    lon = float(lon)
    if not latlon_is_numeric_and_in_geodetic_range(lat, lon):
        return render_template("400/bad_request.html"), 400

    var_coverages = all_coverages.get(variable)

    for cov_id in var_coverages:

        metadata = asyncio.run(get_cmip6_metadata(cov_id))
        cmip6_downscaled_bbox = construct_latlon_bbox_from_coverage_bounds(metadata)
        within_bounds = validate_latlon_in_bboxes(
            lat, lon, [cmip6_downscaled_bbox], [cov_id]
        )
        if within_bounds == 404:
            return (
                render_template("404/no_data.html"),
                404,
            )
        if within_bounds == 422:
            return (
                render_template(
                    "422/invalid_latlon_outside_coverage.html",
                    bboxes=[cmip6_downscaled_bbox],
                ),
                422,
            )

    lon, lat = project_latlon(lat, lon, dst_crs=3338)

    return lon, lat


def validate_operator(operator):
    """Validate operator is 'above' or 'below' and convert to '>' or '<'"""
    if operator not in ["above", "below"]:
        return render_template("400/bad_request.html"), 400
    if operator == "above":
        operator = ">"
    else:
        operator = "<"
    return operator


def validate_units_threshold_and_variable(units, threshold, variable):
    """Validate units and threshold based on variable type. Convert threshold to standard units if needed."""
    if variable in ["tasmax", "tasmin"]:
        if units not in ["C", "F"]:
            raise ValueError("Units for temperature must be 'C' or 'F'.")
        if threshold is not None:
            if units == "F":
                threshold = (float(threshold) - 32) * 5.0 / 9.0
            else:
                threshold = float(threshold)
    elif variable == "pr":
        if units not in ["mm", "in"]:
            return render_template("400/bad_request.html"), 400
        if threshold is not None:
            if units == "in":
                threshold = float(threshold) * 25.4
            else:
                threshold = float(threshold)
    else:
        return render_template("400/bad_request.html"), 400

    # precipitation thresholds should be >= 0
    if variable == "pr" and threshold is not None and threshold < 0:
        return render_template("400/bad_request.html"), 400

    return units, threshold


def validate_stat(stat):
    """Validate that stat is one of 'max', 'min', 'mean', or 'sum'."""
    if stat not in ["max", "min", "mean", "sum"]:
        return render_template("400/bad_request.html"), 400
    if stat == "mean":
        stat = "avg"  # NOTE: rasdaman uses 'avg' instead of 'mean' in WCPS queries
    return stat


def validate_rank_position_and_direction(position, direction):
    """Validate rank position and direction. Position must be between 1 and 365, and direction must be 'highest' or 'lowest'."""
    try:
        position = int(position)
        if position < 1 or position > 365:
            raise ValueError
    except ValueError:
        return render_template("400/bad_request.html"), 400
    if direction not in ["highest", "lowest"]:
        return render_template("400/bad_request.html"), 400
    return position, direction


def validate_place_id(place_id):
    poly_type = validate_var_id(place_id)
    if type(poly_type) is tuple:
        return poly_type
    try:
        polygon = get_poly(place_id)
    except:
        return render_template("422/invalid_area.html"), 422
    return polygon


def build_year_and_coverage_lists_for_iteration(
    start_year, end_year, variable, time_domains, all_coverages
):
    """Build lists of year ranges and variable coverages for iteration based on start and end years. If years span historical and projected, need to split into two ranges."""
    year_ranges = []
    var_coverages = []
    historical_range = time_domains["historical"]
    projected_range = time_domains["projected"]

    if start_year < historical_range[1] and end_year > historical_range[0]:
        # overlaps historical
        hist_start = max(start_year, historical_range[0])
        hist_end = min(end_year, historical_range[1])
        year_ranges.append((hist_start, hist_end))
        var_coverages.append(all_coverages[variable][0])
    if start_year < projected_range[1] and end_year > projected_range[0]:
        # overlaps projected
        proj_start = max(start_year, projected_range[0])
        proj_end = min(end_year, projected_range[1])
        year_ranges.append((proj_start, proj_end))
        # for projected, use all SSPs
        for ssp_coverage in all_coverages[variable][1:]:
            year_ranges.append((proj_start, proj_end))
            var_coverages.append(ssp_coverage)

    return year_ranges, var_coverages


def calculate_indicators_zonal_stats(polygon, dataset, variable):
    """Calculate zonal mean statistics for the given polygon and dataset."""
    # get scale factor once, not per variable or time slice!
    ds = dataset
    spatial_resolution = ds.rio.resolution()
    grid_cell_area_m2 = abs(spatial_resolution[0]) * abs(spatial_resolution[1])
    polygon_area_m2 = polygon.area
    scale_factor = get_scale_factor(grid_cell_area_m2, polygon_area_m2)

    # create an initial array for the basis of polygon rasterization
    # why? polygon rasterization bogs down hard when doing it in the loop
    da_i = interpolate(
        ds.isel(ansi=0), variable, "X", "Y", scale_factor, method="nearest"
    )

    rasterized_polygon_array = rasterize_polygon(da_i, "X", "Y", polygon)

    da_i_3d = interpolate(ds, variable, "X", "Y", scale_factor, method="nearest")
    # calculate zonal stats for the entire time series
    # rename the ansi dimensions to "time" so this function will work
    da_i_3d = da_i_3d.rename({"ansi": "time"})
    time_series_means = calculate_zonal_means_vectorized(
        da_i_3d, rasterized_polygon_array, "X", "Y"
    )

    return time_series_means


async def fetch_count_days_data(
    var_coverages, year_ranges, threshold, operator, lon, lat
):
    """Fetch count of days above or below threshold for given variable coverages and year ranges."""
    tasks = []
    for coverage, year_range in zip(var_coverages, year_ranges):
        url = generate_wcs_query_url(
            "ProcessCoverages&query="
            + construct_count_annual_days_above_or_below_threshold_wcps_query_string(
                coverage,
                operator,
                threshold,
                year_range[0],
                year_range[1],
                x_coord=lon,
                y_coord=lat,
            )
        )

        tasks.append(fetch_data([url]))
    data = await asyncio.gather(*tasks)

    return data


async def fetch_area_data_and_calculate_zonal_stats(
    variable, var_coverages, year_ranges, polygon
):
    """
    Make an async request for CMIP6 downscaled daily data for provided coverage within a specified polygon.
    Args:
        var_coverages (list): list of coverage IDs
        year_ranges (tuple): (start_year, end_year)
        polygon (shapely.geometry.Polygon): polygon geometry
    Returns:
        list of data results within the specified polygon
    """
    urls = []
    for cov_id, year_range in zip(var_coverages, year_ranges):
        # Generate WCS GetCoverage request string for the polygon bounds
        wcs_str = generate_netcdf_wcs_getcov_str(polygon.total_bounds, cov_id)
        # add time range to WCS string
        wcs_str += f'&SUBSET=ansi("{year_range[0]}-01-01","{year_range[1]}-12-31")'
        # Generate the URL for the WCS query
        url = generate_wcs_query_url(wcs_str)
        urls.append(url)
    # Fetch the data and add to a list of area datasets (one dataset per coverage)
    area_dataset_list = await fetch_bbox_netcdf_list(urls)

    # Calculate zonal stats for each dataset
    area_daily_means_list = []
    for dataset in area_dataset_list:
        area_daily_means_list.append(
            calculate_indicators_zonal_stats(polygon, dataset, variable)
        )

    return area_daily_means_list


async def fetch_count_days_area_data(
    variable, var_coverages, year_ranges, threshold, operator, polygon
):
    """Fetch daily data covering a polygon area, and calculatee the zonal mean of the area.
    Count days above or below threshold for given variable coverages and year range."""

    area_daily_means_list = await fetch_area_data_and_calculate_zonal_stats(
        variable, var_coverages, year_ranges, polygon
    )

    # area_daily_means is a list, where each item corresponds to a coverage/year range combo
    # and each item is a list of daily means for each day in that range
    # to count days above or below threshold for each year, we need to iterate through each item in area_daily_means_list
    # and divide each list into chunks of 365 (no leap years) to count days above/below threshold per year
    # we will output a list of lists, where each sublist contains the counts for each year in the corresponding year range

    data = []

    for i, daily_means in enumerate(area_daily_means_list):
        num_years_in_range = year_ranges[i][1] - year_ranges[i][0] + 1
        yearly_counts = []
        for year_idx in range(num_years_in_range):
            year_daily_means = daily_means[year_idx * 365 : (year_idx + 1) * 365]
            if operator == ">":
                count = sum(1 for day_mean in year_daily_means if day_mean > threshold)
            else:
                count = sum(1 for day_mean in year_daily_means if day_mean < threshold)
            yearly_counts.append(count)
        data.append(yearly_counts)

    return data


def postprocess_count_days(data, start_year, end_year):
    """Postprocess count days data into structured dictionary output.
    If the year range spans historical and projected, our data will be a list of 5 lists:
    The first has day counts for each historical year, and the rest have day counts for each projected year for each SSP.
    If the year range is only historical or only projected, our data will be a list of 1-4 lists accordingly.

    We want to create dictionary for output, with the following structure:
    {
        "historical": {
            "data": {"2000": 45, "2001": 50, ...},
            "summary": {"min": 30, "max": 60, "mean": 45.5},
        },
        "projected": {
            "ssp126": {
                "data": {"2020": 55, "2021": 60, ...},
                "summary": {"min": 40, "max": 70, "mean": 55.5},
            },
            "ssp245": {
                "data": {"2020": 50, "2021": 55, ...},
                "summary": {"min": 35, "max": 65, "mean": 50.5},
            },
            ...
        }
    }
    """

    start_year = int(start_year)
    end_year = int(end_year)

    result = {}
    current_index = 0
    if (
        start_year < time_domains["historical"][1]
        and end_year > time_domains["historical"][0]
    ):
        # historical data present
        hist_data = data[current_index]
        hist_years = list(
            range(
                max(start_year, time_domains["historical"][0]),
                min(end_year, time_domains["historical"][1]) + 1,
            )
        )
        hist_day_counts = {str(year): hist_data[i] for i, year in enumerate(hist_years)}
        result["historical"] = {
            "data": hist_day_counts,
            "summary": {
                "min": round(min(hist_day_counts.values()), 2),
                "max": round(max(hist_day_counts.values()), 2),
                "mean": round(sum(hist_day_counts.values()) / len(hist_day_counts), 2),
            },
        }
        current_index += 1
    if (
        start_year < time_domains["projected"][1]
        and end_year > time_domains["projected"][0]
    ):
        # projected data present
        result["projected"] = {}
        ssp_names = ["ssp126", "ssp245", "ssp370", "ssp585"]
        proj_years = list(
            range(
                max(start_year, time_domains["projected"][0]),
                min(end_year, time_domains["projected"][1]) + 1,
            )
        )
        for ssp in ssp_names:
            proj_data = data[current_index]
            proj_day_counts = {
                str(year): proj_data[i] for i, year in enumerate(proj_years)
            }
            result["projected"][ssp] = {
                "data": proj_day_counts,
                "summary": {
                    "min": round(min(proj_day_counts.values()), 2),
                    "max": round(max(proj_day_counts.values()), 2),
                    "mean": round(
                        sum(proj_day_counts.values()) / len(proj_day_counts), 2
                    ),
                },
            }
            current_index += 1
    return result


async def fetch_annual_stat_data(var_coverages, year_ranges, stat, lon, lat):
    """Fetch annual statistic data for given variable coverages and year ranges."""
    tasks = []
    for coverage, year_range in zip(var_coverages, year_ranges):
        url = generate_wcs_query_url(
            "ProcessCoverages&query="
            + construct_get_annual_mmm_stat_wcps_query_string(
                coverage,
                stat,
                year_range[0],
                year_range[1],
                x_coord=lon,
                y_coord=lat,
            )
        )
        tasks.append(fetch_data([url]))
    data = await asyncio.gather(*tasks)

    return data


async def fetch_annual_stat_area_data(
    variable, var_coverages, year_ranges, stat, polygon
):
    """Fetch daily data covering a polygon area, and calculatee the zonal mean of the area.
    Calculate requested stat for given variable coverages and year range."""

    area_daily_means_list = await fetch_area_data_and_calculate_zonal_stats(
        variable, var_coverages, year_ranges, polygon
    )

    # area_daily_means is a list, where each item corresponds to a coverage/year range combo
    # and each item is a list of daily means for each day in that range
    # to get the stat for each year, we need to iterate through each item in area_daily_means_list
    # and divide each list into chunks of 365 (no leap years) and apply the stat function to the yearly values
    # we will output a list of lists, where each sublist contains the stat for each year in the corresponding year range

    # if no stat is provided, return all daily means (split by year) for ranking purposes

    data = []

    for i, daily_means in enumerate(area_daily_means_list):
        num_years_in_range = year_ranges[i][1] - year_ranges[i][0] + 1
        yearly_stats = []
        for year_idx in range(num_years_in_range):
            year_daily_means = daily_means[year_idx * 365 : (year_idx + 1) * 365]
            if stat == "max":
                stat_value = round(max(year_daily_means), 2)
            elif stat == "min":
                stat_value = round(min(year_daily_means), 2)
            elif stat == "sum":
                stat_value = round(sum(year_daily_means), 2)
            elif stat == "avg":
                stat_value = round(sum(year_daily_means) / len(year_daily_means), 2)
            else:
                stat_value = [round(value, 2) for value in year_daily_means]
            yearly_stats.append(stat_value)
        data.append(yearly_stats)

    return data


def postprocess_annual_stat(data, start_year, end_year, units):
    """Postprocess annual statistic data into structured dictionary output."""

    # define conversion functions
    def mm_to_inches(mm):
        return round((mm / 25.4), 2)

    def c_to_f(c):
        return round(((c * 9 / 5) + 32), 2)

    if units == "in":
        convert = mm_to_inches
    elif units == "F":
        convert = c_to_f
    else:  # round values regardless of units
        convert = lambda x: round(x, 2)

    start_year = int(start_year)
    end_year = int(end_year)
    result = {}
    current_index = 0

    if (
        start_year < time_domains["historical"][1]
        and end_year > time_domains["historical"][0]
    ):
        # historical data present
        hist_data = data[current_index]
        hist_years = list(
            range(
                max(start_year, time_domains["historical"][0]),
                min(end_year, time_domains["historical"][1]) + 1,
            )
        )
        hist_stats = {
            str(year): convert(hist_data[i]) for i, year in enumerate(hist_years)
        }
        result["historical"] = {
            "data": hist_stats,
            "summary": {
                "min": round(min(hist_stats.values()), 2),
                "max": round(max(hist_stats.values()), 2),
                "mean": round(sum(hist_stats.values()) / len(hist_stats), 2),
            },
        }
        current_index += 1

    if (
        start_year < time_domains["projected"][1]
        and end_year > time_domains["projected"][0]
    ):
        # projected data present
        result["projected"] = {}
        ssp_names = ["ssp126", "ssp245", "ssp370", "ssp585"]
        proj_years = list(
            range(
                max(start_year, time_domains["projected"][0]),
                min(end_year, time_domains["projected"][1]) + 1,
            )
        )
        for ssp in ssp_names:
            proj_data = data[current_index]
            proj_stats = {
                str(year): convert(proj_data[i]) for i, year in enumerate(proj_years)
            }
            result["projected"][ssp] = {
                "data": proj_stats,
                "summary": {
                    "min": round(min(proj_stats.values()), 2),
                    "max": round(max(proj_stats.values()), 2),
                    "mean": round(sum(proj_stats.values()) / len(proj_stats), 2),
                },
            }
            current_index += 1

    return result


def postprocess_annual_rank(data, start_year, end_year, position, direction):
    """Postprocess annual rank data into structured dictionary output."""
    start_year = int(start_year)
    end_year = int(end_year)
    result = {}
    current_index = 0

    if (
        start_year < time_domains["historical"][1]
        and end_year > time_domains["historical"][0]
    ):
        # historical data present
        hist_data = data[current_index]
        hist_years = list(
            range(
                max(start_year, time_domains["historical"][0]),
                min(end_year, time_domains["historical"][1]) + 1,
            )
        )
        hist_ranks = {}
        for i, year in enumerate(hist_years):
            sorted_values = sorted(hist_data[i])
            if direction == "highest":
                rank_value = sorted_values[-position]
            else:
                rank_value = sorted_values[position - 1]
            hist_ranks[str(year)] = rank_value
        result["historical"] = {
            "data": hist_ranks,
            "summary": {
                "min": round(min(hist_ranks.values()), 2),
                "max": round(max(hist_ranks.values()), 2),
                "mean": round(sum(hist_ranks.values()) / len(hist_ranks), 2),
            },
        }

        current_index += 1

    if (
        start_year < time_domains["projected"][1]
        and end_year > time_domains["projected"][0]
    ):
        # projected data present
        result["projected"] = {}
        ssp_names = ["ssp126", "ssp245", "ssp370", "ssp585"]
        proj_years = list(
            range(
                max(start_year, time_domains["projected"][0]),
                min(end_year, time_domains["projected"][1]) + 1,
            )
        )
        for ssp in ssp_names:
            proj_data = data[current_index]
            proj_ranks = {}
            for i, year in enumerate(proj_years):
                sorted_values = sorted(proj_data[i])
                if direction == "highest":
                    rank_value = sorted_values[-position]
                else:
                    rank_value = sorted_values[position - 1]
                proj_ranks[str(year)] = rank_value
            result["projected"][ssp] = {
                "data": proj_ranks,
                "summary": {
                    "min": round(min(proj_ranks.values()), 2),
                    "max": round(max(proj_ranks.values()), 2),
                    "mean": round(sum(proj_ranks.values()) / len(proj_ranks), 2),
                },
            }
            current_index += 1

    return result


@routes.route("/dynamic_indicators/")
def dyanmic_indicators_about():
    return render_template("documentation/dynamic_indicators.html")


###### POINT QUERIES ######


@routes.route(
    "/dynamic_indicators/count_days/<operator>/<threshold>/<units>/<variable>/point/<lat>/<lon>/<start_year>/<end_year>/"
)
def count_days_point(
    operator, threshold, units, variable, lat, lon, start_year, end_year
):
    """Count the number of days above or below a threshold for a given variable and location over a specified year range.

    Example usage:
    - http://127.0.0.1:5000/dynamic_indicators/count_days/above/25/C/tasmax/point/64.5/-147.5/2000/2030/  ->>> can recreate the "summer days" indicator
    - http://127.0.0.1:5000/dynamic_indicators/count_days/below/-30/C/tasmin/point/64.5/-147.5/2000/2030/  ->>> can recreate the "deep winter days" indicator
    - http://127.0.0.1:5000/dynamic_indicators/count_days/above/10/mm/pr/point/64.5/-147.5/2000/2030/  ->>> can recreate the "days above 10mm precip" indicator
    - http://127.0.0.1:5000/dynamic_indicators/count_days/above/1/mm/pr/point/64.5/-147.5/2000/2030/  ->>> can recreate the "wet days" indicator
    """
    # Validate request params
    try:
        operator = validate_operator(operator)
        units, threshold = validate_units_threshold_and_variable(
            units=units, threshold=threshold, variable=variable
        )
        lon, lat = validate_latlon_and_reproject_to_epsg_3338(lat, lon, variable)
        validate_year(start_year, end_year)
    except:
        return render_template("400/bad_request.html"), 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    try:
        data = asyncio.run(
            fetch_count_days_data(
                var_coverages, year_ranges, threshold, operator, lon, lat
            )
        )
        result = postprocess_count_days(data, start_year, end_year)
        return result
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route(
    "/dynamic_indicators/stat/<stat>/<variable>/<units>/point/<lat>/<lon>/<start_year>/<end_year>/"
)
def get_annual_stat_point(stat, variable, units, lat, lon, start_year, end_year):
    """Get annual statistic (max, min, mean, sum) for a given variable and location over a specified year range.
    Example usage:
    - http://127.0.0.1:5000/dynamic_indicators/stat/max/pr/mm/point/64.5/-147.5/2000/2030   ->>> can recreate the "maxmimum one day precip" indicator
    - http://127.0.0.1:5000/dynamic_indicators/stat/min/tasmin/C/point/64.5/-147.5/2000/2030   ->>> coldest day per year
    - http://127.0.0.1:5000/dynamic_indicators/stat/max/tasmax/C/point/64.5/-147.5/2000/2030   ->>> hottest day per year
    - http://127.0.0.1:5000/dynamic_indicators/stat/sum/pr/mm/point/64.5/-147.5/2000/2030/  ->>> total annual precipitation (NOTE: summary section of return will show mean annual precip over the year range)
    - http://127.0.0.1:5000/dynamic_indicators/stat/mean/pr/mm/point/64.5/-147.5/2000/2030/  ->>> mean daily precipitation (NOTE: this is not a common mean statistic for precip - avg amount of precip per day over the year)
    """
    # Validate request params
    try:
        stat = validate_stat(stat)
        units, _threshold = validate_units_threshold_and_variable(
            units=units, threshold=None, variable=variable
        )
        lon, lat = validate_latlon_and_reproject_to_epsg_3338(lat, lon, variable)
        validate_year(start_year, end_year)
    except:
        return render_template("400/bad_request.html"), 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    try:
        data = asyncio.run(
            fetch_annual_stat_data(var_coverages, year_ranges, stat, lon, lat)
        )
        result = postprocess_annual_stat(data, start_year, end_year, units)
        return result
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route(
    "/dynamic_indicators/rank/<position>/<direction>/<variable>/point/<lat>/<lon>/<start_year>/<end_year>/"
)
def get_annual_rank_point(
    position, direction, variable, lat, lon, start_year, end_year
):
    """Get annual rank value (e.g., 6th highest, 10th lowest) for a given variable and location over a specified year range.
    Example usage:
    - http://127.0.0.1:5000/dynamic_indicators/rank/6/highest/tasmax/point/64.5/-147.5/2000/2030/  ->>> can recreate "hot day threshold" indicator
    - http://127.0.0.1:5000/dynamic_indicators/rank/6/lowest/tasmin/point/64.5/-147.5/2000/2030/  ->>> can recreate "cold day threshold" indicators
    """
    # Validate request params
    if variable not in ["tasmax", "tasmin", "pr"]:
        return render_template("400/bad_request.html"), 400
    try:
        position, direction = validate_rank_position_and_direction(position, direction)
        lon, lat = validate_latlon_and_reproject_to_epsg_3338(lat, lon, variable)
        validate_year(start_year, end_year)
    except:
        return render_template("400/bad_request.html"), 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    stat = ""  # NOTE: omitting stat will force a return of all values, which we need for ranking
    try:
        data = asyncio.run(
            fetch_annual_stat_data(var_coverages, year_ranges, stat, lon, lat)
        )
        result = postprocess_annual_rank(
            data, start_year, end_year, position, direction
        )
        return result
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


###### AREA QUERIES ######


@routes.route(
    "/dynamic_indicators/count_days/<operator>/<threshold>/<units>/<variable>/area/<place_id>/<start_year>/<end_year>/"
)
def count_days_area(
    operator, threshold, units, variable, place_id, start_year, end_year
):
    """Count the number of days above or below a threshold for a given variable and specified year range, and compute zonal mean over area.

    Example usage:
    - http://127.0.0.1:5000/dynamic_indicators/count_days/above/25/C/tasmax/area/1908030609/2000/2030/  ->>> can recreate the "summer days" indicator
    - http://127.0.0.1:5000/dynamic_indicators/count_days/below/-30/C/tasmin/area/1908030609/2000/2030/  ->>> can recreate the "deep winter days" indicator
    - http://127.0.0.1:5000/dynamic_indicators/count_days/above/10/mm/pr/area/1908030609/2000/2030/  ->>> can recreate the "days above 10mm precip" indicator
    - http://127.0.0.1:5000/dynamic_indicators/count_days/above/1/mm/pr/area/1908030609/2000/2030/  ->>> can recreate the "wet days" indicator
    """
    # Validate request params
    try:
        operator = validate_operator(operator)
        units, threshold = validate_units_threshold_and_variable(
            units=units, threshold=threshold, variable=variable
        )
        validate_year(start_year, end_year)
        polygon = validate_place_id(place_id)
    except:
        return render_template("400/bad_request.html"), 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    try:
        data = asyncio.run(
            fetch_count_days_area_data(
                variable, var_coverages, year_ranges, threshold, operator, polygon
            )
        )

        print(data)

        result = postprocess_count_days(data, start_year, end_year)
        return result
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route(
    "/dynamic_indicators/stat/<stat>/<variable>/<units>/area/<place_id>/<start_year>/<end_year>/"
)
def get_annual_stat_area(stat, variable, units, place_id, start_year, end_year):
    """Get annual statistic (max, min, mean, sum) for a given variable over a specified year range, and compute zonal mean over area.
    Example usage:
    - http://127.0.0.1:5000/dynamic_indicators/stat/max/pr/mm/area/1908030609/2000/2030   ->>> can recreate the "maxmimum one day precip" indicator
    - http://127.0.0.1:5000/dynamic_indicators/stat/min/tasmin/C/area/1908030609/2000/2030   ->>> coldest day per year
    - http://127.0.0.1:5000/dynamic_indicators/stat/max/tasmax/C/area/1908030609/2000/2030   ->>> hottest day per year
    - http://127.0.0.1:5000/dynamic_indicators/stat/sum/pr/mm/area/1908030609/2000/2030/  ->>> total annual precipitation (NOTE: summary section of return will show mean annual precip over the year range)
    - http://127.0.0.1:5000/dynamic_indicators/stat/mean/pr/mm/area/1908030609/2000/2030/  ->>> mean daily precipitation (NOTE: this is not a common mean statistic for precip - avg amount of precip per day over the year)
    """
    # Validate request params
    try:
        stat = validate_stat(stat)
        units, _threshold = validate_units_threshold_and_variable(
            units=units, threshold=None, variable=variable
        )
        validate_year(start_year, end_year)
        polygon = validate_place_id(place_id)
    except:
        return render_template("400/bad_request.html"), 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    try:
        data = asyncio.run(
            fetch_annual_stat_area_data(
                variable, var_coverages, year_ranges, stat, polygon
            )
        )
        result = postprocess_annual_stat(data, start_year, end_year, units)
        return result
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route(
    "/dynamic_indicators/rank/<position>/<direction>/<variable>/area/<place_id>/<start_year>/<end_year>/"
)
def get_annual_rank_area(position, direction, variable, place_id, start_year, end_year):
    """Get annual rank value (e.g., 6th highest, 10th lowest) for a given variable over a specified year range, and compute zonal mean over area.
    Example usage:
    - http://127.0.0.1:5000/dynamic_indicators/rank/6/highest/tasmax/area/1908030609/2000/2030/  ->>> can recreate "hot day threshold" indicator
    - http://127.0.0.1:5000/dynamic_indicators/rank/6/lowest/tasmin/area/1908030609/2000/2030/  ->>> can recreate "cold day threshold" indicators
    """
    # Validate request params
    if variable not in ["tasmax", "tasmin", "pr"]:
        return render_template("400/bad_request.html"), 400
    try:
        position, direction = validate_rank_position_and_direction(position, direction)
        validate_year(start_year, end_year)
        polygon = validate_place_id(place_id)
    except:
        return render_template("400/bad_request.html"), 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    # stat is empty to force return of all values for ranking
    stat = ""
    try:
        data = asyncio.run(
            fetch_annual_stat_area_data(
                variable, var_coverages, year_ranges, stat, polygon
            )
        )
        result = postprocess_annual_rank(
            data, start_year, end_year, position, direction
        )
        return result
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
