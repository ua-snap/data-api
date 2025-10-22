import asyncio
import logging
from flask import Blueprint, render_template, request

# local imports
from fetch_data import fetch_data
from generate_urls import generate_wcs_query_url
from generate_requests import (
    construct_count_annual_days_above_or_below_threshold_wcps_query_string,
    construct_get_annual_mmm_stat_wcps_query_string,
)
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    validate_year,
    project_latlon,
)

# TODO: for additional postprocessing or csv output, uncomment these imports and add code
# from postprocessing import postprocess, prune_nulls_with_max_intensity
# from csv_functions import create_csv

from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("dynamic_indicators_api", __name__)

all_coverages = {
    "pr": [
        "cmip6_downscaled_pr_5ModelAvg_historical_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp126_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp245_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp370_wcs",
        "cmip6_downscaled_pr_5ModelAvg_ssp585_wcs",
    ],
    "tasmin": [
        "cmip6_downscaled_tasmin_5ModelAvg_historical_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp126_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp245_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp370_wcs",
        "cmip6_downscaled_tasmin_5ModelAvg_ssp585_wcs",
    ],
    "tasmax": [
        "cmip6_downscaled_tasmax_5ModelAvg_historical_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp126_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp245_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp370_wcs",
        "cmip6_downscaled_tasmax_5ModelAvg_ssp585_wcs",
    ],
}

time_domains = {
    "historical": (
        1966,
        2014,
    ),  # actual data starts at 1965, but we are having an issue with noon time stamps at lower bound! Ask JP for more details
    "projected": (
        2016,
        2100,
    ),  # actual data starts at 2015, but we are having an issue with noon time stamps at lower bound! Ask JP for more details
}


def validate_latlon_and_reproject_to_epsg_3338(lat, lon):
    lat = float(lat)
    lon = float(lon)
    if not latlon_is_numeric_and_in_geodetic_range(lat, lon):
        raise ValueError("Latitude and/or longitude are out of range or not numeric.")

    # TODO: add step to validate geographic lat/lon with geotiff

    lon, lat = project_latlon(lat, lon, dst_crs=3338)

    return lon, lat


def validate_operator(operator):
    if operator not in ["above", "below"]:
        raise ValueError("Operator must be 'above' or 'below'.")
    if operator == "above":
        operator = ">"
    else:
        operator = "<"
    return operator


def validate_units_threshold_and_variable(units, threshold, variable):
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
            raise ValueError("Units for precipitation must be 'mm' or 'in'.")
        if threshold is not None:
            if units == "in":
                threshold = float(threshold) * 25.4
            else:
                threshold = float(threshold)
    else:
        raise ValueError("Variable must be 'tasmax', 'tasmin', or 'pr'.")

    # TODO: validate threshold ranges based on variable if needed
    # ie, temperature thresholds within reasonable limits, no negative precip allowed, etc.

    return units, threshold


def validate_stat(stat):
    if stat not in ["max", "min", "mean", "sum"]:
        raise ValueError("Stat must be 'max', 'min', 'mean', or 'sum'.")
    if stat == "mean":
        stat = "avg"  # rasdaman uses 'avg' instead of 'mean' in WCPS queries
    return stat


def validate_rank_position_and_direction(position, direction):
    # position must be between 1 and 365, and direction must be "highest" or "lowest"
    try:
        position = int(position)
        if position < 1 or position > 365:
            raise ValueError
    except ValueError:
        raise ValueError("Position must be an integer between 1 and 365.")
    if direction not in ["highest", "lowest"]:
        raise ValueError("Direction must be 'highest' or 'lowest'.")
    return position, direction


def build_year_and_coverage_lists_for_iteration(
    start_year, end_year, variable, time_domains, all_coverages
):
    # if years span historical and projected, need to split into two ranges
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


async def fetch_count_days_data(
    var_coverages, year_ranges, threshold, operator, lon, lat
):
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


def postprocess_count_days(data, start_year, end_year):
    # if the year range spans historical and projected, our data will be a list of 5 lists:
    # the first has day counts for each historical year, and the rest have day counts for each projected year for each SSP
    # if the year range is only historical or only projected, our data will be a list of 1-4 lists accordingly

    # we want to create dictionary for output, with the following structure:
    # {
    #     "historical": {
    #         "data": {"2000": 45, "2001": 50, ...},
    #         "summary": {"min": 30, "max": 60, "mean": 45.5},
    #     },
    #     "projected": {
    #         "ssp126": {
    #             "data": {"2020": 55, "2021": 60, ...},
    #             "summary": {"min": 40, "max": 70, "mean": 55.5},
    #         },
    #         "ssp245": {
    #             "data": {"2020": 50, "2021": 55, ...},
    #             "summary": {"min": 35, "max": 65, "mean": 50.5},
    #         },
    #         ...
    #     }
    # }

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
                "min": min(hist_day_counts.values()),
                "max": max(hist_day_counts.values()),
                "mean": sum(hist_day_counts.values()) / len(hist_day_counts),
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
                    "min": min(proj_day_counts.values()),
                    "max": max(proj_day_counts.values()),
                    "mean": sum(proj_day_counts.values()) / len(proj_day_counts),
                },
            }
            current_index += 1
    return result


async def fetch_annual_stat_data(var_coverages, year_ranges, stat, lon, lat):
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


def postprocess_annual_stat(data, start_year, end_year, units):
    # data in / out is similar to postprocess_count_days
    # define conversion functions
    def mm_to_inches(mm):
        return mm / 25.4

    def c_to_f(c):
        return (c * 9 / 5) + 32

    # Determine if and how to convert
    if units == "in":
        convert = mm_to_inches
    elif units == "F":
        convert = c_to_f
    else:
        convert = lambda x: x  # No conversion

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
                "min": min(hist_stats.values()),
                "max": max(hist_stats.values()),
                "mean": sum(hist_stats.values()) / len(hist_stats),
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
                    "min": min(proj_stats.values()),
                    "max": max(proj_stats.values()),
                    "mean": sum(proj_stats.values()) / len(proj_stats),
                },
            }
            current_index += 1

    return result


def postprocess_annual_rank(data, start_year, end_year, position, direction):
    # data in / out is similar to postprocess_annual_stat
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
                "min": min(hist_ranks.values()),
                "max": max(hist_ranks.values()),
                "mean": sum(hist_ranks.values()) / len(hist_ranks),
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
                    "min": min(proj_ranks.values()),
                    "max": max(proj_ranks.values()),
                    "mean": sum(proj_ranks.values()) / len(proj_ranks),
                },
            }
            current_index += 1

    return result


@routes.route(
    "/dynamic_indicators/count_days/<operator>/<threshold>/<units>/<variable>/<lat>/<lon>/<start_year>/<end_year>/"
)
def count_days(operator, threshold, units, variable, lat, lon, start_year, end_year):
    # example usage:
    # http://127.0.0.1:5000/dynamic_indicators/count_days/above/25/C/tasmax/64.5/-147.5/2000/2030/  ->>> can recreate the "summer days" indicator
    # http://127.0.0.1:5000/dynamic_indicators/count_days/below/-30/C/tasmax/64.5/-147.5/2000/2030/  ->>> can recreate the "deep winter days" indicator
    # http://127.0.0.1:5000/dynamic_indicators/count_days/above/10/mm/pr/64.5/-147.5/2000/2030/  ->>> can recreate the "days above 10mm precip" indicator
    # http://127.0.0.1:5000/dynamic_indicators/count_days/above/1/mm/pr/64.5/-147.5/2000/2030/  ->>> can recreate the "wet days" indicator

    # Validate request params
    try:
        lon, lat = validate_latlon_and_reproject_to_epsg_3338(lat, lon)
        operator = validate_operator(operator)
        units, threshold = validate_units_threshold_and_variable(
            units=units, threshold=threshold, variable=variable
        )
        validate_year(start_year, end_year)
    except ValueError as e:
        return {"error": str(e)}, 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    data = asyncio.run(
        fetch_count_days_data(var_coverages, year_ranges, threshold, operator, lon, lat)
    )

    result = postprocess_count_days(data, start_year, end_year)

    return result


@routes.route(
    "/dynamic_indicators/stat/<stat>/<variable>/<units>/<lat>/<lon>/<start_year>/<end_year>/"
)
def get_annual_stat(stat, variable, units, lat, lon, start_year, end_year):
    # example usage:
    # http://127.0.0.1:5000/dynamic_indicators/stat/max/pr/mm/64.5/-147.5/2000/2030   ->>> can recreate the "maxmimum one day precip" indicator
    # http://127.0.0.1:5000/dynamic_indicators/stat/min/tasmin/C/64.5/-147.5/2000/2030   ->>> coldest day per year
    # http://127.0.0.1:5000/dynamic_indicators/stat/max/tasmax/C/64.5/-147.5/2000/2030   ->>> hottest day per year
    # http://127.0.0.1:5000/dynamic_indicators/stat/sum/pr/mm/64.5/-147.5/2000/2030/  ->>> total annual precipitation (NOTE: summary section of return will show mean annual precip over the year range)
    # http://127.0.0.1:5000/dynamic_indicators/stat/mean/pr/mm/64.5/-147.5/2000/2030/  ->>> mean daily precipitation (NOTE: this is not a common mean statistic for precip - avg amount of precip per day over the year)

    # Validate request params
    try:
        stat = validate_stat(stat)
        lon, lat = validate_latlon_and_reproject_to_epsg_3338(lat, lon)
        units, _threshold = validate_units_threshold_and_variable(
            units=units, threshold=None, variable=variable
        )
        validate_year(start_year, end_year)
    except ValueError as e:
        return {"error": str(e)}, 400

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    data = asyncio.run(
        fetch_annual_stat_data(var_coverages, year_ranges, stat, lon, lat)
    )

    result = postprocess_annual_stat(data, start_year, end_year, units)

    return result


@routes.route(
    "/dynamic_indicators/rank/<position>/<direction>/<variable>/<lat>/<lon>/<start_year>/<end_year>/"
)
def get_annual_rank(position, direction, variable, lat, lon, start_year, end_year):
    # example usage:
    # http://127.0.0.1:5000/dynamic_indicators/rank/6/highest/tasmax/64.5/-147.5/2000/2030/  ->>> can recreate "hot day threshold" indicator
    # http://127.0.0.1:5000/dynamic_indicators/rank/6/lowest/tasmin/64.5/-147.5/2000/2030/  ->>> can recreate "cold day threshold" indicators

    # Validate request params
    try:
        position, direction = validate_rank_position_and_direction(position, direction)
        lon, lat = validate_latlon_and_reproject_to_epsg_3338(lat, lon)
        validate_year(start_year, end_year)
    except ValueError as e:
        return {"error": str(e)}, 400
    if variable not in ["tasmax", "tasmin", "pr"]:
        raise ValueError("Variable must be 'tasmax', 'tasmin', or 'pr'.")

    # build lists for iteration
    year_ranges, var_coverages = build_year_and_coverage_lists_for_iteration(
        int(start_year), int(end_year), variable, time_domains, all_coverages
    )

    stat = (
        ""  # omitting stat will force a return of all values, which we need for ranking
    )

    data = asyncio.run(
        fetch_annual_stat_data(var_coverages, year_ranges, stat, lon, lat)
    )

    result = postprocess_annual_rank(data, start_year, end_year, position, direction)

    return result
