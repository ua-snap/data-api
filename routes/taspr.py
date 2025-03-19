import asyncio
import io
import time
import itertools
from urllib.parse import quote
import numpy as np
import pandas as pd
import xarray as xr
from math import floor
from flask import (
    Blueprint,
    render_template,
    request,
    current_app as app,
    jsonify,
)

# local imports
from generate_requests import generate_wcs_getcov_str, generate_mmm_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_data,
    fetch_wcs_point_data,
    get_from_dict,
    interpolate_and_compute_zonal_stats,
    summarize_within_poly,
    get_poly,
    generate_nested_dict,
)
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
    validate_year,
)
from postprocessing import (
    nullify_and_prune,
    postprocess,
)
from csv_functions import create_csv
from config import WEST_BBOX, EAST_BBOX
from . import routes

taspr_api = Blueprint("taspr_api", __name__)

# Dimensions for January and July min, mean, max temperature statistics
# Table generated from luts.py file found here:
# https://github.com/ua-snap/rasdaman-ingest/blob/main/arctic_eds/jan_july_tas_stats/jan_min_mean_max_tas/luts.py
mmm_dim_encodings = {
    "tempstats": {
        0: "tasmax",
        1: "tasmean",
        2: "tasmin",
    },
    "models": {
        2: "GFDL-CM3",
        3: "GISS-E2-R",
        4: "IPSL-CM5A-LR",
        5: "MRI-CGCM3",
        6: "NCAR-CCSM4",
    },
    "scenarios": {
        1: "rcp45",
        2: "rcp60",
        3: "rcp85",
    },
    "months": {"jan": "January", "july": "July"},
}

tas_2km_dim_encodings = {
    "tempstats": {
        0: "tasmean",
        1: "tasmax",
        2: "tasmin",
    },
    "models": {
        0: "5ModelAvg",
        1: "GFDL-CM3",
        2: "NCAR-CCSM4",
    },
    "scenarios": {0: "rcp45", 1: "rcp85"},
    "months": {
        0: "January",
        1: "February",
        2: "March",
        3: "April",
        4: "May",
        5: "June",
        6: "July",
        7: "August",
        8: "September",
        9: "October",
        10: "November",
        11: "December",
    },
}

dot_dim_encodings = {
    "durations": {
        0: "10d",
        1: "12h",
        2: "20d",
        3: "24h",
        4: "2d",
        5: "2h",
        6: "30d",
        7: "3d",
        8: "3h",
        9: "45d",
        10: "4d",
        11: "60d",
        12: "60m",
        13: "6h",
        14: "7d",
    },
    "models": {0: "GFDL-CM3", 1: "NCAR-CCSM4"},
    "eras": {0: "2020-2049", 1: "2050-2079", 2: "2080-2099"},
    "intervals": {
        0: "2",
        1: "5",
        2: "10",
        3: "25",
        4: "50",
        5: "100",
        6: "200",
        7: "500",
        8: "1000",
    },
}

dot_precip_coverage_id = "dot_precip"


# encodings hardcoded for now
# fmt: off
# lookup tables derived from the IEM rasdaman ingest luts.py
# pay attention to any changes with ingest and change here as needed
dim_encodings = {
    "varname": {
        0: "pr",
        1: "tas",
    },
    "decade": {
        0: "2010_2019",
        1: "2020_2029",
        2: "2030_2039",
        3: "2040_2049",
        4: "2050_2059",
        5: "2060_2069",
        6: "2070_2079",
        7: "2080_2089",
        8: "2090_2099",
    },
    "model": {
        0: "5modelAvg",
        1: "CCSM4",
        2: "MRI-CGCM3",
    },
    "scenario": {
        0: "rcp45",
        1: "rcp60",
        2: "rcp85",
    },
    "season": {
        0: "DJF",
        1: "JJA",
        2: "MAM",
        3: "SON",
    },
    "stat": {
        0: "hi_std",
        1: "lo_std",
        2: "max",
        3: "mean",
        4: "median",
        5: "min",
        6: "q1",
        7: "q3",
    },
    "rounding": {
        "tas": 1,
        "pr": 0,
    },
}
# fmt: on

var_ep_lu = {
    "temperature": "tas",
    "precipitation": "pr",
}


def make_fetch_args():
    """Fixed helper function for ensuring
    consistency between point and HUC queries
    """
    cov_ids = [
        "iem_cru_2km_taspr_seasonal_baseline_stats",
        "iem_ar5_2km_taspr_seasonal",
        "iem_ar5_2km_taspr_seasonal",
        "iem_ar5_2km_taspr_seasonal",
    ]
    summary_decades = [None, (3, 5), (6, 8), None]

    return cov_ids, summary_decades


def get_wcps_request_str(x, y, var_coord, cov_id, summary_decades, encoding="json"):
    """Generates a WCPS query specific to the
    coverages used in the endpoints herein. The only
    axis we are currently averaging over is "decade", so
    this function creates a WCPS query from integer
    values corresponding to decades to summarize over.

    Args:
        x (float or str): x-coordinate for point query, or string
            composed as "x1:x2" for bbox query, where x1 and x2 are
            lower and upper bounds of bbox
        y (float or str): y-coordinate for point query, or string
            composed as "y1:y2" for bbox query, where y1 and y2 are
            lower and upper bounds of bbox
        var_coord (int): coordinate value corresponding to varname to query
        cov_id (str): Rasdaman coverage ID
        summary_decades (tuple): 2-tuple of integers mapped to
            desired range of decades to summarise over,
            e.g. (6, 8) for 2070-2099
        encoding (str): currently supports either "json" or "netcdf"
            for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """
    d1, d2 = summary_decades
    n = len(np.arange(d1, d2 + 1))
    wcps_request_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({cov_id}) "
            f"let $a := (condense + over $t decade({d1}:{d2}) "
            f"using $c[decade($t),X({x}),Y({y}),varname({var_coord})] ) / {n} "
            f'return encode( $a , "application/{encoding}")'
        )
    )

    return wcps_request_str


def get_mmm_wcps_request_str(
    x, y, cov_id, scenarios, models, years, tempstat, encoding="json"
):
    """Generates a WCPS query specific to the
    coverages used for the temperature min-mean-max.

    Args:
        x (float or str): x-coordinate for point query, or string
            composed as "x1:x2" for bbox query, where x1 and x2 are
            lower and upper bounds of bbox
        y (float or str): y-coordinate for point query, or string
            composed as "y1:y2" for bbox query, where y1 and y2 are
            lower and upper bounds of bbox
        cov_id (str): Rasdaman coverage ID
        scenarios (str): Comma-separated numbers of requested scenarios
        models (str): Comma-separated numbers of requested models
        years (str): Colon-separated full date-time i.e.
            "\"2006-01-01T00:00:00.000Z\":\"2100-01-01T00:00:00.000Z\""
        tempstat(int): Integer between 0-2 where:
            - 0 = tasmax
            - 1 = tasmean
            - 2 = tasmin
        encoding (str): currently supports either "json" or "netcdf"
            for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """

    if tempstat == 0:
        operation = "max"
    elif tempstat == 2:
        operation = "min"
    else:
        operation = "+"

    variable = ""
    if cov_id not in ("annual_precip_totals_mm", "annual_mean_temp"):
        variable = f",tempstat({tempstat})"

    if tempstat == 0 or tempstat == 2:
        wcps_request_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := {operation}(condense {operation} over $s scenario({scenarios}), $m model({models}) "
                f"using $c[scenario($s),model($m),year({years}),X({x}),Y({y}){variable}] ) "
                f'return encode( $a, "application/{encoding}")'
            )
        )
        return wcps_request_str
    else:
        # Generates the mean across models and scenarios on the tasmean variable

        # For projected, 5 models * 3 scenarios
        num_results = 15

        # For historical, only a single model + scenario
        if scenarios == "0:0":
            num_results = 1

        wcps_request_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := avg(condense {operation} over $s scenario({scenarios}), $m model({models}) "
                f"using $c[scenario($s),model($m),year({years}),X({x}),Y({y}){variable}] / {num_results} ) "
                f'return encode( $a, "application/{encoding}")'
            )
        )
        return wcps_request_str


async def fetch_mmm_point_data(x, y, cov_id, start_year, end_year):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_id (str): Rasdaman coverage ID string

    Returns:
        list of data results from each cov_id for CRU TS 4.0 and all 5 projected
        models for a given coordinate
    """
    point_data_list = []
    if request.args.get("summarize") == "mmm":
        timestring = '"1901-01-01T00:00:00.000Z":"2015-01-01T00:00:00.000Z"'
        if start_year is not None:
            timestring = (
                f'"{start_year}-01-01T00:00:00.000Z":"{end_year}-01-01T00:00:00.000Z"'
            )

        # Generates URL for historical scenario of CRU TS 4.0
        for tempstat in range(0, 3):
            request_str = get_mmm_wcps_request_str(
                x, y, cov_id, "0:0", "0:0", timestring, tempstat
            )
            point_data_list.append(
                await fetch_data([generate_wcs_query_url(request_str)])
            )

        timestring = '"2006-01-01T00:00:00.000Z":"2100-01-01T00:00:00.000Z"'
        if start_year is not None:
            timestring = (
                f'"{start_year}-01-01T00:00:00.000Z":"{end_year}-01-01T00:00:00.000Z"'
            )
        # All other models and scenarios captured in this loop
        for tempstat in range(0, 3):
            request_str = get_mmm_wcps_request_str(
                x, y, cov_id, "1:3", "2:6", timestring, tempstat
            )
            point_data_list.append(
                await fetch_data([generate_wcs_query_url(request_str)])
            )
    else:
        request_str = generate_mmm_wcs_getcov_str(x, y, cov_id, "0,6", "0,3")
        point_data_list = await fetch_data([generate_wcs_query_url(request_str)])

    return point_data_list


async def fetch_tas_2km_mmm_point_data(x, y, month, summary_years):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        month (int): month to summarize over, one of 0 - 11
        summary_years (tuple): 2-tuple of integers mapped to
            desired range of years to summarise over,
            e.g. (0, 10) for 2006-2016

    Returns:
        list of averaged mmm data results
    """
    point_data_list = await fetch_data(
        [
            generate_wcs_query_url(
                get_tas_2km_wcps_request_str(x, y, month, summary_years)
            )
        ]
    )

    return point_data_list


async def fetch_point_data(x, y, var_coord, cov_ids, summary_decades):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        var_coord (str): coordinate value corresponding to varname
            to query, one of 0 or 1
        cov_ids (list): Rasdaman coverage ids
        summary_decades (tuple): 2-tuple of integers mapped to
            desired range of decades to summarise over,
            e.g. (6, 8) for 2070-2099

    Returns:
        list of data results from each cov_id/summary_decades
        pairing
    """
    urls = []
    for cov_id, decade_tpl in zip(cov_ids, summary_decades):
        if decade_tpl:
            # if summary decades are given, create a WCPS request string
            request_str = get_wcps_request_str(x, y, var_coord, cov_id, decade_tpl)
        else:
            # otheriwse use generic WCS request str
            request_str = generate_wcs_getcov_str(x, y, cov_id, var_coord)
        urls.append(generate_wcs_query_url(request_str))
    point_data_list = await fetch_data(urls)

    return point_data_list


def package_mmm_point_data(point_data, cov_id, varname, start_year=None, end_year=None):
    """Packages min-mean-max point data into JSON-formatted return data

    Args:
        point_data (list): Nested list of returned data from Rasdaman
            * point_data is a four-dimensional list with the following indices:
                - point_data[i] = variable (tasmax, tasmean, and tasmin)
                - point_data[i][j] = year starting from 1900-200 [0-199]
                - point_data[i][j][k] = model number between 0-6 in mmm_dim_encodings
                - point_data[i][j][k][l] = scenario number between 0-3 in mmm_dim_encodings
        varname (str): variable name to fetch point data
            for one of "tas" or "pr"

    Returns:
        Python dictionary of either all data or summary if ?summarize=mmm is set.
    """
    point_pkg = dict()
    if request.args.get("summarize") == "mmm":
        # Generate the min, mean and max for historical CRU-TS 4.0 data at this point
        # We only want to generate statistics from the years 1900-2015 as that's all
        # that is available in CRU-TS 4.0
        historical_max = round(point_data[0], dim_encodings["rounding"][varname])
        historical_mean = round(point_data[1], dim_encodings["rounding"][varname])
        historical_min = round(point_data[2], dim_encodings["rounding"][varname])

        point_pkg["historical"] = dict()

        if cov_id == "annual_precip_totals_mm":
            point_pkg["historical"]["prmin"] = historical_min
            point_pkg["historical"]["prmean"] = historical_mean
            point_pkg["historical"]["prmax"] = historical_max
        else:
            point_pkg["historical"]["tasmin"] = historical_min
            point_pkg["historical"]["tasmean"] = historical_mean
            point_pkg["historical"]["tasmax"] = historical_max

        projected_max = round(point_data[3], dim_encodings["rounding"][varname])
        projected_mean = round(point_data[4], dim_encodings["rounding"][varname])
        projected_min = round(point_data[5], dim_encodings["rounding"][varname])

        point_pkg["projected"] = dict()

        if cov_id == "annual_precip_totals_mm":
            point_pkg["projected"]["prmin"] = projected_min
            point_pkg["projected"]["prmean"] = projected_mean
            point_pkg["projected"]["prmax"] = projected_max
        else:
            point_pkg["projected"]["tasmin"] = projected_min
            point_pkg["projected"]["tasmean"] = projected_mean
            point_pkg["projected"]["tasmax"] = projected_max
    else:
        ### HISTORICAL CRU-TS 4.0 ###
        # Puts together the historical tasmin, tasmean, and tasmax for all 200 years
        point_pkg["CRU-TS"] = dict()
        point_pkg["CRU-TS"]["historical"] = dict()

        for year_offset in range(0, 115):
            year = year_offset + 1901
            if None not in [start_year, end_year]:
                if year < int(start_year) or year > int(end_year):
                    continue
            point_pkg["CRU-TS"]["historical"][str(year)] = dict()
            if cov_id == "annual_precip_totals_mm":
                point_pkg["CRU-TS"]["historical"][str(year)]["pr"] = point_data[0][
                    year_offset
                ][0]
            elif cov_id == "annual_mean_temp":
                point_pkg["CRU-TS"]["historical"][str(year)]["tas"] = point_data[0][
                    year_offset
                ][0]
            else:
                point_pkg["CRU-TS"]["historical"][str(year)]["tasmax"] = point_data[0][
                    year_offset
                ][0][0]
                point_pkg["CRU-TS"]["historical"][str(year)]["tasmean"] = point_data[1][
                    year_offset
                ][0][0]
                point_pkg["CRU-TS"]["historical"][str(year)]["tasmin"] = point_data[2][
                    year_offset
                ][0][0]

        ### PROJECTED FUTURE MODELS ###
        # For all models, scenarios, and variables found in mmm_dim_encodings dictionary

        for model in mmm_dim_encodings["models"].keys():
            dim_model = mmm_dim_encodings["models"][model]
            point_pkg[dim_model] = dict()
            for scenario in mmm_dim_encodings["scenarios"].keys():
                dim_scenario = mmm_dim_encodings["scenarios"][scenario]
                point_pkg[dim_model][dim_scenario] = dict()
                for year_offset in range(106, 200):
                    year = year_offset + 1901
                    if None not in [start_year, end_year]:
                        if year < int(start_year) or year > int(end_year):
                            continue
                    point_pkg[dim_model][dim_scenario][str(year)] = dict()
                    if cov_id == "annual_precip_totals_mm":
                        point_pkg[dim_model][dim_scenario][str(year)]["pr"] = (
                            point_data[model][year_offset][scenario]
                        )
                    elif cov_id == "annual_mean_temp":
                        point_pkg[dim_model][dim_scenario][str(year)]["tas"] = (
                            point_data[model][year_offset][scenario]
                        )
                    else:
                        for variable in mmm_dim_encodings["tempstats"].keys():
                            dim_variable = mmm_dim_encodings["tempstats"][variable]
                            point_pkg[dim_model][dim_scenario][str(year)][
                                dim_variable
                            ] = point_data[variable][year_offset][model][scenario]

    return point_pkg


def package_tas_2km_point_data(point_data):
    """Add dim names to JSON response from tas 2km point query

    Args:
        point_data (list): nested list containing JSON
            results of tas 2km point query

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = dict()
    point_data_pkg["historical"] = dict()
    point_data_pkg["historical"]["CRU-TS"] = dict()
    point_data_pkg["historical"]["CRU-TS"]["historical"] = dict()
    point_data_pkg["projected"] = dict()

    for month_idx, month_li in enumerate(point_data[0]):
        month = tas_2km_dim_encodings["months"][month_idx]
        point_data_pkg["historical"]["CRU-TS"]["historical"][month] = dict()
        for year_idx, year_li in enumerate(month_li):
            # First year is 1901, so add 1901 to the index
            year = year_idx + 1901
            var_values = year_li.split(" ")
            point_data_pkg["historical"]["CRU-TS"]["historical"][month][year] = {
                "tasmean": var_values[0],
                "tasmax": var_values[1],
                "tasmin": var_values[2],
            }
    for model_idx, model_li in enumerate(point_data[1]):
        model = tas_2km_dim_encodings["models"][model_idx]
        point_data_pkg["projected"][model] = dict()
        for scenario_idx, scenario_li in enumerate(model_li):
            scenario = tas_2km_dim_encodings["scenarios"][scenario_idx]
            point_data_pkg["projected"][model][scenario] = dict()
            for month_idx, month_li in enumerate(scenario_li):
                month = tas_2km_dim_encodings["months"][month_idx]
                point_data_pkg["projected"][model][scenario][month] = dict()
                for year_idx, year_li in enumerate(month_li):
                    # First year is 2006, so add 2006 to the index
                    year = year_idx + 2006
                    var_values = year_li.split(" ")
                    point_data_pkg["projected"][model][scenario][month][year] = {
                        "tasmean": var_values[0],
                        "tasmax": var_values[1],
                        "tasmin": var_values[2],
                    }

    return point_data_pkg


def package_cru_point_data(point_data, varname):
    """Add dim names to JSON response from point query
    for the CRU TS historical basline coverage

    Args:
        point_data (list): nested list containing JSON
            results of CRU point query
        varname (str): variable name to fetch point data
            for one of "tas" or "pr"

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = {}
    # hard-code summary period for CRU
    for si, s_li in enumerate(point_data):  # (nested list with varname at dim 0)
        season = dim_encodings["season"][si]
        model = "CRU-TS40"
        scenario = "CRU_historical"
        point_data_pkg[season] = {model: {scenario: {varname: {}}}}
        for si, value in enumerate(s_li):  # (nested list with statistic at dim 0)
            stat = dim_encodings["stat"][si]
            if value is None:
                point_data_pkg[season][model][scenario][varname][stat] = None
            else:
                point_data_pkg[season][model][scenario][varname][stat] = round(
                    value, dim_encodings["rounding"][varname]
                )

    return point_data_pkg


def package_ar5_point_data(point_data, varname):
    """Add dim names to JSON response from AR5 point query

    Args:
        point_data (list): nested list containing JSON
            results of AR5 or CRU point query
        varname (str): name of variable, either "tas" or "pr"

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = {}
    # AR5 data:
    # varname, decade, month, model, scenario
    #   Since we are relying on some hardcoded mappings between
    # integers and the dataset dimensions, we should consider
    # having that mapping tracked somewhere such that it is
    # imported to help prevent breakage.
    for di, m_li in enumerate(point_data):  # (nested list with month at dim 0)
        decade = dim_encodings["decade"][di]
        point_data_pkg[decade] = {}
        for ai, mod_li in enumerate(m_li):  # (nested list with model at dim 0)
            season = dim_encodings["season"][ai]
            point_data_pkg[decade][season] = {}
            for mod_i, s_li in enumerate(
                mod_li
            ):  # (nested list with scenario at dim 0)
                model = dim_encodings["model"][mod_i]
                point_data_pkg[decade][season][model] = {}
                for si, value in enumerate(s_li):  # (nested list with varname at dim 0)
                    scenario = dim_encodings["scenario"][si]
                    point_data_pkg[decade][season][model][scenario] = {
                        varname: (
                            None
                            if value is None
                            else round(value, dim_encodings["rounding"][varname])
                        )
                    }

    return point_data_pkg


def package_ar5_point_summary(point_data, varname):
    """Add dim names to JSON response from point query
    for the AR5 coverages

    Args:
        point_data (list): nested list containing JSON
            results of AR5 or CRU point query
        varname (str): name of variable, either "tas" or "pr"

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = {}
    for si, mod_li in enumerate(point_data):  # (nested list with model at dim 0)
        season = dim_encodings["season"][si]
        point_data_pkg[season] = {}
        for mod_i, s_li in enumerate(mod_li):  # (nested list with scenario at dim 0)
            model = dim_encodings["model"][mod_i]
            point_data_pkg[season][model] = {}
            for si, value in enumerate(s_li):  # (nested list with varname at dim 0)
                scenario = dim_encodings["scenario"][si]
                point_data_pkg[season][model][scenario] = {
                    varname: (
                        None
                        if value is None
                        else round(value, dim_encodings["rounding"][varname])
                    )
                }

    return point_data_pkg


def create_temperature_eds_summary(temp_json):
    hist_df = pd.DataFrame.from_dict(
        temp_json["historical"]["CRU-TS"]["historical"], orient="index"
    )
    hist_monthly_mmm = pd.DataFrame(columns=["Month", "tasmean", "tasmax", "tasmin"])

    all_monthly_means = list()

    # Iterate through all months
    for month in hist_df.index:
        # Extract 'tasmean', 'tasmax', and 'tasmin' values if they exist, or set to NaN if missing
        try:
            monthly_data = hist_df.loc[month]

            # Initialize lists to store 'tasmean', 'tasmax', and 'tasmin' values for each year in the month
            monthly_mean_values = list()
            monthly_max_values = list()
            monthly_min_values = list()

            # Iterate through the year data within the month
            for year, data in monthly_data.items():
                if "tasmean" in data:
                    monthly_mean_values.append(pd.to_numeric(data["tasmean"]))
                if "tasmax" in data:
                    monthly_max_values.append(pd.to_numeric(data["tasmax"]))
                if "tasmin" in data:
                    monthly_min_values.append(pd.to_numeric(data["tasmin"]))

            # Calculate the mean of 'tasmean', the maximum of 'tasmax', and the minimum of 'tasmin' for the month
            if monthly_mean_values:
                monthly_mean = round(
                    sum(monthly_mean_values) / len(monthly_mean_values), 1
                )
            else:
                monthly_mean = None

            if monthly_max_values:
                monthly_max = max(monthly_max_values)
            else:
                monthly_max = None

            if monthly_min_values:
                monthly_min = min(monthly_min_values)
            else:
                monthly_min = None

        except KeyError:
            monthly_mean = None
            monthly_max = None
            monthly_min = None

        # Append the results to the hist_monthly_mmm DataFrame
        row_to_append = pd.DataFrame(
            {
                "Month": [month],
                "tasmean": [monthly_mean],
                "tasmax": [monthly_max],
                "tasmin": [monthly_min],
            }
        )
        hist_monthly_mmm = pd.concat(
            [hist_monthly_mmm, row_to_append], ignore_index=True
        )

        # Append the monthly mean values to generate all years worth of
        # monthly values
        all_monthly_means.extend(monthly_mean_values)

    # Calculate the annual mean across all of the monthly means
    annual_mean = round(sum(all_monthly_means) / len(all_monthly_means), 1)

    # Generate annual means from the monthly means
    annual_means = []

    # There are 115 values (1901-2015) for each month
    for year in range(0, 115):
        yearly_values = []
        for month in range(0, 12):
            # Index is how the data is ordered from the above code
            # Each month has 115 years + the year we are currently on
            index = (month * 115) + year
            yearly_values.append(all_monthly_means[index])
        # For this year, generate the annual mean
        annual_means.append(round(sum(yearly_values) / len(yearly_values), 1))
    # Find the minimum and maximum annual mean from all the historical years
    annual_max = max(annual_means)
    annual_min = min(annual_means)

    row_to_append = pd.DataFrame(
        {
            "Month": ["Annual"],
            "tasmean": [annual_mean],
            "tasmax": [annual_max],
            "tasmin": [annual_min],
        }
    )
    hist_monthly_mmm = pd.concat([hist_monthly_mmm, row_to_append], ignore_index=True)

    eras_of_interest = [
        [year for year in range(2010, 2040)],
        [year for year in range(2040, 2070)],
        [year for year in range(2070, 2100)],
    ]
    model_options = ["5ModelAvg", "GFDL-CM3", "NCAR-CCSM4"]
    rcp_options = ["rcp45", "rcp85"]

    # Initialize DataFrames to store the results
    projected_monthly_mmm = pd.DataFrame(
        columns=["Month", "tasmean", "tasmax", "tasmin"]
    )

    # Iterate through sub-sections and rcp options
    for years_of_interest in eras_of_interest:
        for model_option in model_options:
            for rcp_option in rcp_options:
                # Extract the relevant data for the current combination
                projected_data = temp_json["projected"][model_option][rcp_option]

                # Initialize list to store
                all_monthly_mean_values = list()

                # Iterate through all months
                for month, year_data in projected_data.items():
                    # Initialize lists to store 'tasmean', 'tasmax', and 'tasmin' values for the years of interest
                    monthly_mean_values = list()
                    monthly_max_values = list()
                    monthly_min_values = list()

                    # Iterate through the year data within the month
                    for year, data in year_data.items():
                        if year in years_of_interest:
                            if "tasmean" in data:
                                monthly_mean_values.append(
                                    pd.to_numeric(data["tasmean"])
                                )
                            if "tasmax" in data:
                                monthly_max_values.append(pd.to_numeric(data["tasmax"]))
                            if "tasmin" in data:
                                monthly_min_values.append(pd.to_numeric(data["tasmin"]))

                    # Only collect the monthly mean values for the 5 Model Average
                    # and on the scenario RCP 8.5
                    if model_option == "5ModelAvg" and rcp_option == "rcp85":
                        # Append the monthly values to the list of annual values
                        all_monthly_mean_values.extend(monthly_mean_values)

                    if monthly_mean_values:
                        monthly_mean = round(
                            sum(monthly_mean_values) / len(monthly_mean_values), 1
                        )
                    else:
                        monthly_mean = None

                    if monthly_max_values:
                        monthly_max = max(monthly_mean_values)
                    else:
                        monthly_max = None

                    if monthly_min_values:
                        monthly_min = min(monthly_mean_values)
                    else:
                        monthly_min = None

                    combination_label = f"{model_option}_{rcp_option}_{month}_{years_of_interest[0]}_{years_of_interest[-1]}"
                    row_to_append = pd.DataFrame(
                        {
                            "Month": [combination_label],
                            "tasmean": [monthly_mean],
                            "tasmax": [monthly_max],
                            "tasmin": [monthly_min],
                        }
                    )
                    projected_monthly_mmm = pd.concat(
                        [projected_monthly_mmm, row_to_append], ignore_index=True
                    )

                # If working on the 5 Model Average and RCP 8.5 scenario,
                # generate the annual mean, min, and max
                if model_option == "5ModelAvg" and rcp_option == "rcp85":
                    if all_monthly_mean_values:
                        annual_mean = round(
                            sum(all_monthly_mean_values) / len(all_monthly_mean_values),
                            1,
                        )
                        annual_means = []
                        # Iterates through a 30-year era
                        for year in years_of_interest:
                            yearly_values = []
                            for month in range(0, 12):
                                # Index is a combination of 30 yearly values per month
                                # and the year that it currently is. Since the year is
                                # a value in the 2000s, we subtract the starting year to
                                # have a valid value to increase the index by.
                                index = (month * 30) + (year - years_of_interest[0])
                                yearly_values.append(all_monthly_mean_values[index])
                            # Get the annual mean for this given year in the era of interest
                            annual_means.append(
                                round(sum(yearly_values) / len(yearly_values), 1)
                            )
                        # Set the annual minimum and maximum from the annual means
                        annual_max = max(annual_means)
                        annual_min = min(annual_means)
                    else:
                        annual_mean = None
                        annual_max = None
                        annual_min = None

                    # Append the results to the projected_monthly_mmm DataFrame
                    # only if the 5ModelAvg and RCP 8.5 since we only want the annual values
                    # from that model and scenario option.
                    combination_label = f"{model_option}_{rcp_option}_Annual_{years_of_interest[0]}_{years_of_interest[-1]}"
                    row_to_append = pd.DataFrame(
                        {
                            "Month": [combination_label],
                            "tasmean": [annual_mean],
                            "tasmax": [annual_max],
                            "tasmin": [annual_min],
                        }
                    )
                    projected_monthly_mmm = pd.concat(
                        [projected_monthly_mmm, row_to_append], ignore_index=True
                    )

    # Initialize an empty dictionary for the final JSON
    result_json = {}

    # Create the 'historical' section
    hist_data = {}
    for index, row in hist_monthly_mmm.iterrows():
        month = row["Month"]
        tasmean = row["tasmean"]
        tasmax = row["tasmax"]
        tasmin = row["tasmin"]
        hist_data[month] = {"tasmean": tasmean, "tasmax": tasmax, "tasmin": tasmin}

    result_json["historical"] = dict()
    result_json["historical"]["CRU-TS"] = dict()
    result_json["historical"]["CRU-TS"]["historical"] = hist_data

    # Create the 'projected' section
    projected_data = {}

    grouped_projected = projected_monthly_mmm.groupby("Month")
    for combination_label, group in grouped_projected:
        model_option, rcp_option, month, era_start, era_end = combination_label.split(
            "_"
        )

        if model_option not in projected_data:
            projected_data[model_option] = {}

        if rcp_option not in projected_data[model_option]:
            projected_data[model_option][rcp_option] = {}

        if month not in projected_data[model_option][rcp_option]:
            projected_data[model_option][rcp_option][month] = {}

        era_key = f"{era_start}-{era_end}"
        projected_data[model_option][rcp_option][month][era_key] = {
            "tasmean": group["tasmean"].values[0],
            "tasmax": group["tasmax"].values[0],
            "tasmin": group["tasmin"].values[0],
        }

    result_json["projected"] = projected_data
    return result_json


async def fetch_bbox_netcdf(x1, y1, x2, y2, var_coord, cov_ids, summary_decades):
    """Make the async request for the data within the specified bbox

    Args:
        x1 (float): lower x-coordinate bound
        y1 (float): lower y-coordinate bound
        x2 (float): upper x-coordinate bound
        y2 (float): upper y-coordinate bound
        var_coord (int): coordinate value corresponding to varname to query
        cov_ids (str): list of Coverage ids to fetch the same bbox over
        summary_decades (list): list of either None or 2-tuples of integers
            mapped to desired range of decades to summarise over,
            e.g. (6, 8) for 2070-2099. List items need to
            correspond to items in cov_ids.

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    encoding = "netcdf"

    urls = []
    for cov_id, decade_tpl in zip(cov_ids, summary_decades):
        if decade_tpl:
            # if summary decades are given, create a WCPS request string
            x = f"{x1}:{x2}"
            y = f"{y1}:{y2}"
            request_str = get_wcps_request_str(
                x, y, var_coord, cov_id, decade_tpl, encoding
            )
        else:
            # otheriwse use generic WCS request str
            x = f"{x1},{x2}"
            y = f"{y1},{y2}"
            request_str = generate_wcs_getcov_str(
                x, y, cov_id, var_coord, encoding=encoding
            )
        urls.append(generate_wcs_query_url(request_str))

    start_time = time.time()
    data_list = await fetch_data(urls)
    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )

    # create xarray.DataSet from bytestring
    ds_list = [xr.open_dataset(io.BytesIO(netcdf_bytes)) for netcdf_bytes in data_list]

    return ds_list


def combine_pkg_dicts(tas_di, pr_di):
    """combine and return to packaed data dicts,
    for combining tas and pr individual endpoint results

    Args:
        tas_di (dict): result dict from point or HUC query for temperature
        pr_di (dict): result dict from point or HUC query for precip

    Returns:
        Combined dict containing both tas and pr results
    """
    # merge pr_di with tas_di
    # do so by creating all dim combinations up to level of "tas"/"pr"
    # and pull/place values
    # start with CRU separateley since we don't have valid combinations
    # for models/scenarios etc with AR5 data
    dim_combos = [
        ("1950_2009", season, "CRU-TS40", "CRU_historical")
        for season in dim_encodings["season"].values()
    ]
    # generate combinations of AR5 coords
    periods = ["2040_2069", "2070_2099", *dim_encodings["decade"].values()]
    dim_basis = [periods]
    dim_basis.extend(
        [dim_encodings[dimname].values() for dimname in ["season", "model", "scenario"]]
    )
    dim_combos.extend(itertools.product(*dim_basis))
    for map_list in dim_combos:
        result_di = get_from_dict(pr_di, map_list)
        get_from_dict(tas_di, map_list)["pr"] = result_di["pr"]

    return tas_di


def run_fetch_mmm_point_data(var_ep, lat, lon, cov_id, start_year, end_year):
    """Run the async tas/pr data requesting for a single point
    and return data as json

    Args:
        var_ep (str): temperature or precipitation
        lat (float): latitude
        lon (float): longitude
        cov_id (list):
            string of jan_min_max_mean_temp or july_min_max_mean_temp

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    if validate_latlon(lat, lon) is not True:
        return None

    x, y = project_latlon(lat, lon, 3338)

    point_data_list = asyncio.run(
        fetch_mmm_point_data(x, y, cov_id, start_year, end_year)
    )

    varname = var_ep_lu[var_ep]
    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = package_mmm_point_data(
        point_data_list, cov_id, varname, start_year, end_year
    )

    return point_pkg


async def run_fetch_tas_2km_point_data(lat, lon):
    """Run the async tas/pr data requesting for a single point
    and return data as json

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    if validate_latlon(lat, lon) is not True:
        return None

    x, y = project_latlon(lat, lon, 3338)

    tasks = []
    for coverage in ["historical", "projected"]:
        tasks.append(
            asyncio.create_task(fetch_wcs_point_data(x, y, f"tas_2km_{coverage}"))
        )
    results = await asyncio.gather(*tasks)

    point_pkg = package_tas_2km_point_data(results)

    return point_pkg


def run_fetch_var_point_data(var_ep, lat, lon):
    """Run the async tas/pr data requesting for a single point
    and return data as json

    Args:
        var_ep (str): Abbreviation name for variable of interest,
            either "tas" or "pr"
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    if validate_latlon(lat, lon) is not True:
        return None

    varname = var_ep_lu[var_ep]
    # get the coordinate value for the specified variable
    # just a way to lookup reverse of varname
    var_coord = list(dim_encodings["varname"].keys())[
        list(dim_encodings["varname"].values()).index(varname)
    ]

    x, y = project_latlon(lat, lon, 3338)

    # get and combine the CRU and AR5 packages
    # use CRU as basis for combined point package for chronological consistency
    # order of listing: CRU (1950-2009), AR5 2040-2069 summary,
    #     AR5 2070-2099 summary, AR5 seasonal data
    # query CRU baseline summary
    cov_ids, summary_decades = make_fetch_args()
    point_data_list = asyncio.run(
        fetch_point_data(x, y, var_coord, cov_ids, summary_decades)
    )

    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = dict()
    point_pkg["1950_2009"] = package_cru_point_data(point_data_list[0], varname)
    point_pkg["2040_2069"] = package_ar5_point_summary(point_data_list[1], varname)
    point_pkg["2070_2099"] = package_ar5_point_summary(point_data_list[2], varname)
    # package AR5 decadal data with decades and fold into data pakage
    ar5_point_pkg = package_ar5_point_data(point_data_list[3], varname)
    for decade, summaries in ar5_point_pkg.items():
        point_pkg[decade] = summaries

    return point_pkg


def run_fetch_point_data(lat, lon):
    """Fetch and combine point data for both
       temperature and precipitation

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and
        longitude
    """
    tas_pkg, pr_pkg = [
        run_fetch_var_point_data(var_ep, lat, lon)
        for var_ep in ["temperature", "precipitation"]
    ]

    combined_pkg = combine_pkg_dicts(tas_pkg, pr_pkg)

    return combined_pkg


def run_aggregate_allvar_polygon(poly_id):
    """Get data summary (e.g. zonal mean) within a Polygon for all variables."""
    tas_pkg, pr_pkg = [
        run_aggregate_var_polygon(var_ep, poly_id)
        for var_ep in ["temperature", "precipitation"]
    ]
    combined_pkg = combine_pkg_dicts(tas_pkg, pr_pkg)
    return combined_pkg


def run_aggregate_var_polygon(var_ep, poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon.
    Fetches data on the individual instances of the singular dimension combinations.
    Args:
        var_ep (str): Data variable. One of 'taspr', 'temperature', or 'precipitation'.
        poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal mean.
    Returns:
        aggr_results (dict): data representing zonal means within the polygon.
    """
    polygon = get_poly(poly_id)
    varname = var_ep_lu[var_ep]
    bandname = "Gray"

    # find the integer coordinate for the variable name
    var_coord = list(dim_encodings["varname"].keys())[
        list(dim_encodings["varname"].values()).index(varname)
    ]

    # fetch variable data within the Polygon bounding box
    # data is fetched from "iem_cru_2km_taspr_seasonal_baseline_stats":
    #       ds_list[0] = pre-summarized 1950-2009 baseline
    # and from "iem_cru_2km_taspr_ar5_seasonal":
    #       ds_list[1] = summarized decades 3-5 (2040-2069)
    #       ds_list[2] = summarized decades 6-8 (2070-2099)
    #       ds_list[3] = unsummarized all decades (2010-2099)
    summary_periods = ["1950_2009", "2040_2069", "2070_2099", None]
    cov_ids, summary_decades = make_fetch_args()
    ds_list = asyncio.run(
        fetch_bbox_netcdf(*polygon.total_bounds, var_coord, cov_ids, summary_decades)
    )
    # use a flag to indicate if we need to add CRU labels (they are not included in the coverage axes)
    add_cru_flag = [True, False, False, False]
    # use another flag to indicate if we need have the summary period as a dimension (ie the 4th dataset)
    add_ar5_flag = [False, False, False, True]

    aggr_results_combined = {}

    # run the zonal stats process for the summary periods
    for ds, period, cru, ar5 in zip(
        ds_list, summary_periods, add_cru_flag, add_ar5_flag
    ):
        # get all combinations of non-XY dimensions in the dataset and their corresponding encodings
        # and create a dict to hold the results for each combo
        all_dims = ds[bandname].dims
        dimnames = [dim for dim in all_dims if dim not in ["X", "Y"]]
        iter_coords = list(
            itertools.product(*[list(ds[dim].values) for dim in dimnames])
        )
        dim_combos = []
        for coords in iter_coords:
            map_list = [
                dim_encodings[dimname][coord]
                for coord, dimname in zip(coords, dimnames)
            ]
            dim_combos.append(map_list)

        # if we are summarizing over a period, we need to use the summary periods as keys
        # otherwise we use the full dim_combos to populate decades as keys (ie the 4th dataset)
        if period is not None:
            aggr_results = {}
            aggr_results[period] = generate_nested_dict(dim_combos)
        else:
            aggr_results = generate_nested_dict(dim_combos)

        # add model/scenario labels for the CRU dataset, and varname labels for all datasets
        for era in aggr_results:
            for season in aggr_results[era]:
                if cru:
                    season_stat_dict = aggr_results[era][season].copy()
                    aggr_results[era][season] = {
                        "CRU-TS40": {"CRU_historical": {varname: season_stat_dict}}
                    }
                else:
                    for model in aggr_results[era][season]:
                        for scenario in aggr_results[era][season][model]:
                            aggr_results[era][season][model][scenario] = {varname: {}}

        # fetch the dim combo from the dataset and calculate zonal stats, adding to the results dict
        for coords, dim_combo in zip(iter_coords, dim_combos):
            sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
            combo_ds = ds.sel(sel_di)
            combo_zonal_stats_dict = interpolate_and_compute_zonal_stats(
                polygon, combo_ds
            )

            if cru:
                if dim_combo[1] == "mean":
                    result = round(
                        combo_zonal_stats_dict["mean"],
                        dim_encodings["rounding"][varname],
                    )
                elif dim_combo[1] == "min":
                    result = round(
                        combo_zonal_stats_dict["min"],
                        dim_encodings["rounding"][varname],
                    )
                elif dim_combo[1] == "max":
                    result = round(
                        combo_zonal_stats_dict["max"],
                        dim_encodings["rounding"][varname],
                    )
                # we are taking means of all other stat values here, which is mathematically questionable!
                else:
                    result = round(
                        combo_zonal_stats_dict["mean"],
                        dim_encodings["rounding"][varname],
                    )

                # use the dim_combo to index into the results dict (period, season, model, scenario, varname, stat)
                aggr_results["1950_2009"][dim_combo[0]]["CRU-TS40"]["CRU_historical"][
                    varname
                ][dim_combo[1]] = result

            else:
                mean = round(
                    combo_zonal_stats_dict["mean"], dim_encodings["rounding"][varname]
                )
                min = round(
                    combo_zonal_stats_dict["min"], dim_encodings["rounding"][varname]
                )
                max = round(
                    combo_zonal_stats_dict["max"], dim_encodings["rounding"][varname]
                )

                result = mean  # default to mean for projected data

                # option to return min/mean/max for projected data
                # result = {
                #     "mean": mean,
                #     "min": min,
                #     "max": max,
                # }

                # populate the dict with results
                # need to skip a dimension in the combo for the 4th dataset
                if ar5:
                    aggr_results[dim_combo[0]][dim_combo[1]][dim_combo[2]][
                        dim_combo[3]
                    ][varname] = result
                else:
                    aggr_results[period][dim_combo[0]][dim_combo[1]][dim_combo[2]][
                        varname
                    ] = result

        # combine the results for this summary period with the overall results
        for era in aggr_results:
            aggr_results_combined[era] = aggr_results[era]

    return aggr_results_combined


def run_fetch_proj_precip_point_data(lat, lon, csv=False):
    """Fetch projected precipitation data for a
       given latitude and longitude.

    Args:
        lat (float): latitude
        lon (float): longitude
        csv (boolean): if csv is true, we change the intervals to
                       exceedance_probability

    Returns:
        JSON-like dict of data at provided latitude and
        longitude
    """
    x, y = project_latlon(lat, lon, 3338)

    rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, dot_precip_coverage_id))

    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = dict()
    for interval in range(len(dot_dim_encodings["intervals"])):
        if csv:
            interval_key = 100 / int(dot_dim_encodings["intervals"][interval])
        else:
            interval_key = dot_dim_encodings["intervals"][interval]
        point_pkg[interval_key] = dict()
        for duration in range(len(dot_dim_encodings["durations"])):
            duration_key = dot_dim_encodings["durations"][duration]
            point_pkg[interval_key][duration_key] = dict()
            for model in range(len(dot_dim_encodings["models"])):
                model_key = dot_dim_encodings["models"][model]
                point_pkg[interval_key][duration_key][model_key] = dict()
                for era in range(len(dot_dim_encodings["eras"])):
                    era_key = dot_dim_encodings["eras"][era]
                    point_pkg[interval_key][duration_key][model_key][era_key] = dict()
                    pf_data = rasdaman_response[interval][duration][model][era].split(
                        " "
                    )
                    # Convert values to metric (millimeters) before returning them in the API
                    point_pkg[interval_key][duration_key][model_key][era_key]["pf"] = (
                        round((float(pf_data[0]) / 1000) * 25.4, 2)
                    )
                    point_pkg[interval_key][duration_key][model_key][era_key][
                        "pf_upper"
                    ] = round((float(pf_data[1]) / 1000) * 25.4, 2)
                    point_pkg[interval_key][duration_key][model_key][era_key][
                        "pf_lower"
                    ] = round((float(pf_data[2]) / 1000) * 25.4, 2)

    return point_pkg


@routes.route("/taspr/")
@routes.route("/temperature/")
@routes.route("/temperature/abstract/")
@routes.route("/precipitation/")
@routes.route("/precipitation/abstract/")
@routes.route("/taspr/abstract/")
@routes.route("/taspr/point/")
@routes.route("/temperature/point/")
@routes.route("/precipitation/point/")
@routes.route("/taspr/area/")
@routes.route("/temperature/area/")
@routes.route("/precipitation/area/")
def about():
    return render_template("documentation/taspr.html")


@routes.route("/eds/temperature/<lat>/<lon>")
def get_temperature_plate(lat, lon):
    """
    Endpoint for requesting all data required for the Temperature Plate
    in the ArcticEDS client.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/eds/temperature/65.0628/-146.1627
    """
    temp = dict()

    temp_json = tas_2km_point_data_endpoint(lat, lon)

    if isinstance(temp_json, tuple):
        # Returns error template that was generated for invalid request
        return temp_json

    temp["summary"] = create_temperature_eds_summary(temp_json)

    point_pkg = nullify_and_prune(temp_json, "tas2km")
    if point_pkg in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    place_id = request.args.get("community")
    temp_csv = create_csv(point_pkg, "tas2km", place_id, lat, lon)
    temp_csv = temp_csv.data.decode("utf-8")
    first = "\n".join(temp_csv.split("\n")[6:12]) + "\n"
    last = "\n".join(temp_csv.split("\n")[-6:])

    temp["preview"] = first + last

    return jsonify(temp)


@routes.route("/eds/precipitation/<lat>/<lon>")
def get_precipitation_plate(lat, lon):
    """
    Endpoint for requesting all data required for the Precipitation Plate
    in the ArcticEDS client.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/eds/precipitation/65.0628/-146.1627
    """

    pr = dict()

    summarized_data = {}
    all_data = mmm_point_data_endpoint("precipitation", lat, lon)

    # Checks if error exists from fetching DD point
    if isinstance(all_data, tuple):
        # Returns error template that was generated for invalid request
        return all_data

    historical_values = list(
        map(lambda x: x["pr"], all_data["CRU-TS"]["historical"].values())
    )
    summarized_data["historical"] = {
        "prmin": min(historical_values),
        "prmean": round(np.mean(historical_values)),
        "prmax": max(historical_values),
    }
    eras = [
        {"start": 2010, "end": 2039},
        {"start": 2040, "end": 2069},
        {"start": 2070, "end": 2099},
    ]
    models = list(all_data.keys())
    models.remove("CRU-TS")
    for era in eras:
        values = []
        for model in models:
            for scenarios in all_data[model].keys():
                for key, value in all_data[model][scenarios].items():
                    year = int(key)
                    if year >= era["start"] and year <= era["end"]:
                        values.append(value["pr"])
        summarized_data[str(era["start"]) + "-" + str(era["end"])] = {
            "prmin": min(values),
            "prmean": round(np.mean(values)),
            "prmax": max(values),
        }

    pr["summary"] = summarized_data

    first = mmm_point_data_endpoint("precipitation", lat, lon, None, 1901, 1905, True)
    last = mmm_point_data_endpoint("precipitation", lat, lon, None, 2096, 2100, True)

    for response in [first, last]:
        if isinstance(response, tuple):
            # Returns error template that was generated for invalid request
            return response

    no_metadata = "\n".join(first.data.decode("utf-8").split("\n")[4:])
    no_header = "\n".join(last.data.decode("utf-8").split("\n")[-6:])

    pr["preview"] = no_metadata + no_header

    return jsonify(pr)


@routes.route("/<var_ep>/<lat>/<lon>")
@routes.route("/<var_ep>/<month>/<lat>/<lon>")
@routes.route("/<var_ep>/<lat>/<lon>/<start_year>/<end_year>")
@routes.route("/<var_ep>/<month>/<lat>/<lon>/<start_year>/<end_year>")
def mmm_point_data_endpoint(
    var_ep, lat, lon, month=None, start_year=None, end_year=None, preview=None
):
    """Point data endpoint. Fetch point data for
    specified var/lat/lon and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either temperature or precipitation
        month (str): jan or july
        lat (float): latitude
        lon (float): longitude
        start_year (int): Starting year (1901-2099)
        end_year (int): Ending year (1901-2099)

    Notes:
        example request: http://localhost:5000/jan/65.0628/-146.1627
    """

    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    if month is not None:
        if month == "jan":
            cov_id = "jan_min_max_mean_temp"
        elif month == "july":
            cov_id = "july_min_max_mean_temp"
        else:
            return render_template("400/bad_request.html"), 400
    else:
        if var_ep == "precipitation":
            cov_id = "annual_precip_totals_mm"
        elif var_ep == "temperature":
            cov_id = "annual_mean_temp"
        else:
            return render_template("400/bad_request.html"), 400

    if None not in [start_year, end_year]:
        validation = validate_year(start_year, end_year)
        if validation == 400:
            return render_template("400/bad_request.html"), 400

    # validate request args before fetching data
    if len(request.args) == 0:
        pass  # no additional request args will be passed to the run_fetch_mmm_point_data function
    else:
        # if args exist, check if they are allowed
        allowed_args = ["summarize", "format", "community"]
        if not all(key in allowed_args for key in request.args.keys()):
            return render_template("400/bad_request.html"), 400

    # if args exist and are allowed, return the appropriate response
    try:
        point_pkg = run_fetch_mmm_point_data(
            var_ep, lat, lon, cov_id, start_year, end_year
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    # if preview, return as CSV
    # the preview arg is only used for CSV generation and should never occur with additional request args
    if preview:
        point_pkg = nullify_and_prune(point_pkg, "taspr")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        place_id = request.args.get("community")
        month_labels = {"jan": "January", "july": "July"}
        if month is not None:
            filename_prefix = month_labels[month]
            return create_csv(
                point_pkg,
                var_ep + "_mmm",
                place_id,
                lat,
                lon,
                filename_prefix=filename_prefix,
                start_year=start_year,
                end_year=end_year,
            )
        else:
            return create_csv(
                point_pkg,
                var_ep + "_all",
                place_id,
                lat,
                lon,
                start_year=start_year,
                end_year=end_year,
            )

    if not request.args.get("summarize") == "mmm" and (
        request.args.get("format") == "csv" or preview
    ):
        point_pkg = nullify_and_prune(point_pkg, "taspr")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        place_id = request.args.get("community")
        month_labels = {"jan": "January", "july": "July"}
        if month is not None:
            filename_prefix = month_labels[month]
            return create_csv(
                point_pkg,
                var_ep + "_mmm",
                place_id,
                lat,
                lon,
                filename_prefix=filename_prefix,
                start_year=start_year,
                end_year=end_year,
            )
        else:
            return create_csv(
                point_pkg,
                var_ep + "_all",
                place_id,
                lat,
                lon,
                start_year=start_year,
                end_year=end_year,
            )

    return postprocess(point_pkg, "taspr")


@routes.route("/tas2km/point/<lat>/<lon>")
def tas_2km_point_data_endpoint(lat, lon):
    """Point data endpoint. Fetch point data for
    specified var/lat/lon and return JSON-like dict.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/tas2km/point/65.0628/-146.1627
    """

    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    # validate request args before fetching data
    if len(request.args) == 0:
        pass
    else:
        # if args exist, check if they are allowed
        allowed_args = ["format", "community"]
        if not all(key in allowed_args for key in request.args.keys()):
            return render_template("400/bad_request.html"), 400

    try:
        point_pkg = asyncio.run(run_fetch_tas_2km_point_data(lat, lon))
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    if "format" in request.args:
        point_pkg = nullify_and_prune(point_pkg, "tas2km")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        place_id = request.args.get("community")
        return create_csv(point_pkg, "tas2km", place_id, lat, lon)

    return postprocess(point_pkg, "tas2km")


@routes.route("/temperature/point/<lat>/<lon>")
@routes.route("/precipitation/point/<lat>/<lon>")
@routes.route("/taspr/point/<lat>/<lon>")
def point_data_endpoint(lat, lon):
    """Point data endpoint. Fetch point data for
    specified var/lat/lon and return JSON-like dict.

    Args:
        lat (float): latitude
        lon (float): longitude

    Notes:
        example request: http://localhost:5000/temperature/point/65.0628/-146.1627
    """

    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    # validate request args before fetching data
    if len(request.args) == 0:
        pass
    else:
        # if args exist, check if they are allowed
        allowed_args = ["format", "community"]
        if not all(key in allowed_args for key in request.args.keys()):
            return render_template("400/bad_request.html"), 400

    if "temperature" in request.path:
        var_ep = "temperature"
    elif "precipitation" in request.path:
        var_ep = "precipitation"
    else:
        var_ep = "taspr"

    if var_ep in var_ep_lu.keys():
        point_pkg = run_fetch_var_point_data(var_ep, lat, lon)
    elif var_ep == "taspr":
        try:
            point_pkg = run_fetch_point_data(lat, lon)
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500
    else:
        return render_template("400/bad_request.html"), 400

    if "format" in request.args:
        point_pkg = nullify_and_prune(point_pkg, "taspr")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        place_id = request.args.get("community")
        return create_csv(point_pkg, var_ep, place_id, lat, lon)

    return postprocess(point_pkg, "taspr")


@routes.route("/temperature/area/<var_id>")
@routes.route("/precipitation/area/<var_id>")
@routes.route("/taspr/area/<var_id>")
def taspr_area_data_endpoint(var_id):
    """Aggregation data endpoint. Fetch data within polygon area
    for specified variable and return JSON-like dict.

    Args:
        var_id (str): ID for given polygon from polygon endpoint.
    Returns:
        poly_pkg (dict): zonal mean of variable(s) for AOI polygon

    """

    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
        if "temperature" in request.path:
            var_ep = "temperature"
        elif "precipitation" in request.path:
            var_ep = "precipitation"
        else:
            var_ep = "taspr"

        if var_ep in var_ep_lu.keys():
            poly_pkg = run_aggregate_var_polygon(var_ep, var_id)
        elif var_ep == "taspr":
            poly_pkg = run_aggregate_allvar_polygon(var_id)
        else:
            return render_template("400/bad_request.html"), 400

    except:
        return render_template("422/invalid_area.html"), 422

    # validate request args before fetching data
    if len(request.args) == 0:
        pass
    else:
        # if args exist, check if they are allowed
        allowed_args = ["format", "community"]
        if not all(key in allowed_args for key in request.args.keys()):
            return render_template("400/bad_request.html"), 400

    if "format" in request.args:
        poly_pkg = nullify_and_prune(poly_pkg, "taspr")
        if poly_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        return create_csv(poly_pkg, var_ep, var_id)
    return postprocess(poly_pkg, "taspr")


@routes.route("/precipitation/frequency/point/<lat>/<lon>")
def proj_precip_point(lat, lon):
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    # validate request args before fetching data
    if len(request.args) == 0:
        pass
    else:
        # if args exist, check if they are allowed
        allowed_args = ["format", "community"]
        if not all(key in allowed_args for key in request.args.keys()):
            return render_template("400/bad_request.html"), 400

    if "format" in request.args:
        csv = True
    else:
        csv = False

    try:
        point_pkg = run_fetch_proj_precip_point_data(lat, lon, csv)
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    if "format" in request.args:
        point_pkg = nullify_and_prune(point_pkg, "proj_precip")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        place_id = request.args.get("community")
        return create_csv(point_pkg, "proj_precip", place_id, lat, lon)

    return postprocess(point_pkg, "proj_precip")
