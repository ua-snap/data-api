import asyncio
import io
import csv
import json
import time
import itertools
from urllib.parse import quote
import numpy as np
import xarray as xr
import pandas as pd
from flask import (
    Blueprint,
    Response,
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
    get_from_dict,
    summarize_within_poly,
    csv_metadata,
)
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
    validate_year,
)
from validate_data import (
    get_poly_3338_bbox,
    nullify_and_prune,
    postprocess,
    place_name_and_type,
)
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

# encodings hardcoded for now
# fmt: off
# lookup tables derived from the IEM rasdaman ingest luts.py
# pay attention to any changes with ingest and change here as needed
dim_encodings = {
    "varnames": {
        0: "pr",
        1: "tas",
    },
    "decades": {
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
    "months": {
        0: "Jan",
        1: "Feb",
        2: "Mar",
        3: "Apr",
        4: "May",
        5: "Jun",
        6: "Jul",
        7: "Aug",
        8: "Sep",
        9: "Oct",
        10: "Nov",
        11: "Dec",
    },
    "models": {
        0: "5modelAvg",
        1: "CCSM4",
        2: "MRI-CGCM3",
    },
    "scenarios": {
        0: "rcp45",
        1: "rcp60",
        2: "rcp85",
    },
    "seasons": {
        0: "DJF",
        1: "JJA",
        2: "MAM",
        3: "SON",
    },
    "stats": {
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

var_label_lu = {
    "temperature": "Temperature",
    "precipitation": "Precipitation",
    "taspr": "Temperature & Precipitation",
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


async def fetch_mmm_point_data(x, y, cov_id, horp, start_year, end_year):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_id (str): Rasdaman coverage ID string
        horp [Historical or Projected ](str): historical, projected, hp, or all

    Returns:
        list of data results from each cov_id for CRU TS 4.0 and all 5 projected
        models for a given coordinate
    """
    point_data_list = []
    if horp == "all":
        request_str = generate_mmm_wcs_getcov_str(x, y, cov_id, "0,6", "0,3")
        point_data_list = await fetch_data([generate_wcs_query_url(request_str)])

    if horp == "historical" or horp == "hp":
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

    if horp == "projected" or horp == "hp":
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


def package_mmm_point_data(point_data, cov_id, horp, varname):
    """Packages min-mean-max point data into JSON-formatted return data

    Args:
        point_data (list): Nested list of returned data from Rasdaman
            * point_data is a four-dimensional list with the following indices:
                - point_data[i] = variable (tasmax, tasmean, and tasmin)
                - point_data[i][j] = year starting from 1900-200 [0-199]
                - point_data[i][j][k] = model number between 0-6 in mmm_dim_encodings
                - point_data[i][j][k][l] = scenario number between 0-3 in mmm_dim_encodings
        horp [Historical or Projected ](str): historical, projected, hp, or all
        varname (str): variable name to fetch point data
            for one of "tas" or "pr"

    Returns:
        Python dictionary of one of four outcomes:
            * horp == 'historical' - Historical minimum, mean, and maximum
            * horp == 'projected'  - Projected minimum, mean, and maximum
            * horp == 'hp'         - Historical & projected minimum, mean, and maximum
            * horp == 'all'        - All data returned from Rasdaman formatted with string indices
    """
    point_pkg = dict()
    if horp == "all":

        ### HISTORICAL CRU-TS 4.0 ###
        # Puts together the historical tasmin, tasmean, and tasmax for all 200 years
        point_pkg["CRU-TS"] = dict()
        point_pkg["CRU-TS"]["historical"] = dict()

        for year in range(0, 115):
            full_year = str(year + 1901)
            point_pkg["CRU-TS"]["historical"][full_year] = dict()
            if cov_id == "annual_precip_totals_mm":
                point_pkg["CRU-TS"]["historical"][full_year]["pr"] = point_data[0][
                    year
                ][0]
            elif cov_id == "annual_mean_temp":
                point_pkg["CRU-TS"]["historical"][full_year]["tas"] = point_data[0][
                    year
                ][0]
            else:
                point_pkg["CRU-TS"]["historical"][full_year]["tasmax"] = point_data[0][
                    year
                ][0][0]
                point_pkg["CRU-TS"]["historical"][full_year]["tasmean"] = point_data[1][
                    year
                ][0][0]
                point_pkg["CRU-TS"]["historical"][full_year]["tasmin"] = point_data[2][
                    year
                ][0][0]

        ### PROJECTED FUTURE MODELS ###
        # For all models, scenarios, and variables found in mmm_dim_encodings dictionary

        for model in mmm_dim_encodings["models"].keys():
            dim_model = mmm_dim_encodings["models"][model]
            point_pkg[dim_model] = dict()
            for scenario in mmm_dim_encodings["scenarios"].keys():
                dim_scenario = mmm_dim_encodings["scenarios"][scenario]
                point_pkg[dim_model][dim_scenario] = dict()
                for year in range(106, 200):
                    full_year = str(year + 1901)
                    point_pkg[dim_model][dim_scenario][full_year] = dict()
                    if cov_id == "annual_precip_totals_mm":
                        point_pkg[dim_model][dim_scenario][full_year][
                            "pr"
                        ] = point_data[model][year][scenario]
                    elif cov_id == "annual_mean_temp":
                        point_pkg[dim_model][dim_scenario][full_year][
                            "tas"
                        ] = point_data[model][year][scenario]
                    else:
                        for variable in mmm_dim_encodings["tempstats"].keys():
                            dim_variable = mmm_dim_encodings["tempstats"][variable]
                            point_pkg[dim_model][dim_scenario][full_year][
                                dim_variable
                            ] = point_data[variable][year][model][scenario]

    else:
        if horp == "historical" or horp == "hp":
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

        if horp == "projected" or horp == "hp":
            if horp == "projected":
                projected_max = round(point_data[0], dim_encodings["rounding"][varname])
                projected_mean = round(
                    point_data[1], dim_encodings["rounding"][varname]
                )
                projected_min = round(point_data[2], dim_encodings["rounding"][varname])
            else:
                projected_max = round(point_data[3], dim_encodings["rounding"][varname])
                projected_mean = round(
                    point_data[4], dim_encodings["rounding"][varname]
                )
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

    return point_pkg


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
        season = dim_encodings["seasons"][si]
        model = "CRU-TS40"
        scenario = "CRU_historical"
        point_data_pkg[season] = {model: {scenario: {varname: {}}}}
        for si, value in enumerate(s_li):  # (nested list with statistic at dim 0)
            stat = dim_encodings["stats"][si]
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
        decade = dim_encodings["decades"][di]
        point_data_pkg[decade] = {}
        for ai, mod_li in enumerate(m_li):  # (nested list with model at dim 0)
            season = dim_encodings["seasons"][ai]
            point_data_pkg[decade][season] = {}
            for mod_i, s_li in enumerate(
                mod_li
            ):  # (nested list with scenario at dim 0)
                model = dim_encodings["models"][mod_i]
                point_data_pkg[decade][season][model] = {}
                for si, value in enumerate(s_li):  # (nested list with varname at dim 0)
                    scenario = dim_encodings["scenarios"][si]
                    point_data_pkg[decade][season][model][scenario] = {
                        varname: None
                        if value is None
                        else round(value, dim_encodings["rounding"][varname])
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
        season = dim_encodings["seasons"][si]
        point_data_pkg[season] = {}
        for mod_i, s_li in enumerate(mod_li):  # (nested list with scenario at dim 0)
            model = dim_encodings["models"][mod_i]
            point_data_pkg[season][model] = {}
            for si, value in enumerate(s_li):  # (nested list with varname at dim 0)
                scenario = dim_encodings["scenarios"][si]
                point_data_pkg[season][model][scenario] = {
                    varname: None
                    if value is None
                    else round(value, dim_encodings["rounding"][varname])
                }

    return point_data_pkg


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


def create_csv(
    packaged_data, var_ep, place_id, lat=None, lon=None, mmm=False, month=None
):
    """
    Returns a CSV version of the fetched data, as a string.

    Args:
        packaged_data (json): JSON-like data package output
            from the run_fetch_* and run_aggregate_* functions
        var_ep (str): tas, pr, mmm, or taspr
        place_id (str): community or area ID unless just a lat/lon value
        lat: latitude unless an area
        lon: longitude unless an area
        mmm (bool): flag for whether packaged_data is for the min-mean-max endpoints
        month (str): month option used for mmm endpoint for packaged_data

    Returns:
        string of CSV data
    """

    output = io.StringIO()

    place_name, place_type = place_name_and_type(place_id)

    metadata = csv_metadata(place_name, place_id, place_type, lat, lon)

    if var_ep in ["temperature", "taspr"]:
        metadata += "# tas is the temperature at surface in degrees Celsius\n"
        if mmm is True:
            metadata = "# tas is the temperature at surface in degrees Fahrenheit\n"
            if month is not None:
                metadata += "# tasmin is the minimum temperature for the specified model and scenario\n"
                metadata += "# tasmean is the mean temperature for the specified model and scenario\n"
                metadata += "# tasmax is the maximum temperature for the specified model and scenario\n"
            else:
                metadata = "# tas is the mean annual near-surface air temperature for the specified model and scenario\n"
    if var_ep in ["precipitation", "taspr"]:
        metadata += "# pr is precipitation in millimeters\n"
        if mmm is True:
            metadata = "# pr is precipitation in millimeters\n"
            metadata += "# pr is the total annual precipitation for the specified model and scenario\n"

    if mmm is not True:
        metadata += "# mean is the mean of annual means\n"
        metadata += "# median is the median of annual means\n"
        metadata += "# max is the maximum annual mean\n"
        metadata += "# min is the minimum annual mean\n"
        metadata += "# q1 is the first quartile of the annual means\n"
        metadata += "# q3 is the third quartile of the annual means\n"
        metadata += "# hi_std is the mean + standard deviation of annual means\n"
        metadata += "# lo_std is the mean - standard deviation of annual means\n"
        metadata += "# DJF is December - February\n"
        metadata += "# MAM is March - May\n"
        metadata += "# JJA is June - August\n"
        metadata += "# SON is September - November\n"

    output.write(metadata)

    if mmm is not True:
        fieldnames = [
            "variable",
            "date_range",
            "season",
            "model",
            "scenario",
            "stat",
            "value",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()

        # add CRU data
        cru_period = "1950_2009"
        for season in dim_encodings["seasons"].values():
            for varname in ["pr", "tas"]:
                for stat in dim_encodings["stats"].values():
                    try:
                        writer.writerow(
                            {
                                "variable": varname,
                                "date_range": cru_period,
                                "season": season,
                                "model": "CRU-TS40",
                                "scenario": "Historical",
                                "stat": stat,
                                "value": packaged_data[cru_period][season]["CRU-TS40"][
                                    "CRU_historical"
                                ][varname][stat],
                            }
                        )
                    except KeyError:
                        # if single var query, just ignore attempts to
                        # write the non-chosen var
                        pass

        # AR5 periods
        for ar5_period in ["2040_2069", "2070_2099"]:
            for season in dim_encodings["seasons"].values():
                for model in dim_encodings["models"].values():
                    for scenario in dim_encodings["scenarios"].values():
                        for varname in ["pr", "tas"]:
                            try:
                                writer.writerow(
                                    {
                                        "variable": varname,
                                        "date_range": ar5_period,
                                        "season": season,
                                        "model": model,
                                        "scenario": scenario,
                                        "stat": "mean",
                                        "value": packaged_data[ar5_period][season][
                                            model
                                        ][scenario][varname],
                                    }
                                )
                            except KeyError:
                                # if single var query, just ignore attempts to
                                # write the non-chosen var
                                pass

        for decade in dim_encodings["decades"].values():
            for season in dim_encodings["seasons"].values():
                for model in dim_encodings["models"].values():
                    for scenario in dim_encodings["scenarios"].values():
                        for varname in ["pr", "tas"]:
                            try:
                                writer.writerow(
                                    {
                                        "variable": varname,
                                        "date_range": decade,
                                        "season": season,
                                        "model": model,
                                        "scenario": scenario,
                                        "stat": "mean",
                                        "value": packaged_data[decade][season][model][
                                            scenario
                                        ][varname],
                                    }
                                )
                            except KeyError:
                                # if single var query, just ignore attempts to
                                # write the non-chosen var
                                pass
    else:
        # This is for a min-mean-max CSV for temperature or precipitation
        if var_ep == "temperature" and month is not None:
            fieldnames = [
                "year",
                "model",
                "scenario",
                "tasmin",
                "tasmean",
                "tasmax",
            ]
        else:
            fieldnames = ["year", "model", "scenario", var_ep_lu[var_ep]]

        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()

        for model in packaged_data.keys():
            for scenario in packaged_data[model].keys():
                for year in packaged_data[model][scenario].keys():
                    try:
                        if var_ep == "temperature" and month is not None:
                            writer.writerow(
                                {
                                    "year": year,
                                    "model": model,
                                    "scenario": scenario,
                                    "tasmin": packaged_data[model][scenario][year][
                                        "tasmin"
                                    ],
                                    "tasmean": packaged_data[model][scenario][year][
                                        "tasmean"
                                    ],
                                    "tasmax": packaged_data[model][scenario][year][
                                        "tasmax"
                                    ],
                                }
                            )
                        else:
                            writer.writerow(
                                {
                                    "year": year,
                                    "model": model,
                                    "scenario": scenario,
                                    var_ep_lu[var_ep]: packaged_data[model][scenario][
                                        year
                                    ][var_ep_lu[var_ep]],
                                }
                            )
                    except KeyError:
                        pass

    return output.getvalue()


def return_csv(csv_data, var_ep, place_id, lat=None, lon=None, month=None):
    """Return the CSV data as a download

    Args:
        csv_data (?): csv data created with create_csv() function
        var_ep (str): tas, pr, or taspr
        place_id (str): community or area ID unless just a lat/lon value
        lat: latitude unless an area
        lon: longitude unless an area
        month (str): Month for MMM (jan or july)

    Returns:
        CSV Response
    """

    place_name, place_type = place_name_and_type(place_id)

    if place_name is not None:
        filename = var_label_lu[var_ep] + " for " + quote(place_name) + ".csv"
    elif month is not None:
        filename = (
            mmm_dim_encodings["months"][month]
            + " "
            + var_label_lu[var_ep]
            + " for "
            + lat
            + ", "
            + lon
            + ".csv"
        )
    else:
        filename = var_label_lu[var_ep] + " for " + lat + ", " + lon + ".csv"

    response = Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": 'attachment; filename="'
            + filename
            + "\"; filename*=utf-8''\""
            + filename
            + '"',
        },
    )

    return response


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
        for season in dim_encodings["seasons"].values()
    ]
    # generate combinations of AR5 coords
    periods = ["2040_2069", "2070_2099", *dim_encodings["decades"].values()]
    dim_basis = [periods]
    dim_basis.extend(
        [
            dim_encodings[dimname].values()
            for dimname in ["seasons", "models", "scenarios"]
        ]
    )
    dim_combos.extend(itertools.product(*dim_basis))
    for map_list in dim_combos:
        result_di = get_from_dict(pr_di, map_list)
        get_from_dict(tas_di, map_list)["pr"] = result_di["pr"]

    return tas_di


def run_fetch_mmm_point_data(var_ep, lat, lon, cov_id, horp, start_year, end_year):
    """Run the async tas/pr data requesting for a single point
    and return data as json

    Args:
        var_ep (str): temperature or precipitation
        lat (float): latitude
        lon (float): longitude
        cov_id (list):
            string of jan_min_max_mean_temp or july_min_max_mean_temp
        horp [Historical or Projected] (str): historical, projected, or all

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    if validate_latlon(lat, lon) is not True:
        return None

    x, y = project_latlon(lat, lon, 3338)

    point_data_list = asyncio.run(
        fetch_mmm_point_data(x, y, cov_id, horp, start_year, end_year)
    )

    varname = var_ep_lu[var_ep]
    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = package_mmm_point_data(point_data_list, cov_id, horp, varname)

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
    var_coord = list(dim_encodings["varnames"].keys())[
        list(dim_encodings["varnames"].values()).index(varname)
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

    Args:
        var_ep (str): Data variable. One of 'taspr', 'temperature', or 'precipitation'.
        poly_gdf (GeoDataFrame): the object from which to fetch the polygon, e.g. the HUC 8 geodataframe for watershed polygons
        poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal mean.

    Returns:
        aggr_results (dict): data representing zonal means within the polygon.

    Notes:
        Fetches data on the individual instances of the singular dimension combinations. Consider validating polygon IDs in `validate_data` or `lat_lon` module.
    """
    poly = get_poly_3338_bbox(poly_id)
    # mapping between coordinate values (ints) and variable names (strs)
    varname = var_ep_lu[var_ep]
    var_coord = list(dim_encodings["varnames"].keys())[
        list(dim_encodings["varnames"].values()).index(varname)
    ]
    # fetch data within the Polygon bounding box
    cov_ids, summary_decades = make_fetch_args()
    ds_list = asyncio.run(
        fetch_bbox_netcdf(*poly.bounds, var_coord, cov_ids, summary_decades)
    )
    # average over the following decades / time periods
    aggr_results = {}
    summary_periods = ["1950_2009", "2040_2069", "2070_2099"]
    for ds, period in zip(ds_list[:-1], summary_periods):
        aggr_results[period] = summarize_within_poly(
            ds, poly, dim_encodings, "Gray", varname
        )
    ar5_results = summarize_within_poly(
        ds_list[-1], poly, dim_encodings, "Gray", varname
    )
    for decade, summaries in ar5_results.items():
        aggr_results[decade] = summaries
    #  add the model, scenario, and varname levels for CRU
    for season in aggr_results[summary_periods[0]]:
        aggr_results[summary_periods[0]][season] = {
            "CRU-TS40": {
                "CRU_historical": {varname: aggr_results[summary_periods[0]][season]}
            }
        }
    # add the varnames for AR5
    for period in summary_periods[1:] + list(dim_encodings["decades"].values()):
        for season in aggr_results[period]:
            for model in aggr_results[period][season]:
                for scenario in aggr_results[period][season][model]:
                    aggr_results[period][season][model][scenario] = {
                        varname: aggr_results[period][season][model][scenario]
                    }
    return aggr_results


@routes.route("/temperature/")
@routes.route("/temperature/abstract/")
@routes.route("/precipitation/")
@routes.route("/precipitation/abstract/")
@routes.route("/taspr/")
@routes.route("/taspr/abstract/")
def about():
    return render_template("taspr/abstract.html")


@routes.route("/taspr/point/")
@routes.route("/temperature/point/")
@routes.route("/precipitation/point/")
def about_point():
    return render_template("taspr/point.html")


@routes.route("/taspr/area/")
@routes.route("/temperature/area/")
@routes.route("/precipitation/area/")
def about_huc():
    return render_template("taspr/area.html")


@routes.route("/mmm/")
@routes.route("/mmm/abstract/")
def about_mmm():
    return render_template("mmm/abstract.html")


@routes.route("/mmm/temperature")
def about_mmm_temp():
    return render_template("mmm/temperature.html")


@routes.route("/mmm/precipitation")
def about_mmm_precip():
    return render_template("mmm/precipitation.html")


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
    temp_plate = dict()

    ### HISTORICAL ###
    temp_plate["historical"] = dict()

    all = mmm_point_data_endpoint("temperature", "historical", lat, lon)
    temp_plate["historical"]["all"] = all["historical"]

    jan = mmm_point_data_endpoint("temperature", "historical", lat, lon, "jan")
    temp_plate["historical"]["jan"] = jan["historical"]

    july = mmm_point_data_endpoint("temperature", "historical", lat, lon, "july")
    temp_plate["historical"]["july"] = july["historical"]

    ### 2010-2039 ###
    temp_plate["2010-2039"] = dict()

    all = mmm_point_data_endpoint(
        "temperature", "projected", lat, lon, start_year="2010", end_year="2039"
    )
    temp_plate["2010-2039"]["all"] = all["projected"]

    jan = mmm_point_data_endpoint(
        "temperature",
        "projected",
        lat,
        lon,
        month="jan",
        start_year="2010",
        end_year="2039",
    )
    temp_plate["2010-2039"]["jan"] = jan["projected"]

    july = mmm_point_data_endpoint(
        "temperature",
        "projected",
        lat,
        lon,
        month="july",
        start_year="2010",
        end_year="2039",
    )
    temp_plate["2010-2039"]["july"] = july["projected"]

    ### 2040-2069 ###
    temp_plate["2040-2069"] = dict()

    all = mmm_point_data_endpoint(
        "temperature", "projected", lat, lon, start_year="2040", end_year="2069"
    )
    temp_plate["2040-2069"]["all"] = all["projected"]

    jan = mmm_point_data_endpoint(
        "temperature",
        "projected",
        lat,
        lon,
        month="jan",
        start_year="2040",
        end_year="2069",
    )
    temp_plate["2040-2069"]["jan"] = jan["projected"]

    july = mmm_point_data_endpoint(
        "temperature",
        "projected",
        lat,
        lon,
        month="july",
        start_year="2040",
        end_year="2069",
    )
    temp_plate["2040-2069"]["july"] = july["projected"]

    ### 2070-2099 ###
    temp_plate["2070-2099"] = dict()

    all = mmm_point_data_endpoint(
        "temperature", "projected", lat, lon, start_year="2070", end_year="2099"
    )
    temp_plate["2070-2099"]["all"] = all["projected"]

    jan = mmm_point_data_endpoint(
        "temperature",
        "projected",
        lat,
        lon,
        month="jan",
        start_year="2070",
        end_year="2099",
    )
    temp_plate["2070-2099"]["jan"] = jan["projected"]

    july = mmm_point_data_endpoint(
        "temperature",
        "projected",
        lat,
        lon,
        month="july",
        start_year="2070",
        end_year="2099",
    )
    temp_plate["2070-2099"]["july"] = july["projected"]

    return jsonify(temp_plate)


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
    pr_plate = dict()
    pr_plate = mmm_point_data_endpoint("precipitation", "historical", lat, lon)

    projected = mmm_point_data_endpoint(
        "precipitation", "projected", lat, lon, start_year="2010", end_year="2039"
    )
    pr_plate["2010-2039"] = projected["projected"]

    projected = mmm_point_data_endpoint(
        "precipitation", "projected", lat, lon, start_year="2040", end_year="2069"
    )
    pr_plate["2040-2069"] = projected["projected"]

    projected = mmm_point_data_endpoint(
        "precipitation", "projected", lat, lon, start_year="2070", end_year="2099"
    )
    pr_plate["2070-2099"] = projected["projected"]

    return jsonify(pr_plate)


@routes.route("/mmm/<var_ep>/<horp>/<lat>/<lon>")
@routes.route("/mmm/<var_ep>/<month>/<horp>/<lat>/<lon>")
@routes.route("/mmm/<var_ep>/<horp>/<lat>/<lon>/<start_year>/<end_year>")
@routes.route("/mmm/<var_ep>/<month>/<horp>/<lat>/<lon>/<start_year>/<end_year>")
def mmm_point_data_endpoint(
    var_ep, horp, lat, lon, month=None, start_year=None, end_year=None
):
    """Point data endpoint. Fetch point data for
    specified var/lat/lon and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either temperature or precipitation
        month (str): jan or july
        horp [Historical or Projected] (str):  historical, projected, hp, or all
        lat (float): latitude
        lon (float): longitude
        start_year (int): Starting year (1901-2099)
        end_year (int): Ending year (1901-2099)

    Notes:
        example request: http://localhost:5000/mmm/jan/all/65.0628/-146.1627
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

    if start_year is not None:
        if horp == "all":
            return render_template("400/bad_request.html"), 400

        if end_year is not None:
            validation = validate_year(start_year, end_year)

            if validation == 400:
                return render_template("400/bad_request.html"), 400
        else:
            return render_template("400/bad_request.html"), 400

    try:
        point_pkg = run_fetch_mmm_point_data(
            var_ep, lat, lon, cov_id, horp, start_year, end_year
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    if horp == "all" and request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(point_pkg, "taspr")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        place_id = request.args.get("community")
        csv_data = create_csv(point_pkg, var_ep, place_id, lat, lon, True, month)
        return return_csv(csv_data, var_ep, place_id, lat, lon, month)

    return postprocess(point_pkg, "taspr")


@routes.route("/<var_ep>/point/<lat>/<lon>")
def point_data_endpoint(var_ep, lat, lon):
    """Point data endpoint. Fetch point data for
    specified var/lat/lon and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either taspr, temperature,
            or precipitation
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

    if var_ep in var_ep_lu.keys():
        point_pkg = run_fetch_var_point_data(var_ep, lat, lon)
    elif var_ep == "taspr":
        try:
            point_pkg = run_fetch_point_data(lat, lon)
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500

    if request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(point_pkg, "taspr")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        place_id = request.args.get("community")
        csv_data = create_csv(point_pkg, var_ep, place_id, lat, lon)
        return return_csv(csv_data, var_ep, place_id, lat, lon)

    return postprocess(point_pkg, "taspr")


@routes.route("/<var_ep>/area/<var_id>")
def taspr_area_data_endpoint(var_ep, var_id):
    """Aggregation data endpoint. Fetch data within polygon area
    for specified variable and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either taspr, temperature,
            or precipitation
        var_id (str): ID for given polygon from polygon endpoint.
    Returns:
        poly_pkg (dict): zonal mean of variable(s) for AOI polygon

    """

    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
    if var_ep in var_ep_lu.keys():
        poly_pkg = run_aggregate_var_polygon(var_ep, var_id)
    elif var_ep == "taspr":
        poly_pkg = run_aggregate_allvar_polygon(var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    if request.args.get("format") == "csv":
        poly_pkg = nullify_and_prune(poly_pkg, "taspr")
        if poly_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        csv_data = create_csv(poly_pkg, var_ep, var_id)
        return return_csv(csv_data, var_ep, var_id)
    return postprocess(poly_pkg, "taspr")
