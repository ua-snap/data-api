import asyncio
import ast
import numpy as np
import logging
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import (
    fetch_data,
    describe_via_wcps,
    get_encoding_from_axis_attributes,
    get_variables_from_coverage_metadata,
    get_attributes_from_time_axis,
    ymd_to_cftime_value,
    cftime_value_to_ymd,
)
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    get_coverage_encodings,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
)
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv
from . import routes

logger = logging.getLogger(__name__)

cmip6_api = Blueprint("cmip6_api", __name__)

cmip6_monthly_coverage_id = "cmip6_monthly_cf"


async def get_cmip6_metadata():
    """Get the coverage metadata and encodings for CMIP6 monthly coverage"""
    metadata = await describe_via_wcps(cmip6_monthly_coverage_id)
    return metadata


metadata = asyncio.run(get_cmip6_metadata())
base_date, time_min, time_max = get_attributes_from_time_axis(metadata)

coverage_metadata = {
    "variables": get_variables_from_coverage_metadata(metadata),
    "model_encoding": get_encoding_from_axis_attributes("model", metadata),
    "scenario_encoding": get_encoding_from_axis_attributes("scenario", metadata),
    "start_cf_time": time_min,
    "end_cf_time": time_max,
    "start_date": cftime_value_to_ymd(time_min, base_date),
    "end_date": cftime_value_to_ymd(time_max, base_date),
}


async def fetch_cmip6_monthly_point_data(lat, lon, vars=None, time_slice=None):
    """
    Make an async request for CMIP6 monthly data for a range of models, scenarios, and years at a specified point

    Args:
        lat (float): latitude
        lon (float): longitude
        vars (str): comma-separated variable names, if specified
        time_slice (str): time slice in CF units, if requested

    Returns:
        list of data results from each of historical and future data at a specified point
    """

    # We must use EPSG:4326 for the CMIP6 monthly coverage to match the coverage projection
    wcs_str = generate_wcs_getcov_str(
        lon,
        lat,
        cov_id=cmip6_monthly_coverage_id,
        projection="EPSG:4326",
        var_coord=None,
        time_slice=("time", time_slice),
    )

    # Generate the URL for the WCS query
    url = generate_wcs_query_url(wcs_str)

    # If a specific variable is requested, add the RANGESUBSET to the URL
    if vars is not None:
        var_str = ",".join(vars)
        url += f"&RANGESUBSET={var_str}"

    # Fetch the data
    point_data_list = await fetch_data([url])

    return point_data_list


def package_cmip6_monthly_data(
    point_data_list, coverage_metadata, vars, start_year, end_year
):
    """
    Package the CMIP6 monthly values into human-readable JSON format

    Args:
        point_data_list (list): nested list of data from Rasdaman WCPS query
        coverage_metadata (dict): metadata and encodings for the coverage
        vars (list): list of variable names, if specified
        start_year (int): start year
        end_year (int): end year

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from coverage metadata
    """
    di = dict()

    # get vars from coverage metadata if not specified
    if vars is None:
        vars = coverage_metadata["variables"]

    # create a list of time values, in "YYYY-MM" format, for the full coverage time range
    times = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            times.append(f"{year}-{str(month).zfill(2)}")

    # reverse the model and scenario encodings dicts to create lists of their names from the dict values
    models = [v for _k, v in coverage_metadata["model_encoding"].items()]
    scenarios = [v for _k, v in coverage_metadata["scenario_encoding"].items()]

    # first level of the nested point_data_list is model (should be 14)
    # second level is scenario (should be 5)
    # third is a large list of strings:
    #   each string represents a month in the time range requested (start to end year)
    #   each string is a space-separated list of values, one for each variable requested

    for mi, model_li in enumerate(point_data_list):
        model = models[mi]
        if model not in di:
            di[model] = dict()

        for si, scenario_li in enumerate(model_li):
            scenario = scenarios[si]
            if scenario not in di[model]:
                di[model][scenario] = dict()

            for toi, time_str in enumerate(scenario_li):
                time = times[toi]
                di[model][scenario][time] = dict()

                # split the space-separated string of values into a list
                # first check if its a float (occurs if only 1 variable is requested)
                if isinstance(time_str, float):
                    value_list = [str(time_str)]
                else:
                    value_list = time_str.split(" ")
                for vi, varname in enumerate(vars):
                    value = value_list[vi]

                    # clean data
                    # replace "null" or None with np.nan -> these will be pruned from the response
                    if value == "null" or value is None:
                        value = np.nan
                    # Evaporation has very tiny values.
                    if varname == "evspsbl":
                        precision = 8
                    else:
                        precision = 3

                    di[model][scenario][time][varname] = round(float(value), precision)

    # Responses from Rasdaman include the same array length for both
    # historical and projected data, representing every possible year
    # in the request. This means both the historical and projected data
    # arrays may include nodata years populated with NaNs if the year range
    # spans 2014 -2015 (2014 is the last year for historical data, and
    # 2015 is the first year of projected data).

    # The code below replaces NaNs with -9999 for nodata years depending on year.
    # If the scenario is historical and the year is greater than 2014,
    # all NaN values are replaced with -9999 and will be pruned from the response.
    # If the scenario is not historical, and the year is less than 2015,
    # all NaN values are replaced with -9999 and will be pruned from the response.

    for model, scenarios in di.items():
        for scenario, months in scenarios.items():
            for month, variables in months.items():
                for variable, value in variables.items():
                    if scenario == "historical" and int(month[:4]) > 2014:
                        if value == np.nan:
                            di[model][scenario][month][variable] = -9999
                    elif scenario != "historical" and int(month[:4]) < 2015:
                        if value == np.nan:
                            di[model][scenario][month][variable] = -9999

    # We can also see entire nodata years in the projected data if a specific
    # scenario did not include data for a particular variable.
    # Iterate through the dictionary again, and check if all values for all variables in a scenario are np.nan
    # If so, replace all those np.nan values with -9999
    for model, scenarios in di.items():
        for scenario, months in scenarios.items():
            for month, variables in months.items():
                all_nan = True
                for variable, value in variables.items():
                    if value != np.nan:
                        all_nan = False
                        break
                if all_nan:
                    for variable in variables:
                        di[model][scenario][month][variable] = -9999

    di = prune_nulls_with_max_intensity(di)

    return di


@routes.route("/cmip6/")
def cmip6_about():
    return render_template("/documentation/cmip6.html")


@routes.route("/cmip6/references")
def cmip6_references():
    return render_template("/documentation/cmip6_refs.html")


@routes.route("/cmip6/point/<lat>/<lon>")
@routes.route("/cmip6/point/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_cmip6_monthly_point_data(lat, lon, start_year=None, end_year=None):
    """
    Query the CMIP6 monthly coverage

    Args:
        lat (float): latitude
        lon (float): longitude
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested CMIP6 monthly data

    Notes:
        example request (all variables): http://localhost:5000/cmip6/point/65.06/-146.16
        example request (select variables): http://localhost:5000/cmip6/point/65.06/-146.16?vars=tas,pr
        example request (all variables, select years): http://localhost:5000/cmip6/point/65.06/-146.16/2000/2005
        example request (select variables, select years): http://localhost:5000/cmip6/point/65.06/-146.16/2000/2005?vars=tas,pr

    """

    # Validate the request start and end years against the coverage time range
    # and create the time slice for the WCPS query
    if None in [start_year, end_year]:
        # use full range if no years requested
        start_year = coverage_metadata["start_date"][0]
        end_year = coverage_metadata["end_date"][0]
        time_slice_cf = (
            str(coverage_metadata["start_cf_time"])
            + ","
            + str(coverage_metadata["end_cf_time"])
        )
    elif None not in [start_year, end_year]:
        # basic validation of year inputs:
        # ensure start_year and end_year are integers in the right order
        start_year, end_year = int(start_year), int(end_year)
        if start_year > end_year:
            return render_template("400/bad_request.html"), 400
        # check the requested years against the coverage range years
        if (
            start_year >= coverage_metadata["start_date"][0]
            and end_year <= coverage_metadata["end_date"][0]
        ):
            # convert to cftime values for the WCPS query
            start_cf_time = ymd_to_cftime_value(start_year, 1, 1, base_date=base_date)
            end_cf_time = ymd_to_cftime_value(end_year, 12, 31, base_date=base_date)
            time_slice_cf = str(start_cf_time) + "," + str(end_cf_time)
        else:
            return (
                render_template(
                    "422/invalid_year.html",
                    start_year=start_year,
                    end_year=end_year,
                    min_year=coverage_metadata["start_date"][0],
                    max_year=coverage_metadata["end_date"][0],
                ),
                422,
            )

    # Validate the lat/lon values
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    cmip6_bbox = construct_latlon_bbox_from_coverage_bounds(metadata)
    within_bounds = validate_latlon_in_bboxes(
        lat, lon, [cmip6_bbox], [cmip6_monthly_coverage_id]
    )
    if within_bounds == 422:
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html", bboxes=[cmip6_bbox]
            ),
            422,
        )

    # Validate requested variables against those available in the coverage
    if request.args.get("vars"):
        vars = request.args.get("vars").split(",")
        for var_id in vars:
            if var_id not in coverage_metadata["variables"]:
                return render_template("400/bad_request.html"), 400
    else:
        vars = None

    # Fetch and package the data
    try:
        point_data_list = asyncio.run(
            fetch_cmip6_monthly_point_data(lat, lon, vars, time_slice=time_slice_cf)
        )

        results = package_cmip6_monthly_data(
            point_data_list, coverage_metadata, vars, start_year, end_year
        )

        results = prune_nulls_with_max_intensity(postprocess(results, "cmip6_monthly"))
    except:
        return render_template("500/server_error.html"), 500

    if request.args.get("format") == "csv":
        try:
            # if no specific var(s) requested, find all unique vars in the results after pruning
            # we need to pass this list explicitly to create_csv since results from land- or sea-only variables may be missing
            if vars is None:
                vars = []
                for model in results:
                    for scenario in results[model]:
                        for month in results[model][scenario]:
                            for var in results[model][scenario][month]:
                                # append only if not already in list
                                if var not in vars:
                                    vars.append(var)
            logger.debug(f"Results limited to {vars}")

            place_id = request.args.get("community")
            return create_csv(
                results,
                "cmip6_monthly",
                place_id,
                lat,
                lon,
                vars=vars,
                start_year=start_year,
                end_year=end_year,
            )
        except:
            return render_template("500/server_error.html"), 500

    return results
