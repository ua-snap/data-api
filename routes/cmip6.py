import asyncio
import ast
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    get_coverage_encodings,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
)
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv
from . import routes

cmip6_api = Blueprint("cmip6_api", __name__)

cmip6_monthly_coverage_id = "cmip6_monthly"


async def get_cmip6_metadata():
    """Get the coverage metadata and encodings for CMIP6 monthly coverage"""
    metadata = await describe_via_wcps(cmip6_monthly_coverage_id)
    return metadata


metadata = asyncio.run(get_cmip6_metadata())
dim_encodings = get_coverage_encodings(metadata)
# TODO: fix cryo coverage so we can delete this line below
# temporary fix for "dictionary inside a string" issue
for dim, value in dim_encodings.items():
    if isinstance(value, str):
        dim_encodings[dim] = ast.literal_eval(value)
    else:
        pass

varnames = dim_encodings["varname"]


async def fetch_cmip6_monthly_point_data(lat, lon, var_coord=None, time_slice=None):
    """
    Make an async request for CMIP6 monthly data for a range of models, scenarios, and years at a specified point

    Args:
        lat (float): latitude
        lon (float): longitude
        var_coord (int): variable coordinate from dim_encoding, if specified
        time_slice (str): time slice for the data request, if specified

    Returns:
        list of data results from each of historical and future data at a specified point
    """

    # We must use EPSG:4326 for the CMIP6 monthly coverage to match the coverage projection
    wcs_str = generate_wcs_getcov_str(
        lon,
        lat,
        cov_id=cmip6_monthly_coverage_id,
        projection="EPSG:4326",
        var_coord=var_coord,
        time_slice=time_slice,
    )

    # Generate the URL for the WCS query
    url = generate_wcs_query_url(wcs_str)

    # Fetch the data
    point_data_list = await fetch_data([url])

    return point_data_list


def package_cmip6_monthly_data(
    point_data_list, var_id=None, start_year=None, end_year=None
):
    """
    Package the CMIP6 monthly values into human-readable JSON format

    Args:
        point_data_list (list): nested list of data from Rasdaman WCPS query
        var_id (str): variable name, if specified
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from dim_encodings global variable
    """
    di = dict()

    # Nest point_data_list one level deeper if var_id is specified.
    # This keeps the nesting level the same for all cases.
    if var_id != None:
        point_data_list = [point_data_list]

    for var_coord, var_li in enumerate(point_data_list):
        if var_id != None:
            varname = var_id
        else:
            varname = dim_encodings["varname"][var_coord]

        for mi, model_li in enumerate(var_li):
            model = dim_encodings["model"][mi]

            if model not in di:
                di[model] = dict()

            for si, scenario_li in enumerate(model_li):
                # Rasdaman Enterprise (as of v10.4.7) returns missing model/scenario NoData gaps
                # as arrays full of 0s, which get converted to 0.0 somewhere along the way.
                # Treat any array that is full of nothing but 0.0s as NoData and skip over it.
                if all(value == 0.0 for value in scenario_li):
                    continue

                scenario = dim_encodings["scenario"][si]
                if scenario not in di[model]:
                    di[model][scenario] = dict()

                # Create an array of every month between start and end year in the format "YYYY-MM"
                # if no start or end year given, use 1950 and 2100
                if None in [start_year, end_year]:
                    months = [
                        f"{year}-{str(month).zfill(2)}"
                        for year in range(1950, 2100 + 1)
                        for month in range(1, 13)
                    ]
                else:
                    months = [
                        f"{year}-{str(month).zfill(2)}"
                        for year in range(int(start_year), int(end_year) + 1)
                        for month in range(1, 13)
                    ]

                for soi, value in enumerate(scenario_li):

                    # replace NaN values (None) with -9999
                    if value is None:
                        value = -9999

                    month = months[soi]
                    if month not in di[model][scenario]:
                        di[model][scenario][month] = dict()

                    # The "ts" variable is still in Kelvin in the Rasdaman coverage.
                    # Convert this to Celsius. All other temperature variables have
                    # already been converted to Celsius before Rasdaman import.
                    if varname == "ts" and float(value) != -9999:
                        value = float(value) - 273.15

                    # Evaporation has very tiny values.
                    if varname == "evspsbl":
                        precision = 8
                    else:
                        precision = 2

                    di[model][scenario][month][varname] = round(float(value), precision)

    # Responses from Rasdaman include the same array length for both
    # historical and projected data, representing every possible year
    # in the request. This means both the historical and projected data
    # arrays may include nodata years populated with 0s if the year range
    # spans 2014 -2015 (2014 is the last year for historical data, and
    # 2015 is the first year of projected data).

    # The code below replaces 0s with -9999 for nodata years depending on year.
    # If the scenario is historical and the year is greater than 2014,
    # all 0 values are replaced with -9999 and will be pruned from the response.
    # If the scenario is not historical, and the year is less than 2015,
    # all 0 values are replaced with -9999 and will be pruned from the response.

    for model, scenarios in di.items():
        for scenario, months in scenarios.items():
            for month, variables in months.items():
                for variable, value in variables.items():
                    if scenario == "historical" and int(month[:4]) > 2014:
                        if value == 0:
                            di[model][scenario][month][variable] = -9999
                    elif scenario != "historical" and int(month[:4]) < 2015:
                        if value == 0:
                            di[model][scenario][month][variable] = -9999

    # We can also see entire nodata years in the projected data if a specific
    # scenario did not include data for a particular variable.
    # For example, try the URL below and examine the "HadGEM3-GC31-MM" model response:
    # http://127.0.0.1:5000/cmip6/point/61.5/-147/2014/2015?vars=pr

    # This is a difficult issue to solve, as we can't safely replace all 0s with -9999
    # because in some variables, that might actually be reasonable data.
    # For example, snow depth or sea ice concentration may really be 0 in all
    # months for a particular year!

    # TODO: find the best approach for handling nodata years in projected data.
    # The brute force approach (replacing all 0s with -9999) is not safe for all variables.

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

    """
    # Validate the start and end years
    if None in [start_year, end_year]:
        time_slice_ansi = None
    elif None not in [start_year, end_year]:
        if int(start_year) >= 1950 and int(end_year) <= 2100:
            start_year_ansi = f"{start_year}-01-15T12:00:00.000Z"
            end_year_ansi = f"{end_year}-12-15T12:00:00.000Z"
            time_slice_ansi = ("ansi", f'"{start_year_ansi}","{end_year_ansi}"')
        else:
            return (
                render_template(
                    "422/invalid_year.html",
                    start_year=start_year,
                    end_year=end_year,
                    min_year=1950,
                    max_year=2100,
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
    try:
        var_parameter = False
        if request.args.get("vars"):
            var_parameter = True
            vars = request.args.get("vars").split(",")
            for var_id in vars:
                if var_id not in varnames.values():
                    return render_template("400/bad_request.html"), 400
        else:
            vars = None

        if var_parameter:
            results = {}
            for var_id in vars:
                var_coord = next(
                    key for key, value in varnames.items() if value == var_id
                )
                point_data_list = asyncio.run(
                    fetch_cmip6_monthly_point_data(
                        lat, lon, var_coord, time_slice=time_slice_ansi
                    )
                )

                new_results = package_cmip6_monthly_data(
                    point_data_list, var_id, start_year, end_year
                )

                for model, scenarios in new_results.items():
                    results.setdefault(model, {})
                    for scenario, months in scenarios.items():
                        results[model].setdefault(scenario, {})
                        for month, variables in months.items():
                            results[model][scenario].setdefault(month, {})
                            results[model][scenario][month].update(variables)
        else:
            point_data_list = asyncio.run(
                fetch_cmip6_monthly_point_data(lat, lon, time_slice=time_slice_ansi)
            )

            results = package_cmip6_monthly_data(
                point_data_list, start_year=start_year, end_year=end_year
            )

        results = prune_nulls_with_max_intensity(postprocess(results, "cmip6_monthly"))

        # if no specific var(s) requested, find all unique vars in the results after pruning
        # we need to pass this list explicitly to create_csv since land- or sea-only variables may be missing
        if vars is None:
            vars = []
            for model in results:
                for scenario in results[model]:
                    for month in results[model][scenario]:
                        for var in results[model][scenario][month]:
                            # append only if not already in list
                            if var not in vars:
                                vars.append(var)

        print(f"Results limited to {vars}")

        if request.args.get("format") == "csv":
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

        return results

    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
