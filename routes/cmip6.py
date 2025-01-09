import asyncio
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from validate_request import validate_latlon, get_coverage_encodings
from postprocessing import postprocess, prune_nulls_with_max_intensity
from csv_functions import create_csv
from . import routes
from config import WEST_BBOX, EAST_BBOX

cmip6_api = Blueprint("cmip6_api", __name__)

cmip6_monthly_coverage_id = "cmip6_monthly"#_cryo_test"


async def get_cmip6_metadata():
    """Get the coverage metadata and encodings for CMIP6 monthly coverage"""
    metadata = await describe_via_wcps(cmip6_monthly_coverage_id)

    return get_coverage_encodings(metadata)


dim_encodings = asyncio.run(get_cmip6_metadata())
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

    print(url)

    # Fetch the data
    point_data_list = await fetch_data([url])

    return point_data_list


def package_cmip6_monthly_data(point_data_list, var_id=None, start_year=None, end_year=None):
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

    return di


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
                    fetch_cmip6_monthly_point_data(lat, lon, var_coord, time_slice=time_slice_ansi)
                )
                new_results = package_cmip6_monthly_data(point_data_list, var_id, start_year, end_year)
                for model, scenarios in new_results.items():
                    results.setdefault(model, {})
                    for scenario, months in scenarios.items():
                        results[model].setdefault(scenario, {})
                        for month, variables in months.items():
                            results[model][scenario].setdefault(month, {})
                            results[model][scenario][month].update(variables)
        else:
            point_data_list = asyncio.run(fetch_cmip6_monthly_point_data(lat, lon, time_slice=time_slice_ansi))
            results = package_cmip6_monthly_data(point_data_list, start_year=start_year, end_year=end_year)

        results = prune_nulls_with_max_intensity(postprocess(results, "cmip6_monthly"))

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(results, "cmip6_monthly", place_id, lat, lon, vars=vars)

        return results
    except ValueError:
        return render_template("400/bad_request.html"), 400
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
