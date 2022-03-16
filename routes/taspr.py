import asyncio
import io
import csv
import time
import itertools
from urllib.parse import quote
import numpy as np
import xarray as xr
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)

# local imports
from generate_requests import generate_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_data,
    get_from_dict,
    summarize_within_poly,
)
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
)
from validate_data import get_poly_3338_bbox, postprocess
from luts import type_di
from config import WEST_BBOX, EAST_BBOX
from . import routes

taspr_api = Blueprint("taspr_api", __name__)


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
                        varname: None if value is None else round(value, dim_encodings["rounding"][varname])
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
                    varname: None if value is None else round(value, dim_encodings["rounding"][varname])
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
            request_str = generate_wcs_getcov_str(x, y, cov_id, var_coord, encoding)
        urls.append(generate_wcs_query_url(request_str))

    start_time = time.time()
    data_list = await fetch_data(urls)
    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )

    # create xarray.DataSet from bytestring
    ds_list = [xr.open_dataset(io.BytesIO(netcdf_bytes)) for netcdf_bytes in data_list]

    return ds_list


def create_csv(packaged_data):
    """
    Returns a CSV version of the fetched data, as a string.

    Args:
        packaged_data (json): JSON-like data pakage output
            from the run_fetch_* and run_aggregate_* functions

    Returns:
        string of CSV data
    """
    output = io.StringIO()

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
                                    "value": packaged_data[ar5_period][season][model][
                                        scenario
                                    ][varname],
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

    return output.getvalue()


def return_csv(csv_data):
    """Return the CSV data as a download

    Args:
        csv_data (?): csv data created with create_csv() function

    Returns:
        CSV Response
    """
    response = Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Type": 'text/csv; name="climate.csv"',
            "Content-Disposition": 'attachment; filename="climate.csv"',
        },
    )

    return response


def combine_pkg_dicts(tas_di, pr_di):
    """combine and return to packaed data dicts,
    for combining tas and pr individual endpoint results

    Args:
        di1 (dict): result dict from point or HUC query for temperature
        di2 (dict): result dict from point or HUC query for precip

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


def run_fetch_var_point_data(var_ep, lat, lon):
    """Run the async tas/pr data requesting for a single point
    and return data as json

    Args:
        varname (str): Abbreviation name for variable of interest,
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
    # use CRU as basis for combined point package for chronolical consistency
    # order of listing: CRU (1950-2009), AR5 2040-2069 summary,
    #     AR5 2070-2099 summary, AR5 seasonal data
    # query CRU baseline summary
    cov_ids, summary_decades = make_fetch_args()
    point_data_list = asyncio.run(
        fetch_point_data(x, y, var_coord, cov_ids, summary_decades)
    )

    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = {}
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
    temperature and precipitation andpoints

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


def run_aggregate_allvar_polygon(poly_gdf, poly_id):
    """Get data summary (e.g. zonal mean) within a Polygon for all variables."""
    tas_pkg, pr_pkg = [
        run_aggregate_var_polygon(var_ep, poly_gdf, poly_id)
        for var_ep in ["temperature", "precipitation"]
    ]
    combined_pkg = combine_pkg_dicts(tas_pkg, pr_pkg)
    return combined_pkg


def run_aggregate_var_polygon(var_ep, poly_gdf, poly_id):
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
    poly = get_poly_3338_bbox(poly_gdf, poly_id)
    # mapping between coordinate values (ints) and variable names (strs)
    varname = var_ep_lu[var_ep]
    var_coord = list(dim_encodings["varnames"].keys())[list(dim_encodings["varnames"].values()).index(varname)]
    # fetch data within the Polygon bounding box
    cov_ids, summary_decades = make_fetch_args()
    ds_list = asyncio.run(fetch_bbox_netcdf(*poly.bounds, var_coord, cov_ids, summary_decades))
    # average over the following decades / time periods
    aggr_results = {}
    summary_periods = ["1950_2009", "2040_2069", "2070_2099"]
    for ds, period in zip(ds_list[:-1], summary_periods):
        aggr_results[period] = summarize_within_poly(ds, poly, dim_encodings, "Gray", varname)
    ar5_results = summarize_within_poly(ds_list[-1], poly, dim_encodings, "Gray", varname)
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


@routes.route("/taspr/huc/")
@routes.route("/temperature/huc/")
@routes.route("/precipitation/huc/")
def about_huc():
    return render_template("taspr/huc.html")

@routes.route("/taspr/protectedarea/")
@routes.route("/temperature/protectedarea/")
@routes.route("/precipitation/protectedarea/")
def taspr_about_protectedarea():
    return render_template("taspr/protectedarea.html")


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
        return render_template("422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX), 422

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
        csv_data = create_csv(point_pkg)
        return return_csv(csv_data)

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
            poly_pkg = run_aggregate_var_polygon(var_ep, type_di[poly_type], var_id)
        elif var_ep == "taspr":
            poly_pkg = run_aggregate_allvar_polygon(type_di[poly_type], var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    if request.args.get("format") == "csv":
        csv_data = create_csv(poly_pkg)
        return return_csv(csv_data)

    return postprocess(poly_pkg, "taspr")