import asyncio
import io
import csv
import time
import itertools
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
from generate_requests import generate_wcs_getcov_str, generate_average_wcps_str
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_data,
    get_from_dict,
    summarize_within_poly,
    get_dim_encodings,
)
from validate_request import (
    validate_latlon,
    validate_huc8,
    validate_akpa,
    project_latlon,
)
from validate_data import get_poly_3338_bbox, postprocess
from luts import huc8_gdf, akpa_gdf
from config import WEST_BBOX, EAST_BBOX
from . import routes

alfresco_api = Blueprint("alfresco_api", __name__)


# create encodings for coverages (currently all coverages share encodings)
future_dim_encodings = asyncio.run(get_dim_encodings("relative_flammability_future"))
historical_dim_encodings = asyncio.run(
    get_dim_encodings("relative_flammability_historical")
)

var_ep_lu = {
    "flammability": {"cov_id_str": "relative_flammability", "varname": "rf",},
    "veg_change": {"cov_id_str": "relative_vegetation_change", "varname": "rvc",},
}


# def make_fetch_args():
#     """Fixed helper function for ensuring
#     consistency between point and HUC queries
#     """
#     cov_ids = [
#         "iem_cru_2km_taspr_seasonal_baseline_stats",
#         "iem_ar5_2km_taspr_seasonal",
#         "iem_ar5_2km_taspr_seasonal",
#         "iem_ar5_2km_taspr_seasonal",
#     ]
#     summary_decades = [None, (3, 5), (6, 8), None]

#     return cov_ids, summary_decades


async def fetch_alf_point_data(x, y, var_ep):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        var_ep (str): alfresco endpoint name (flammability or veg_change)

    Returns:
        list of data results from each of historical and future coverages
    """
    # set up WCS request strings
    request_strs = []
    # historical data request string
    request_strs.append(
        generate_wcs_getcov_str(x, y, f"{var_ep_lu[var_ep]['cov_id_str']}_historical")
    )
    # generate both future average requests (averages over decades)
    cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
    for coords in [(3, 5), (6, 8)]:
        request_strs.append(
            generate_average_wcps_str(x, y, f"{cov_id_str}_future", "era", coords)
        )
    # future non-average request str
    request_strs.append(generate_wcs_getcov_str(x, y, f"{cov_id_str}_future"))

    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    point_data_list = await fetch_data(urls)

    return point_data_list


def package_historical_alf_point_data(point_data, varname):
    """Add dim names to JSON response from point query
    for the historical coverages

    Args:
        point_data (list): nested list containing JSON
            results of historical alfresco point query
        varname (str): variable name

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = {}
    # hard-code summary period for CRU
    for ei, value in enumerate(point_data):  # (nested list with value at dim 0)
        era = historical_dim_encodings["era"][ei]
        point_data_pkg[era] = {"CRU-TS40": {"CRU_historical": {varname: value}}}

    return point_data_pkg


def package_ar5_alf_point_data(point_data, varname):
    """Add dim names to JSON response from typical AR5 point query

    Args:
        point_data (list): nested list containing JSON
            results of AR5 WCPS query
        varname (str): variable name

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = {}
    # AR5 data:
    # era, model, scenario
    for ei, m_li in enumerate(point_data):  # (nested list with model at dim 0)
        era = future_dim_encodings["era"][ei]
        point_data_pkg[era] = {}
        for mi, s_li in enumerate(m_li):  # (nested list with scenario at dim 0)
            model = future_dim_encodings["model"][mi]
            point_data_pkg[era][model] = {}
            for si, value in enumerate(s_li):
                scenario = future_dim_encodings["scenario"][si]
                point_data_pkg[era][model][scenario] = {varname: value}

    return point_data_pkg


def package_ar5_alf_averaged_point_data(point_data, varname):
    """Add dim names to JSON response from WCPS point query
    for the AR5/CMIP5 coverages

    Args:
        point_data (list): nested list containing JSON
            results of AR5 WCPS query
        varname (str): variable name

    Returns:
        JSON-like dict of query results
    """
    point_data_pkg = {}
    # AR5 data:
    # model, scenario
    for mi, s_li in enumerate(point_data):  # (nested list with scenario at dim 0)
        model = future_dim_encodings["model"][mi]
        point_data_pkg[model] = {}
        for si, value in enumerate(s_li):
            scenario = future_dim_encodings["scenario"][si]
            point_data_pkg[model][scenario] = {varname: round(value, 4)}

    return point_data_pkg


# async def fetch_bbox_netcdf(x1, y1, x2, y2, var_coord, cov_ids, summary_decades):
#     """Make the async request for the data within the specified bbox

#     Args:
#         x1 (float): lower x-coordinate bound
#         y1 (float): lower y-coordinate bound
#         x2 (float): upper x-coordinate bound
#         y2 (float): upper y-coordinate bound
#         var_coord (int): coordinate value corresponding to varname to query
#         cov_ids (str): list of Coverage ids to fetch the same bbox over
#         summary_decades (list): list of either None or 2-tuples of integers
#             mapped to desired range of decades to summarise over,
#             e.g. (6, 8) for 2070-2099. List items need to
#             correspond to items in cov_ids.

#     Returns:
#         xarray.DataSet containing results of WCS netCDF query
#     """
#     encoding = "netcdf"

#     urls = []
#     for cov_id, decade_tpl in zip(cov_ids, summary_decades):
#         if decade_tpl:
#             # if summary decades are given, create a WCPS request string
#             x = f"{x1}:{x2}"
#             y = f"{y1}:{y2}"
#             request_str = get_wcps_request_str(
#                 x, y, var_coord, cov_id, decade_tpl, encoding
#             )
#         else:
#             # otheriwse use generic WCS request str
#             x = f"{x1},{x2}"
#             y = f"{y1},{y2}"
#             request_str = generate_wcs_getcov_str(x, y, cov_id, var_coord, encoding)
#         urls.append(generate_wcs_query_url(request_str))

#     start_time = time.time()
#     data_list = await fetch_data(urls)
#     app.logger.info(
#         f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
#     )

#     # create xarray.DataSet from bytestring
#     ds_list = [xr.open_dataset(io.BytesIO(netcdf_bytes)) for netcdf_bytes in data_list]

#     return ds_list


# def create_csv(packaged_data):
#     """
#     Returns a CSV version of the fetched data, as a string.

#     Args:
#         packaged_data (json): JSON-like data pakage output
#             from the run_fetch_* and run_aggregate_* functions

#     Returns:
#         string of CSV data
#     """
#     output = io.StringIO()

#     fieldnames = [
#         "variable",
#         "date_range",
#         "season",
#         "model",
#         "scenario",
#         "stat",
#         "value",
#     ]
#     writer = csv.DictWriter(output, fieldnames=fieldnames)

#     writer.writeheader()

#     # add CRU data
#     cru_period = "1950_2009"
#     for season in dim_encodings["seasons"].values():
#         for varname in ["pr", "tas"]:
#             for stat in dim_encodings["stats"].values():
#                 try:
#                     writer.writerow(
#                         {
#                             "variable": varname,
#                             "date_range": cru_period,
#                             "season": season,
#                             "model": "CRU-TS40",
#                             "scenario": "Historical",
#                             "stat": stat,
#                             "value": packaged_data[cru_period][season]["CRU-TS40"][
#                                 "CRU_historical"
#                             ][varname][stat],
#                         }
#                     )
#                 except KeyError:
#                     # if single var query, just ignore attempts to
#                     # write the non-chosen var
#                     pass

#     # AR5 periods
#     for ar5_period in ["2040_2069", "2070_2099"]:
#         for season in dim_encodings["seasons"].values():
#             for model in dim_encodings["models"].values():
#                 for scenario in dim_encodings["scenarios"].values():
#                     for varname in ["pr", "tas"]:
#                         try:
#                             writer.writerow(
#                                 {
#                                     "variable": varname,
#                                     "date_range": ar5_period,
#                                     "season": season,
#                                     "model": model,
#                                     "scenario": scenario,
#                                     "stat": "mean",
#                                     "value": packaged_data[ar5_period][season][model][
#                                         scenario
#                                     ][varname],
#                                 }
#                             )
#                         except KeyError:
#                             # if single var query, just ignore attempts to
#                             # write the non-chosen var
#                             pass

#     for decade in dim_encodings["decades"].values():
#         for season in dim_encodings["seasons"].values():
#             for model in dim_encodings["models"].values():
#                 for scenario in dim_encodings["scenarios"].values():
#                     for varname in ["pr", "tas"]:
#                         try:
#                             writer.writerow(
#                                 {
#                                     "variable": varname,
#                                     "date_range": decade,
#                                     "season": season,
#                                     "model": model,
#                                     "scenario": scenario,
#                                     "stat": "mean",
#                                     "value": packaged_data[decade][season][model][
#                                         scenario
#                                     ][varname],
#                                 }
#                             )
#                         except KeyError:
#                             # if single var query, just ignore attempts to
#                             # write the non-chosen var
#                             pass

#     return output.getvalue()


# def return_csv(csv_data):
#     """Return the CSV data as a download

#     Args:
#         csv_data (?): csv data created with create_csv() function

#     Returns:
#         CSV Response
#     """
#     response = Response(
#         csv_data,
#         mimetype="text/csv",
#         headers={
#             "Content-Type": 'text/csv; name="climate.csv"',
#             "Content-Disposition": 'attachment; filename="climate.csv"',
#         },
#     )

#     return response


# def run_fetch_point_data(lat, lon):
#     """Fetch and combine point data for both
#     temperature and precipitation andpoints

#     Args:
#         lat (float): latitude
#         lon (float): longitude

#     Returns:
#         JSON-like dict of data at provided latitude and
#         longitude
#     """
#     tas_pkg, pr_pkg = [
#         run_fetch_var_point_data(var_ep, lat, lon)
#         for var_ep in ["temperature", "precipitation"]
#     ]

#     combined_pkg = combine_pkg_dicts(tas_pkg, pr_pkg)

#     return combined_pkg


# def run_aggregate_allvar_polygon(poly_gdf, poly_id):
#     """Get data summary (e.g. zonal mean) within a Polygon for all variables."""
#     tas_pkg, pr_pkg = [run_aggregate_var_polygon(var_ep, poly_gdf, poly_id) for var_ep in ["temperature", "precipitation"]]
#     combined_pkg = combine_pkg_dicts(tas_pkg, pr_pkg)
#     return combined_pkg

# def run_aggregate_var_polygon(var_ep, poly_gdf, poly_id):
#     """Get data summary (e.g. zonal mean) of single variable in polygon.

#     Args:
#         var_ep (str): Data variable. One of 'taspr', 'temperature', or 'precipitation'.
#         poly_gdf (GeoDataFrame): the object from which to fetch the polygon, e.g. the HUC 8 geodataframe for watershed polygons
#         poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal mean.

#     Returns:
#         aggr_results (dict): data representing zonal means within the polygon.

#     Notes:
#         Fetches data on the individual instances of the singular dimension combinations. Consider validating polygon IDs in `validate_data` or `lat_lon` module.
#     """
#     poly = get_poly_3338_bbox(poly_gdf, poly_id)
#     # mapping between coordinate values (ints) and variable names (strs)
#     varname = var_ep_lu[var_ep]
#     var_coord = list(dim_encodings["varnames"].keys())[list(dim_encodings["varnames"].values()).index(varname)]
#     # fetch data within the Polygon bounding box
#     cov_ids, summary_decades = make_fetch_args()
#     ds_list = asyncio.run(fetch_bbox_netcdf(*poly.bounds, var_coord, cov_ids, summary_decades))
#     # average over the following decades / time periods
#     aggr_results = {}
#     summary_periods = ["1950_2009", "2040_2069", "2070_2099"]
#     for ds, period in zip(ds_list[:-1], summary_periods):
#         aggr_results[period] = summarize_within_poly(ds, poly, dim_encodings, "Gray", varname)
#     ar5_results = summarize_within_poly(ds_list[-1], poly, dim_encodings, "Gray", varname)
#     for decade, summaries in ar5_results.items():
#         aggr_results[decade] = summaries
#     #  add the model, scenario, and varname levels for CRU
#     for season in aggr_results[summary_periods[0]]:
#         aggr_results[summary_periods[0]][season] = {
#             "CRU-TS40": {
#                 "CRU_historical": {varname: aggr_results[summary_periods[0]][season]}
#             }
#         }
#     # add the varnames for AR5
#     for period in summary_periods[1:] + list(dim_encodings["decades"].values()):
#         for season in aggr_results[period]:
#             for model in aggr_results[period][season]:
#                 for scenario in aggr_results[period][season][model]:
#                     aggr_results[period][season][model][scenario] = {
#                         varname: aggr_results[period][season][model][scenario]
#                     }
#     return aggr_results


@routes.route("/alfresco/")
@routes.route("/alfresco/abstract/")
def alfresco_about():
    return render_template("alfresco/abstract.html")


@routes.route("/alfresco/flammability/point/")
def rel_flam_about_point():
    return render_template("alfresco/flam_point.html")


@routes.route("/alfresco/veg_change/point/")
def rel_veg_change_about_point():
    return render_template("alfresco/veg_point.html")


# @routes.route("/taspr/huc/")
# @routes.route("/temperature/huc/")
# @routes.route("/precipitation/huc/")
# def about_huc():
#     return render_template("taspr/huc.html")

# @routes.route("/taspr/protectedarea/")
# @routes.route("/temperature/protectedarea/")
# @routes.route("/precipitation/protectedarea/")
# def taspr_about_protectedarea():
#     return render_template("taspr/protectedarea.html")


@routes.route("/alfresco/<var_ep>/point/<lat>/<lon>")
def run_fetch_alf_point_data(var_ep, lat, lon):
    """Point data endpoint. Fetch point data for
    specified lat/lon and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either flammability or veg_change
        lat (float): latitude
        lon (float): longitude
        
    Returns:
        JSON-like dict of relative flammability data
        from IEM ALFRESCO outputs

    Notes:
        example request: http://localhost:5000/flammability/point/65.0628/-146.1627
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

    x, y = project_latlon(lat, lon, 3338)

    if var_ep in var_ep_lu.keys():
        try:
            point_data_list = asyncio.run(fetch_alf_point_data(x, y, var_ep))
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500
    else:
        return render_template("400/bad_request.html"), 400

    varname = var_ep_lu[var_ep]["varname"]
    point_pkg = package_historical_alf_point_data(point_data_list[0], varname)
    point_pkg["2040-2069"] = package_ar5_alf_averaged_point_data(
        point_data_list[1], varname
    )
    point_pkg["2070-2099"] = package_ar5_alf_averaged_point_data(
        point_data_list[2], varname
    )
    # package AR5 data and fold into data pakage
    ar5_point_pkg = package_ar5_alf_point_data(point_data_list[3], varname)
    for era, summaries in ar5_point_pkg.items():
        point_pkg[era] = summaries

    # if request.args.get("format") == "csv":
    #     csv_data = create_csv(point_pkg)
    #     return return_csv(csv_data)

    return postprocess(point_pkg, "alfresco")


# @routes.route("/<var_ep>/huc/<huc_id>")
# def huc_data_endpoint(var_ep, huc_id):
#     """HUC-aggregation data endpoint. Fetch data within HUC
#     for specified variable and return JSON-like dict.

#     Args:
#         var_ep (str): variable endpoint. Either taspr, temperature,
#             or precipitation
#         huc_id (int): 8-digit HUC ID
#     Returns:
#         huc_pkg (dict): zonal mean of variable(s) for HUC polygon

#     """
#     validation = validate_huc8(huc_id)
#     if validation == 400:
#         return render_template("400/bad_request.html"), 400
#     try:
#         if var_ep in var_ep_lu.keys():
#             huc_pkg = run_aggregate_var_polygon(var_ep, huc8_gdf, huc_id)
#         elif var_ep == "taspr":
#             huc_pkg = run_aggregate_allvar_polygon(huc8_gdf, huc_id)
#     except:
#         return render_template("422/invalid_huc.html"), 422

#     if request.args.get("format") == "csv":
#         csv_data = create_csv(huc_pkg)
#         return return_csv(csv_data)

#     return postprocess(huc_pkg, "taspr")


# @routes.route("/<var_ep>/protectedarea/<akpa_id>")
# def taspr_protectedarea_data_endpoint(var_ep, akpa_id):
#     """Protected Area-aggregation data endpoint. Fetch data within Protected Area for specified variable and return JSON-like dict.
#     Args:
#         var_ep (str): variable endpoint. Either taspr, temperature,
#             or precipitation
#         akpa_id (str): Protected Area ID (e.g. "NPS7")
#     Returns:
#         pa_pkg (dict): zonal mean of variable(s) for protected area polygon
#     """
#     validation = validate_akpa(akpa_id)
#     if validation == 400:
#         return render_template("400/bad_request.html"), 400
#     try:
#         if var_ep in var_ep_lu.keys():
#             pa_pkg = run_aggregate_var_polygon(var_ep, akpa_gdf, akpa_id)
#         elif var_ep == "taspr":
#             pa_pkg = run_aggregate_allvar_polygon(akpa_gdf, akpa_id)
#     except:
#         return render_template("422/invalid_protected_area.html"), 422

#     if request.args.get("format") == "csv":
#         csv_data = create_csv(pa_pkg)
#         return return_csv(csv_data)

#     return pa_pkg
