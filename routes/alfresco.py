import asyncio
import io
import csv
import time
import itertools
import numpy as np
import xarray as xr
from flask import (
    Blueprint,
    render_template,
    request,
    current_app as app,
)

# local imports
from generate_requests import *
from generate_urls import generate_wcs_query_url
from fetch_data import *
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

# CSV field names
alf_fieldnames = ["variable", "date_range", "model", "scenario", "stat", "value"]
# package coordinate names that corresponding to each level of the nested data package dicts
package_coords = ["date_range", "model", "scenario", "variable"]


async def fetch_alf_point_data(x, y, cov_id_str):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_id_str (str): shared portion of coverage_ids to query

    Returns:
        list of data results from each of historical and future coverages
    """
    # set up WCS request strings
    request_strs = []
    # historical data request string
    request_strs.append(generate_wcs_getcov_str(x, y, f"{cov_id_str}_historical"))
    # generate both future average requests (averages over decades)
    for coords in [(3, 5), (6, 8)]:
        request_strs.append(
            generate_average_wcps_str(x, y, f"{cov_id_str}_future", "era", coords)
        )
    # future non-average request str
    request_strs.append(generate_wcs_getcov_str(x, y, f"{cov_id_str}_future"))
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    point_data_list = await fetch_data(urls)
    return point_data_list


async def fetch_alf_bbox_data(bbox_bounds, cov_id_str):
    """Make the async request for the data at the specified point for
    a specific varname.

    Args:
        bbox_bounds (tuple): 4-tuple of x,y lower/upper bounds: (<xmin>,<ymin>,<xmax>,<ymax>)
        cov_id_str (str): shared portion of coverage_ids to query

    Returns:
        list of data results from each of historical and future coverages
    """
    # set up WCS request strings
    request_strs = []
    # historical data request string
    request_strs.append(
        generate_netcdf_wcs_getcov_str(bbox_bounds, f"{cov_id_str}_historical")
    )
    # generate both future average requests (averages over decades)
    for coords in [(3, 5), (6, 8)]:
        # kwargs to pass to function in generate_netcdf_average_wcps_str
        kwargs = {
            "cov_id": f"{cov_id_str}_future",
            "axis_name": "era",
            "axis_coords": coords,
            "encoding": "netcdf",
        }
        request_strs.append(generate_netcdf_average_wcps_str(bbox_bounds, kwargs))
    # future non-average request str
    request_strs.append(
        generate_netcdf_wcs_getcov_str(bbox_bounds, f"{cov_id_str}_future")
    )
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    bbox_ds_list = await fetch_bbox_netcdf_list(urls)
    return bbox_ds_list


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


def get_poly_mask_arr(ds, poly, bandname):
    """Get the polygon mask array from an xarray dataset, intended to be recycled for rapid 
    zonal summary across results from multiple WCS requests for the same bbox. Wrapper for
    rasterstats zonal_stats().

    Args:
        ds (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        poly (shapely.Polygon): polygon to create mask from
        bandname (str): name of the DataArray containing the data
    
    Returns:
        poly_mask_arr (numpy.ma.core.MaskedArra): a masked array masking the cells intersecting 
            the polygon of interest
    """
    # need a data layer of same x/y shape just for running a zonal stats
    xy_shape = ds[bandname].values.shape[-2:]
    data_arr = np.zeros(xy_shape)
    # get affine transform from the xarray.DataSet
    ds.rio.set_spatial_dims("X", "Y")
    transform = ds.rio.transform()
    poly_mask_arr = zonal_stats(
        poly,
        data_arr,
        affine=transform,
        nodata=np.nan,
        stats=["mean"],
        raster_out=True,
    )[0]["mini_raster_array"]
    return poly_mask_arr


def summarize_within_poly_marr(
    ds, poly_mask_arr, dim_encodings, bandname="Gray", varname="Gray"
):
    """Summarize a single Data Variable of a xarray.DataSet within a polygon.
    Return the results as a nested dict.

    NOTE - This is a candidate for de-duplication! Only defining here because some
    things are out-of-sync with existing ways of doing things (e.g., key names 
    in dim_encodings dicts in other endpoints are not equal to axis names in coverages)

    Args:
        ds (xarray.DataSet): DataSet with "Gray" as variable of
            interest
        poly_mask_arr (numpy.ma.core.MaskedArra): a masked array masking the cells intersecting 
            the polygon of interest
        dim_encodings (dict): nested dictionary of thematic key value pairs that chacterize the 
            data and map integer data coordinates to models, scenarios, variables, etc.
        bandname (str): name of variable in ds, defaults to "Gray" for rasdaman coverages where
            the name is not given at ingest
        varname (str): standard variable name used for storing results

    Returns:
        Nested dict of results for all non-X/Y axis combinations,
    """
    # will actually operate on underlying DataArray

    da = ds[bandname]
    # get axis (dimension) names and make list of all coordinate combinations
    all_dims = da.dims
    dimnames = [dimname for dimname in all_dims if dimname not in ("X", "Y")]
    iter_coords = list(
        itertools.product(*[list(ds[dimname].values) for dimname in dimnames])
    )

    # generate all combinations of decoded coordinate values
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            dim_encodings[dimname][coord] for coord, dimname in zip(coords, dimnames)
        ]
        dim_combos.append(map_list)
    aggr_results = generate_nested_dict(dim_combos)

    data_arr = []
    for coords in iter_coords:
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        data_arr.append(da.sel(sel_di).values)
    data_arr = np.array(data_arr)

    # need to transpose the 2D spatial slices if X is the "rows" dimension
    if all_dims.index("X") < all_dims.index("Y"):
        data_arr = data_arr.transpose(0, 2, 1)

    data_arr_mask = np.broadcast_to(poly_mask_arr.mask, data_arr.shape)
    data_arr[data_arr_mask] = np.nan
    results = np.nanmean(data_arr, axis=(1, 2)).astype(float)

    for map_list, result in zip(dim_combos, results):
        if len(map_list) > 1:
            get_from_dict(aggr_results, map_list[:-1])[map_list[-1]] = {
                varname: round(result, 4)
            }
        else:
            aggr_results[map_list[0]] = round(result, 4)
    return aggr_results


def run_aggregate_var_polygon(var_ep, poly_gdf, poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon.

    Args:
        var_ep (str): variable endpoint. Either flammability or veg_change
            poly_gdf (GeoDataFrame): the object from which to fetch the polygon,
            e.g. the HUC 8 geodataframe for watershed polygons
        poly_id (str or int): the unique `id` used to identify the Polygon
            for which to compute the zonal mean.

    Returns:
        aggr_results (dict): data representing zonal means within the polygon.

    Notes:
        Fetches data on the individual instances of the singular dimension combinations. Consider 
            validating polygon IDs in `validate_data` or `lat_lon` module.
    """
    poly = get_poly_3338_bbox(poly_gdf, poly_id)
    # mapping between coordinate values (ints) and variable names (strs)
    varname = var_ep_lu[var_ep]["varname"]
    cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
    ds_list = asyncio.run(fetch_alf_bbox_data(poly.bounds, cov_id_str))

    # get the polygon mask array for rapidly aggregating within the polygon
    #  for all data layers (avoids computing spatial transform for each layer)
    bandname = "Gray"
    poly_mask_arr = get_poly_mask_arr(ds_list[0], poly, bandname)
    # average over the following decades / time periods
    aggr_results = {}
    historical_results = summarize_within_poly_marr(
        ds_list[0], poly_mask_arr, historical_dim_encodings, bandname, varname
    )
    #  add the model, scenario, and varname levels for CRU
    for era in historical_results:
        aggr_results[era] = {
            "CRU-TS40": {"CRU_historical": {varname: historical_results[era]}}
        }
    # run regular future
    ar5_results = summarize_within_poly_marr(
        ds_list[-1], poly_mask_arr, future_dim_encodings, bandname, varname
    )
    for era, summaries in ar5_results.items():
        aggr_results[era] = summaries
    # run summary eras for future
    summary_eras = ["2040_2069", "2070_2099"]
    for ds, era in zip(ds_list[1:3], summary_eras):
        aggr_results[era] = summarize_within_poly_marr(
            ds, poly_mask_arr, future_dim_encodings, bandname, varname
        )

    return aggr_results


@routes.route("/alfresco/")
@routes.route("/alfresco/abstract/")
@routes.route("/alfresco/flammability/")
@routes.route("/alfresco/veg_change/")
def alfresco_about():
    return render_template("alfresco/abstract.html")


@routes.route("/alfresco/flammability/point/")
@routes.route("/alfresco/veg_change/point/")
@routes.route("/alfresco/point/")
def alfresco_about_point():
    return render_template("alfresco/point.html")


@routes.route("/alfresco/flammability/huc/")
@routes.route("/alfresco/veg_change/huc/")
@routes.route("/alfresco/huc/")
def alfresco_about_huc():
    return render_template("alfresco/huc.html")


@routes.route("/alfresco/flammability/protectedarea/")
@routes.route("/alfresco/veg_change/protectedarea/")
@routes.route("/alfresco/protectedarea/")
def alfresco_about_protectedarea():
    return render_template("alfresco/protectedarea.html")


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
        cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
        try:
            point_data_list = asyncio.run(fetch_alf_point_data(x, y, cov_id_str))
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500
    else:
        return render_template("400/bad_request.html"), 400

    varname = var_ep_lu[var_ep]["varname"]
    point_pkg = package_historical_alf_point_data(point_data_list[0], varname)
    # package AR5 data and fold into data pakage
    ar5_point_pkg = package_ar5_alf_point_data(point_data_list[3], varname)
    for era, summaries in ar5_point_pkg.items():
        point_pkg[era] = summaries
    # package summary future eras in
    point_pkg["2040-2069"] = package_ar5_alf_averaged_point_data(
        point_data_list[1], varname
    )
    point_pkg["2070-2099"] = package_ar5_alf_averaged_point_data(
        point_data_list[2], varname
    )

    if request.args.get("format") == "csv":
        csv_data = create_csv(
            point_pkg,
            alf_fieldnames,
            package_coords,
            {"variable": varname, "stat": "mean"},
        )
        return return_csv(csv_data)

    return postprocess(point_pkg, "alfresco")


@routes.route("/alfresco/<var_ep>/huc/<huc_id>")
def run_fetch_alf_huc_data(var_ep, huc_id):
    """HUC-aggregation data endpoint. Fetch data within HUC
    for specified variable and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either veg_change or flammability
        huc_id (int): 8-digit HUC ID
    Returns:
        huc_pkg (dict): zonal mean of variable(s) for HUC polygon

    """
    validation = validate_huc8(huc_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        huc_pkg = run_aggregate_var_polygon(var_ep, huc8_gdf, huc_id)
    except:
        return render_template("422/invalid_huc.html"), 422

    if request.args.get("format") == "csv":
        varname = var_ep_lu[var_ep]["varname"]
        csv_data = create_csv(
            huc_pkg,
            alf_fieldnames,
            package_coords,
            {"variable": varname, "stat": "mean"},
        )
        return return_csv(csv_data)

    return postprocess(huc_pkg, "alfresco")


@routes.route("/alfresco/<var_ep>/protectedarea/<akpa_id>")
def run_fetch_alf_protectedarea_data(var_ep, akpa_id):
    """Protected Area-aggregation data endpoint. Fetch data within Protected Area for specified
    variable and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Either taspr, temperature,
            or precipitation
        akpa_id (str): Protected Area ID (e.g. "NPS7")
    Returns:
        pa_pkg (dict): zonal mean of variable(s) for protected area polygon
    """
    validation = validate_akpa(akpa_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    # pa_pkg = run_aggregate_var_polygon(var_ep, akpa_gdf, akpa_id)
    try:
        pa_pkg = run_aggregate_var_polygon(var_ep, akpa_gdf, akpa_id)
    except:
        return render_template("422/invalid_protected_area.html"), 422

    if request.args.get("format") == "csv":
        varname = var_ep_lu[var_ep]["varname"]
        csv_data = create_csv(
            pa_pkg,
            alf_fieldnames,
            package_coords,
            {"variable": varname, "stat": "mean"},
        )
        return return_csv(csv_data)

    return postprocess(pa_pkg, "alfresco")