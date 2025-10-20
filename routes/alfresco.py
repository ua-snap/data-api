import asyncio
import itertools
import logging
import time

import geopandas as gpd
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from generate_requests import generate_netcdf_wcs_getcov_str
from generate_urls import generate_wcs_query_url, generate_wfs_huc12_intersection_url
from fetch_data import (
    fetch_bbox_netcdf_list,
    fetch_data,
    generate_nested_dict,
    get_poly,
    describe_via_wcps,
    get_all_possible_dimension_combinations,
)
from zonal_stats import vectorized_zonal_means_nd
from validate_request import get_coverage_encodings, get_coverage_crs_str
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    validate_var_id,
)
from postprocessing import nullify_and_prune, postprocess
from config import WEST_BBOX, EAST_BBOX
from . import routes

logger = logging.getLogger(__name__)

alfresco_api = Blueprint("alfresco_api", __name__)

var_ep_lu = {
    "flammability": {
        "cov_id_str": "alfresco_relative_flammability_30yr",
        "dim_encodings": None,  # populated below
        "bandnames": ["Gray"],
        "label": "Flammability",
        "crs": None,
    },
    "veg_type": {
        "cov_id_str": "alfresco_vegetation_type_percentage",
        "dim_encodings": None,  # populated below
        "bandnames": ["Gray"],
        "label": "Vegetation Type",
        "crs": None,
    },
}


async def get_alfresco_metadata(var_ep_lu):
    """Get the coverage metadata and encodings for ALFRESCO coverages and populate the lookup."""
    flam_metadata = await describe_via_wcps(var_ep_lu["flammability"]["cov_id_str"])
    veg_metadata = await describe_via_wcps(var_ep_lu["veg_type"]["cov_id_str"])

    var_ep_lu["flammability"]["dim_encodings"] = get_coverage_encodings(flam_metadata)
    var_ep_lu["veg_type"]["dim_encodings"] = get_coverage_encodings(veg_metadata)

    var_ep_lu["flammability"]["crs"] = get_coverage_crs_str(flam_metadata)
    var_ep_lu["veg_type"]["crs"] = get_coverage_crs_str(veg_metadata)

    return var_ep_lu


# Populate the encodings
var_ep_lu = asyncio.run(get_alfresco_metadata(var_ep_lu))


async def fetch_alf_bbox_data(bbox_bounds, cov_id_str):
    """Make the async request for the data at the specified point for a specific coverage

    Args:
        bbox_bounds (tuple): 4-tuple of x,y lower/upper bounds: (<xmin>,<ymin>,<xmax>,<ymax>)
        cov_id_str (str): shared portion of coverage_ids to query
    Returns:
        bbox_ds (xarray.DataSet): xarray dataset with the data for the bbox
    """
    # set up WCS request strings
    request_strs = []
    request_strs.append(generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id_str))
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    bbox_ds_list = await fetch_bbox_netcdf_list(urls)
    bbox_ds = bbox_ds_list[
        0
    ]  # there is only ever one dataset in the list for this endpoint
    return bbox_ds


def run_aggregate_var_polygon(var_ep, poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon. Fetches data on
    the individual instances of the singular dimension combinations.

    Args:
        var_ep (str): variable endpoint (one of "flammability" or "veg_type")
        poly_id (str or int): the unique `id` used to identify the Polygon
            for which to compute the zonal mean.
    Returns:
        aggr_results (dict): data representing zonal stats within the polygon.
    """
    logger.debug(f"Running aggregate var polygon for {var_ep} with polygon ID {poly_id}")
    polygon = get_poly(poly_id)
    cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
    bandname = var_ep_lu[var_ep]["bandnames"][0]
    crs = var_ep_lu[var_ep]["crs"]
    ds = asyncio.run(fetch_alf_bbox_data(polygon.total_bounds, cov_id_str))

    start_time = time.time()
    # get all combinations of non-XY dimensions in the dataset and their corresponding encodings
    # and create a dict to hold the results for each combo
    all_dims = ds[bandname].dims
    dimnames = [dim for dim in all_dims if dim not in ["X", "Y"]]
    dim_encodings = var_ep_lu[var_ep]["dim_encodings"]
    iter_coords = list(itertools.product(*[list(ds[dim].values) for dim in dimnames]))
    dim_combos = get_all_possible_dimension_combinations(
        iter_coords, dimnames, dim_encodings
    )
    aggr_results = generate_nested_dict(dim_combos)

    # Vectorized zonal means across all non-XY dims
    means_da = vectorized_zonal_means_nd(polygon, ds, crs, var_name=bandname)
    # If an error template/tuple is returned (CRS issues), propagate as-is
    if isinstance(means_da, tuple):
        return means_da

    # Populate aggregated results using the vectorized means
    for coords, dim_combo in zip(iter_coords, dim_combos):
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        mean_val = means_da.sel(sel_di).item()

        if var_ep == "flammability":
            result = round(float(mean_val), 4)
            # use the dim_combo to index into the results dict (era, model, scenario)
            aggr_results[dim_combo[0]][dim_combo[1]][dim_combo[2]] = result
        elif var_ep == "veg_type":
            result = round(float(mean_val) * 100, 2)
            # use the dim_combo to index into the results dict (era, model, scenario, veg_type)
            aggr_results[dim_combo[0]][dim_combo[1]][dim_combo[2]][
                dim_combo[3]
            ] = result

    aggr_results = remove_invalid_dim_combos(var_ep, aggr_results)
    end_time = time.time()
    logger.info(f"Completed in : {end_time - start_time} seconds")
    return aggr_results


def remove_invalid_dim_combos(var_ep, results):
    """Remove data from invalid era/model/scenario dimension combinations

    Args:
        var_ep (str): "flammability" or "veg_type"
        results (dict): point or area results data

    Returns:
        results (dict): point or area results data with invalid combos removed
    """
    start_time = time.time()
    logger.debug(f"Removing invalid dimension combinations for {var_ep}")
    dim_encodings = var_ep_lu[var_ep]["dim_encodings"]

    eras = list(dim_encodings["era"].values())
    models = list(dim_encodings["model"].values())
    scenarios = list(dim_encodings["scenario"].values())

    # Remove empty data from invalid combos of era/model/scenario.
    for era in eras:
        for model in models:
            # Remove non-CRU-TS models and non-historical era from historical data.
            if era in ["1950-2008", "1950-1979", "1980-2008"]:
                if model not in ["CRU-TS", "MODEL-SPINUP"]:
                    results[era].pop(model, None)
                    continue
                for scenario in scenarios:
                    if scenario != "historical":
                        results[era][model].pop(scenario, None)
            # Remove historical era from projected data.
            else:
                results[era][model].pop("historical", None)
        # Remove CRU-TS "model" from projected data.
        if era not in ["1950-2008", "1950-1979", "1980-2008"]:
            results[era].pop("CRU-TS", None)
            results[era].pop("MODEL-SPINUP", None)

    end_time = time.time()
    logger.info(f"Completed removing invalid dimension combinations in : {end_time - start_time:.4f} seconds")
    return results


@routes.route("/alfresco/")
@routes.route("/alfresco/flammability/")
@routes.route("/alfresco/veg_type/")
@routes.route("/alfresco/flammability/point/")
@routes.route("/alfresco/veg_type/point/")
@routes.route("/alfresco/flammability/area/")
@routes.route("/alfresco/veg_type/area/")
@routes.route("/alfresco/flammability/local/")
@routes.route("/alfresco/veg_type/local/")
def alfresco_about():
    return render_template("documentation/alfresco.html")


@routes.route("/alfresco/<var_ep>/local/<lat>/<lon>")
def run_fetch_alf_local_data(var_ep, lat, lon):
    """ "Local" endpoint for ALFRESCO data - finds the HUC-12 that intersects
    the request lat/lon and returns a summary of data within that HUC

    Args:
        var_ep (str): variable endpoint. Flammability or veg_type
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested ALFRESCO data
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

    # Requests for HUC12s that intersect
    huc12_features = asyncio.run(
        fetch_data([generate_wfs_huc12_intersection_url(lat, lon)])
    )["features"]

    if len(huc12_features) < 1:
        return render_template("404/no_data.html"), 404

    huc12_gdf = gpd.GeoDataFrame.from_features(huc12_features)

    # Collect the HUC12 ID for the returned nearest HUC12
    huc_id = huc12_gdf.loc[0, "id"]
    huc12_pkg = run_fetch_alf_area_data(var_ep, huc_id, ignore_csv=True)

    # this is only ever true when it is returning an error template
    if isinstance(huc12_pkg, tuple):
        return huc12_pkg

    if request.args.get("format") == "csv":
        huc12_pkg = nullify_and_prune(huc12_pkg, var_ep)
        if huc12_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        # return CSV with lat/lon info since HUC12 names not handled
        return create_csv(huc12_pkg, var_ep, huc_id, lat, lon)

    huc12_pkg["huc_id"] = huc_id
    huc12_pkg["boundary_url"] = f"https://earthmaps.io/boundary/area/{huc_id}"

    return huc12_pkg


@routes.route("/alfresco/<var_ep>/area/<var_id>")
def run_fetch_alf_area_data(var_ep, var_id, ignore_csv=False):
    """ALFRESCO aggregation data endpoint. Fetch data within AOI polygon for specified
    variable and return JSON-like dict.

    Args:
        var_ep (str): variable endpoint. Flammability, veg change, or veg type
        var_id (str): ID for any AOI polygon
        ignore_csv (bool): if set, ignore the CSV argument as it comes from another
            route and this function is being called from there.
    Returns:
        poly_pkg (dict): zonal mean of variable(s) for AOI polygon
    """
    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    #try:
    poly_pkg = run_aggregate_var_polygon(var_ep, var_id)
    # except:
    #     return render_template("422/invalid_area.html"), 422

    if (request.args.get("format") == "csv") and not ignore_csv:
        poly_pkg = nullify_and_prune(poly_pkg, var_ep)
        if poly_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        return create_csv(poly_pkg, var_ep, var_id)

    return postprocess(poly_pkg, var_ep)
