import asyncio
import itertools
import geopandas as gpd
import numpy as np
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
    # zonal_stats,
    generate_nested_dict,
    get_from_dict,
    get_poly,
    describe_via_wcps,
    interpolate_and_compute_zonal_stats,
)
from validate_request import get_coverage_encodings
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    validate_var_id,
)
from postprocessing import nullify_and_prune, postprocess
from config import WEST_BBOX, EAST_BBOX
from . import routes

alfresco_api = Blueprint("alfresco_api", __name__)

var_ep_lu = {
    "flammability": {
        "cov_id_str": "alfresco_relative_flammability_30yr",
        "bandname": "Gray",
        "label": "Flammability",
    },
    "veg_type": {
        "cov_id_str": "alfresco_vegetation_type_percentage",
        "bandname": "Gray",
        "label": "Vegetation Type",
    },
}


async def get_alfresco_metadata(var_ep_lu):
    """Get the coverage metadata and encodings for ALFRESCO coverages and populate the lookup."""
    flam_metadata = await describe_via_wcps(var_ep_lu["flammability"]["cov_id_str"])
    veg_metadata = await describe_via_wcps(var_ep_lu["veg_type"]["cov_id_str"])

    var_ep_lu["flammability"]["dim_encodings"] = get_coverage_encodings(flam_metadata)
    var_ep_lu["veg_type"]["dim_encodings"] = get_coverage_encodings(veg_metadata)

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

    # def get_poly_mask_arr(ds, poly, bandname):
    #     """Get the polygon mask array from an xarray dataset, intended to be recycled for rapid
    #     zonal summary across results from multiple WCS requests for the same bbox. Wrapper for
    #     rasterstats zonal_stats().

    #     Args:
    #         ds (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
    #         poly (shapely.Polygon): polygon to create mask from
    #         bandname (str): name of the DataArray containing the data

    #     Returns:
    #         cropped_poly_mask (numpy.ma.core.MaskedArra): a masked array masking the cells
    #             intersecting the polygon of interest, cropped to the right shape
    #     """
    #     # need a data layer of same x/y shape just for running a zonal stats
    #     xy_shape = ds[bandname].values.shape[-2:]
    #     data_arr = np.zeros(xy_shape)
    #     # get affine transform from the xarray.DataSet
    #     ds.rio.set_spatial_dims("X", "Y")
    #     transform = ds.rio.transform()
    #     poly_mask_arr = zonal_stats(
    #         poly,
    #         data_arr,
    #         affine=transform,
    #         nodata=np.nan,
    #         stats=["mean"],
    #         raster_out=True,
    #     )[0]["mini_raster_array"]
    #     cropped_poly_mask = poly_mask_arr[0 : xy_shape[1], 0 : xy_shape[0]]
    #     return cropped_poly_mask

    # def summarize_within_poly_marr(
    #     ds, poly_mask_arr, dim_encodings, bandname="Gray", var_ep="Gray"
    # ):
    #     """Summarize a single Data Variable of a xarray.DataSet within a polygon.
    #     Return the results as a nested dict.

    #     NOTE - This is a candidate for de-duplication! Only defining here because some
    #     things are out-of-sync with existing ways of doing things (e.g., key names
    #     in dim_encodings dicts in other endpoints are not equal to axis names in coverages)

    #     Args:
    #         ds (xarray.DataSet): DataSet with "Gray" as variable of
    #             interest
    #         poly_mask_arr (numpy.ma.core.MaskedArra): a masked array masking the cells intersecting
    #             the polygon of interest
    #         dim_encodings (dict): nested dictionary of thematic key value pairs that chacterize the
    #             data and map integer data coordinates to models, scenarios, variables, etc.
    #         bandname (str): name of variable in ds, defaults to "Gray" for rasdaman coverages where
    #             the name is not given at ingest
    #         var_ep (str): variable (flammability or veg_type)

    #     Returns:
    #         Nested dict of results for all non-X/Y axis combinations,
    #     """
    #     # will actually operate on underlying DataArray

    #     da = ds[bandname]
    #     # get axis (dimension) names and make list of all coordinate combinations
    #     all_dims = da.dims
    #     dimnames = [dimname for dimname in all_dims if dimname not in ("X", "Y")]
    #     iter_coords = list(
    #         itertools.product(*[list(ds[dimname].values) for dimname in dimnames])
    #     )

    #     # generate all combinations of decoded coordinate values
    #     dim_combos = []
    #     for coords in iter_coords:
    #         map_list = [
    #             dim_encodings[dimname][coord] for coord, dimname in zip(coords, dimnames)
    #         ]
    #         dim_combos.append(map_list)
    #     aggr_results = generate_nested_dict(dim_combos)

    #     data_arr = []
    #     for coords in iter_coords:
    #         sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
    #         data_arr.append(da.sel(sel_di).values)
    #     data_arr = np.array(data_arr)

    #     # need to transpose the 2D spatial slices if X is the "rows" dimension
    #     if all_dims.index("X") < all_dims.index("Y"):
    #         data_arr = data_arr.transpose(0, 2, 1)

    #     data_arr_mask = np.broadcast_to(poly_mask_arr.mask, data_arr.shape)
    #     data_arr[data_arr_mask] = np.nan
    #     results = np.nanmean(data_arr, axis=(1, 2)).astype(float)

    #     for map_list, result in zip(dim_combos, results):
    #         if len(map_list) > 1:
    #             data = get_from_dict(aggr_results, map_list[:-1])
    #             if var_ep == "flammability":
    #                 result = round(result, 4)
    #             elif var_ep == "veg_type":
    #                 result = round(result * 100, 2)
    #             data[map_list[-1]] = result
    #         else:
    #             aggr_results[map_list[0]] = round(result, 4)
    #     return aggr_results


def run_aggregate_var_polygon(var_ep, poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon. Fetches data on
    the individual instances of the singular dimension combinations.

    Args:
        var_ep (str): variable endpoint (one of "flammability" or "veg_type")
            poly_gdf (GeoDataFrame): the object from which to fetch the polygon,
            e.g. the HUC 8 geodataframe for watershed polygons
        poly_id (str or int): the unique `id` used to identify the Polygon
            for which to compute the zonal mean.
    Returns:
        aggr_results (dict): data representing zonal means within the polygon.
    """
    polygon = get_poly(poly_id)
    cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
    bandname = var_ep_lu[var_ep]["bandname"]
    ds = asyncio.run(fetch_alf_bbox_data(polygon.total_bounds, cov_id_str))

    # get all combinations of non-XY dimensions in the dataset and their corresponding encodings
    # and create a dict to hold the results for each combo
    all_dims = ds[bandname].dims
    dimnames = [dim for dim in all_dims if dim not in ["X", "Y"]]
    dim_encodings = var_ep_lu[var_ep]["dim_encodings"]
    iter_coords = list(itertools.product(*[list(ds[dim].values) for dim in dimnames]))
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            dim_encodings[dimname][coord] for coord, dimname in zip(coords, dimnames)
        ]
        dim_combos.append(map_list)
    aggr_results = generate_nested_dict(dim_combos)

    # select the dim combo from the dataset and calculate zonal stats, adding to the results dict
    for coords, dim_combo in zip(iter_coords, dim_combos):
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        combo_ds = ds.sel(sel_di)
        combo_zonal_stats_dict = interpolate_and_compute_zonal_stats(polygon, combo_ds)
        result = combo_zonal_stats_dict["mean"]
        if var_ep == "flammability":
            result = round(result, 4)
        elif var_ep == "veg_type":
            result = round(result * 100, 2)
        # use the dim_combo to index into the results dict (era, model, scenario)
        aggr_results[dim_combo[0]][dim_combo[1]][dim_combo[2]] = result

    print(aggr_results)

    # return zonal_stats_dict

    # get the polygon mask array for rapidly aggregating within the polygon
    #  for all data layers (avoids computing spatial transform for each layer)
    # bandname = "Gray"
    # poly_mask_arr = get_poly_mask_arr(ds_list[0], poly, bandname)

    # average over the following decades / time periods
    # aggr_results = {}
    # if var_ep == "flammability":
    #     ar5_results = summarize_within_poly_marr(
    #         ds_list[-1], poly_mask_arr, flammability_dim_encodings, bandname, var_ep
    #     )
    # elif var_ep == "veg_type":
    #     ar5_results = summarize_within_poly_marr(
    #         ds_list[-1], poly_mask_arr, veg_type_dim_encodings, bandname, var_ep
    #     )
    # for era, summaries in ar5_results.items():
    #     aggr_results[era] = summaries
    # aggr_results = remove_invalid_dim_combos(var_ep, aggr_results)

    return aggr_results


def remove_invalid_dim_combos(var_ep, results):
    """Remove data from invalid era/model/scenario dimension combinations

    Args:
        results (dict): point or area results data

    Returns:
        results (dict): point or area results data with invalid combos removed
    """
    if var_ep == "flammability":
        dim_encodings = flammability_dim_encodings
    elif var_ep == "veg_type":
        dim_encodings = veg_type_dim_encodings

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

    return results


@routes.route("/alfresco/")
@routes.route("/alfresco/abstract/")
@routes.route("/alfresco/flammability/")
@routes.route("/alfresco/veg_type/")
@routes.route("/alfresco/flammability/point/")
@routes.route("/alfresco/veg_type/point/")
@routes.route("/alfresco/flammability/area/")
@routes.route("/alfresco/veg_type/area/")
@routes.route("/alfresco/area/")
@routes.route("/alfresco/flammability/local/")
@routes.route("/alfresco/veg_type/local/")
@routes.route("/alfresco/local/")
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

    # TODO: use the new function!
    return run_aggregate_var_polygon(var_ep, var_id)

    try:
        poly_pkg = run_aggregate_var_polygon(var_ep, var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    if (request.args.get("format") == "csv") and not ignore_csv:
        poly_pkg = nullify_and_prune(poly_pkg, var_ep)
        if poly_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        return create_csv(poly_pkg, var_ep, var_id)

    return postprocess(poly_pkg, var_ep)
