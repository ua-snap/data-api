import asyncio
import numpy as np
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_netcdf_wcs_getcov_str
from fetch_data import (
    fetch_bbox_netcdf_list,
    fetch_wcs_point_data,
    describe_via_wcps,
    generate_nested_dict,
    # zonal_stats,
    itertools,
    get_poly,
)
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
    get_coverage_encodings,
)
from postprocessing import nullify_and_prune, postprocess
from . import routes
from config import WEST_BBOX, EAST_BBOX

beetles_api = Blueprint("beetles_api", __name__)

var_ep_lu = {
    "beetles": {
        "cov_id_str": "beetle_risk",
        "dim_encodings": None,  # populated below
        "bandname": "Gray",
        "label": None,  # TODO: remove if not used
    },
}


async def get_beetles_metadata(var_ep_lu):
    """Get the coverage metadata and encodings for ALFRESCO coverages and populate the lookup."""
    beetles_metadata = await describe_via_wcps(var_ep_lu["beetles"]["cov_id_str"])
    var_ep_lu["beetles"]["dim_encodings"] = get_coverage_encodings(beetles_metadata)

    return var_ep_lu


# Populate the encodings
var_ep_lu = asyncio.run(get_beetles_metadata(var_ep_lu))


# dim_encodings = {
#     "model": {
#         0: "NCAR-CCSM4",
#         1: "GFDL-ESM2M",
#         2: "HadGEM2-ES",
#         3: "MRI-CGCM3",
#     },
#     "scenario": {
#         0: "rcp45",
#         1: "rcp85",
#     },
#     "era": {
#         0: "2010-2039",
#         1: "2040-2069",
#         2: "2070-2099",
#     },
#     "snowpack": {0: "low", 1: "medium"},
#     # The 0 in climate_protection represents NO DATA in this context,
#     # but needs to remain as 0 to allow for the pruning function
#     # to correctly identify it as unrepresented space in the model.
#     "climate_protection": {0: 0, 1: "high", 2: "minimal", 3: "none"},
# }


async def fetch_beetles_bbox_data(bbox_bounds, cov_id_str):
    """Make the async request for the data at the specified point for
    a specific coverage

    Args:
        bbox_bounds (tuple): 4-tuple of x,y lower/upper bounds: (<xmin>,<ymin>,<xmax>,<ymax>)
        cov_id_str (str): shared portion of coverage_ids to query

    Returns:
        list of data results from each of historical and future coverages
    """
    # set up WCS request strings
    request_strs = []
    request_strs.append(generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id_str))
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    bbox_ds_list = await fetch_bbox_netcdf_list(urls)
    bbox_ds = bbox_ds_list[
        0
    ]  # # there is only ever one dataset in the list for this endpoint
    return bbox_ds


def package_beetle_data(beetle_resp, beetle_percents=None):
    """Package the beetle risk data into a nested JSON-like dict.

    Arguments:
        beetle_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all beetle risk values
    """
    dim_encodings = var_ep_lu["dim_encodings"]

    # initialize the output dict
    di = dict()

    # Gather historical risk levels
    di["1988-2017"] = dict()
    di["1988-2017"]["Daymet"] = dict()
    di["1988-2017"]["Daymet"]["Historical"] = dict()
    for sni in range(len(beetle_resp[0][0][0])):
        snowpack = dim_encodings["snowpack"][sni]
        di["1988-2017"]["Daymet"]["Historical"][snowpack] = dict()
        if beetle_resp[0][0][0][sni] is not None:
            di["1988-2017"]["Daymet"]["Historical"][snowpack]["climate-protection"] = (
                dim_encodings["climate_protection"][int(beetle_resp[0][0][0][sni])]
            )
        else:
            di["1988-2017"]["Daymet"]["Historical"][snowpack]["climate-protection"] = 0
        if beetle_percents is not None:
            # This conditional will check to see if all percentages are 0% meaning that there is no data.
            # We must set the returned data dictionary values explicitly to 0 to ensure the pruning function
            # sets this place as No data available
            if (
                beetle_percents[0][0][0][sni][1] == 0.0
                and beetle_percents[0][0][0][sni][2] == 0.0
                and beetle_percents[0][0][0][sni][3] == 0.0
            ):
                di["1988-2017"]["Daymet"]["Historical"][snowpack][
                    "percent-high-protection"
                ] = 0
                di["1988-2017"]["Daymet"]["Historical"][snowpack][
                    "percent-minimal-protection"
                ] = 0
                di["1988-2017"]["Daymet"]["Historical"][snowpack][
                    "percent-no-protection"
                ] = 0
            else:
                di["1988-2017"]["Daymet"]["Historical"][snowpack][
                    "percent-high-protection"
                ] = beetle_percents[0][0][0][sni][1]
                di["1988-2017"]["Daymet"]["Historical"][snowpack][
                    "percent-minimal-protection"
                ] = beetle_percents[0][0][0][sni][2]
                di["1988-2017"]["Daymet"]["Historical"][snowpack][
                    "percent-no-protection"
                ] = beetle_percents[0][0][0][sni][3]

    # Gather predicted risk levels for future eras
    for ei, mod_li in enumerate(beetle_resp[1:]):
        era = dim_encodings["era"][ei]
        di[era] = dict()
        for mi, sc_li in enumerate(mod_li[1:]):
            model = dim_encodings["model"][mi]
            di[era][model] = dict()
            for si, sn_li in enumerate(sc_li[1:]):
                scenario = dim_encodings["scenario"][si]
                di[era][model][scenario] = dict()
                for sni, risk_level in enumerate(sn_li):
                    snowpack = dim_encodings["snowpack"][sni]
                    di[era][model][scenario][snowpack] = dict()
                    if risk_level is not None:
                        di[era][model][scenario][snowpack]["climate-protection"] = (
                            dim_encodings["climate_protection"][int(risk_level)]
                        )
                    else:
                        di[era][model][scenario][snowpack]["climate-protection"] = 0
                    if beetle_percents is not None:
                        # This conditional will check to see if all percentages are 0% meaning that there is no data.
                        # We must set the returned data dictionary values explicitly to 0 to ensure the pruning function
                        # sets this place as No data available
                        if (
                            beetle_percents[ei + 1][mi + 1][si + 1][sni][1] == 0.0
                            and beetle_percents[ei + 1][mi + 1][si + 1][sni][2] == 0.0
                            and beetle_percents[ei + 1][mi + 1][si + 1][sni][3] == 0.0
                        ):
                            di[era][model][scenario][snowpack][
                                "percent-high-protection"
                            ] = 0
                            di[era][model][scenario][snowpack][
                                "percent-minimal-protection"
                            ] = 0
                            di[era][model][scenario][snowpack][
                                "percent-no-protection"
                            ] = 0
                        else:
                            di[era][model][scenario][snowpack][
                                "percent-high-protection"
                            ] = beetle_percents[ei + 1][mi + 1][si + 1][sni][1]
                            di[era][model][scenario][snowpack][
                                "percent-minimal-protection"
                            ] = beetle_percents[ei + 1][mi + 1][si + 1][sni][2]
                            di[era][model][scenario][snowpack][
                                "percent-no-protection"
                            ] = beetle_percents[ei + 1][mi + 1][si + 1][sni][3]

    return di


# def summarize_within_poly_marr(ds, poly_mask_arr, bandname="Gray"):
#     """Summarize a single Data Variable of a xarray.DataSet within a polygon.
#     Return the results as a nested dict.

#     Args:
#         ds (xarray.DataSet): DataSet with "Gray" as variable of
#             interest
#         poly_mask_arr (numpy.ma.core.MaskedArray): a masked array masking the cells intersecting
#             the polygon of interest
#         dim_encodings (dict): nested dictionary of thematic key value pairs that characterize the
#             data and map integer data coordinates to models, scenarios, eras, snowpacks, etc.
#         bandname (str): name of variable in ds, defaults to "Gray" for Rasdaman coverages where
#             the name is not given at ingest

#     Returns:
#         Nested dict of results for all non-X/Y axis combinations,
#     """

#     # operates on underlying DataArray
#     da = ds[bandname]

#     # get axis (dimension) names and make list of all coordinate combinations
#     all_dims = da.dims
#     dimnames = [dimname for dimname in all_dims if dimname not in ("X", "Y")]
#     iter_coords = list(
#         itertools.product(*[list(ds[dimname].values) for dimname in dimnames])
#     )

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

#     # Adds one to each value to generate correct shape and
#     # iterates through the data below.
#     eras = sel_di["era"] + 1
#     models = sel_di["model"] + 1
#     scenarios = sel_di["scenario"] + 1
#     snowpacks = sel_di["snowpack"] + 1

#     return_arr = np.zeros((eras, models, scenarios, snowpacks))

#     # Percentages of each slice of the data. Has a last dimension that are
#     # integer values representing 1 = low risk, 2 = moderate risk, 3 = high risk
#     return_percentages = np.zeros((eras, models, scenarios, snowpacks, 4))
#     for era in range(eras):
#         for model in range(models):
#             for scenario in range(scenarios):
#                 for snowpack in range(snowpacks):
#                     # Generates an index out of the current iteration
#                     # for use with the data_arr that has a flat shape
#                     # that matches the below code.
#                     index = (
#                         (era * models * scenarios * snowpacks)
#                         + (model * scenarios * snowpacks)
#                         + (scenario * snowpacks)
#                         + snowpack
#                     )
#                     slice = data_arr[index]

#                     # Generates a data array that has all NAN values removed
#                     rm_nan_slice = slice[~np.isnan(slice)]

#                     # If there is no data in this slice, set the
#                     # value and percentages to 0 and continue onto the next data slice.
#                     if len(rm_nan_slice) == 0:
#                         return_arr[era][model][scenario][snowpack] = 0
#                         return_percentages[era][model][scenario][snowpack][1] = 0
#                         return_percentages[era][model][scenario][snowpack][2] = 0
#                         return_percentages[era][model][scenario][snowpack][3] = 0
#                         continue

#                     # Generates counts for all 1, 2, or 3 values for beetle risk
#                     uniques = np.unique(rm_nan_slice, return_counts=True)

#                     # Sets the "mode" to the first of the unique values
#                     # Must have at least one value to have gotten to this part
#                     # of the script.
#                     mode = uniques[0][0]
#                     mode_count = uniques[1][0]

#                     # Sets the return percentage of the first count value
#                     # If the mode is 1 above, it will take the count for all 1's
#                     # and divide that by the total size of the array to get the
#                     # percentage of the area that is low risk
#                     return_percentages[era][model][scenario][snowpack][int(mode)] = (
#                         round(mode_count / len(rm_nan_slice) * 100)
#                     )

#                     # If the uniques variable has more than one value, we need to
#                     # check to see if this value should actually be the mode of the
#                     # dataset.
#                     if len(uniques[0]) > 1:
#                         for i in range(1, len(uniques[0])):
#                             # Sets the percentage for this risk value
#                             return_percentages[era][model][scenario][snowpack][
#                                 int(uniques[0][i])
#                             ] = round(uniques[1][i] / len(rm_nan_slice) * 100)

#                             # If the count of the uniques for the next value is higher
#                             # than the current mode count, change the mode and count of
#                             # the mode.
#                             if uniques[1][i] > mode_count:
#                                 mode = uniques[0][i]
#                                 mode_count = uniques[1][i]
#                     return_arr[era][model][scenario][snowpack] = int(mode)
#     return return_arr, return_percentages


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


def run_aggregate_var_polygon(poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon.

    Args:
        poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal mean.

    Returns:
        aggr_results (dict): data representing zonal means within the polygon.

    Notes:
        Fetches data on the individual instances of the singular dimension combinations. Consider validating polygon IDs in `validate_data` or `lat_lon` module.
    """
    polygon = get_poly(poly_id)
    bandname = var_ep_lu["beetles"]["bandname"]
    ds = asyncio.run(
        fetch_beetles_bbox_data(
            polygon.total_bounds, var_ep_lu["beetles"]["cov_id_str"]
        )
    )

    # get all combinations of non-XY dimensions in the dataset and their corresponding encodings
    # and create a dict to hold the results for each combo
    all_dims = ds[bandname].dims
    dimnames = [dim for dim in all_dims if dim not in ["X", "Y"]]
    dim_encodings = var_ep_lu["beetles"]["dim_encodings"]
    iter_coords = list(itertools.product(*[list(ds[dim].values) for dim in dimnames]))
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            dim_encodings[dimname][coord] for coord, dimname in zip(coords, dimnames)
        ]
        dim_combos.append(map_list)
    aggr_results = generate_nested_dict(dim_combos)

    print(aggr_results)
    # package_beetle_data(agg_results, risk_percentages)

    return aggr_results


@routes.route("/beetles/")
@routes.route("/beetles/abstract/")
@routes.route("/beetles/point/")
@routes.route("/beetles/area/")
def about_beetles():
    return render_template("documentation/beetles.html")


@routes.route("/beetles/point/<lat>/<lon>")
def run_point_fetch_all_beetles(lat, lon):
    """Run the async request for beetle risk data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of beetle risk for a single lat / lon point.
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

    try:
        rasdaman_response = asyncio.run(
            fetch_wcs_point_data(x, y, var_ep_lu["beetles"]["cov_id_str"])
        )
        climate_protection = postprocess(
            package_beetle_data(rasdaman_response), "beetles"
        )
        if request.args.get("format") == "csv":
            if type(climate_protection) is not dict:
                # Returns errors if any are generated
                return climate_protection
            # Returns CSV for download

            # TODO: remove line below if place_id is not needed
            place_id = request.args.get("community")

            return create_csv(climate_protection, "beetles", lat=lat, lon=lon)
        # Returns beetle risk levels
        return climate_protection
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500


@routes.route("/beetles/area/<var_id>")
def beetle_area_data_endpoint(var_id):
    """Aggregation data endpoint. Fetch data within polygon area
    for specified variable and return JSON-like dict.

    Args:
        var_id (str): ID for given polygon from polygon endpoint.
    Returns:
        poly_pkg (dict): zonal mode of beetle risk and percentages for AOI polygon

    """

    poly_type = validate_var_id(var_id)
    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
        climate_protection = run_aggregate_var_polygon(var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    climate_protection = nullify_and_prune(climate_protection, "beetles")
    if climate_protection in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    if request.args.get("format") == "csv":
        return create_csv(climate_protection, "beetles", var_id)

    return postprocess(climate_protection, "beetles")
