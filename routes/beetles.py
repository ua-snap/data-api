import asyncio
import io
import csv
import calendar
import numpy as np
from scipy import stats as st
from math import floor
from flask import (
    Blueprint,
    render_template,
    request,
    Response
)
from shapely.geometry import Point
# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import *
from fetch_data import *
from fetch_data import (
    fetch_data,
    get_from_dict,
    summarize_within_poly,
    csv_metadata,
    fetch_wcs_point_data,
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
from luts import type_di
from . import routes
from config import WEST_BBOX, EAST_BBOX

beetles_api = Blueprint("beetles_api", __name__)
# Rasdaman targets
beetle_coverage_id = "template_beetle_risk"
#
# beetle_dim_encodings = asyncio.run(
#     get_dim_encodings("template_beetle_risk")
# )

dim_encodings = {
    "model": {
        0: "GFDL-ESM2M",
        1: "HadGEM2-ES",
        2: "MRI-CGCM3",
        3: "NCAR-CCSM4",

    },
    "scenario": {
        0: "rcp45",
        1: "rcp85",
    },
    "era": {
        0: "2010-2039",
        1: "2040-2069",
        2: "2070-2099",
    },
    "snowpack": {
        0: "high",
        1: "low",
        2: "medium",
    },
}


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
    return bbox_ds_list


def create_csv(
    packaged_data, place_id, lat=None, lon=None
):
    """
    Returns a CSV version of the fetched data, as a string.

    Args:
        packaged_data (json): JSON-like data package output
            from the run_fetch_* and run_aggregate_* functions
        place_id (str): community or area ID unless just a lat/lon value
        lat: latitude unless an area
        lon: longitude unless an area

    Returns:
        string of CSV data
    """

    output = io.StringIO()

    place_name, place_type = place_name_and_type(place_id)

    metadata = csv_metadata(place_name, place_id, place_type, lat, lon)
    metadata += "# Values shown are given as low risk = 0, medium risk = 1 and high risk = 2\n"
    output.write(metadata)

    fieldnames = [
        "era",
        "model",
        "scenario",
        "snowpack-level",
        "beetle-risk",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()

    for era in dim_encodings["era"].values():
        for model in dim_encodings["model"].values():
            for scenario in dim_encodings["scenario"].values():
                for snowpack in dim_encodings["snowpack"].values():
                    try:
                        writer.writerow(
                            {
                                "era": era,
                                "model": model,
                                "scenario": scenario,
                                "snowpack-level": snowpack,
                                "beetle-risk": packaged_data[era][model][scenario][snowpack],
                            }
                        )
                    except KeyError:
                        # if single var query, just ignore attempts to
                        # write the non-chosen var
                        pass

    return output.getvalue()


def return_csv(csv_data, place_id, lat=None, lon=None):
    """Return the CSV data as a download

    Args:
        csv_data (?): csv data created with create_csv() function
        place_id (str): community or area ID unless just a lat/lon value
        lat: latitude unless an area
        lon: longitude unless an area

    Returns:
        CSV Response
    """

    place_name, place_type = place_name_and_type(place_id)

    if place_name is not None:
        filename = "Beetle Risk for " + quote(place_name) + ".csv"
    else:
        filename = "Beetle Risk for " + lat + ", " + lon + ".csv"

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


def package_beetle_data(beetle_resp):
    """Package the beetle risk data into a nested JSON-like dict.

    Arguments:
        beetle_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all beetle risk values
    """
    # initialize the output dict
    di = dict()

    # Dimensions:
    # era (0 = 2010-2039, 1 = 2040-2069, 2 = 2070-2099)
    # models (0 = GFDL-ESM2M, 1 = HadGEM2-ES, 2 = MRI-CGCM3, 3 = NCAR-CCSM4)
    # scenarios (0 = rcp45, 1 = rcp85)
    # snowpack (0 = high, 1 = low, 2 = medium)
    # risk (1 = low, 2 = medium, 3 = high)
    for ei, mod_li in enumerate(beetle_resp):
        era = dim_encodings["era"][ei]
        di[era] = dict()
        for mi, sc_li in enumerate(mod_li):
            model = dim_encodings["model"][mi]
            di[era][model] = dict()
            for si, sn_li in enumerate(sc_li):
                scenario = dim_encodings["scenario"][si]
                di[era][model][scenario] = dict()
                for sni, ri_li in enumerate(sn_li):
                    snowpack = dim_encodings["snowpack"][sni]
                    di[era][model][scenario][snowpack] = int(beetle_resp[ei][mi][si][sni])

    return di


def summarize_within_poly_marr(
    ds, poly_mask_arr, bandname="Gray", var_ep="Gray"
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
        var_ep (str): variable (flammability or veg_type)

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

    eras = len(dim_encodings['era'].keys())
    models = len(dim_encodings['model'].keys())
    scenarios = len(dim_encodings['scenario'].keys())
    snowpacks = len(dim_encodings['snowpack'].keys())

    return_arr = np.zeros((eras,models,scenarios,snowpacks))
    for era in range(eras):
        for model in range(models):
            for scenario in range(scenarios):
                for snowpack in range(snowpacks):
                    index = (era * 24) + (model * 6) + (scenario * 3) + snowpack
                    slice = data_arr[index]
                    uniques = np.unique(slice[~np.isnan(slice)], return_counts=True)
                    mode = uniques[0][0]
                    if len(uniques[0]) > 1:
                        for i in range(len(uniques[0]) - 1):
                            if uniques[1][i + 1] > uniques[1][i]:
                                mode = uniques[0][i + 1]
                    return_arr[era][model][scenario][snowpack] = int(mode)
    return return_arr


def get_poly_mask_arr(ds, poly, bandname):
    """Get the polygon mask array from an xarray dataset, intended to be recycled for rapid
    zonal summary across results from multiple WCS requests for the same bbox. Wrapper for
    rasterstats zonal_stats().

    Args:
        ds (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        poly (shapely.Polygon): polygon to create mask from
        bandname (str): name of the DataArray containing the data

    Returns:
        cropped_poly_mask (numpy.ma.core.MaskedArra): a masked array masking the cells
            intersecting the polygon of interest, cropped to the right shape
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
        stats=["median"],
        raster_out=True,
    )[0]["mini_raster_array"]
    cropped_poly_mask = poly_mask_arr[0 : xy_shape[1], 0 : xy_shape[0]]
    return cropped_poly_mask


def run_aggregate_var_polygon(poly_gdf, poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon.

    Args:
        poly_gdf (GeoDataFrame): the object from which to fetch the polygon, e.g. the HUC 8 geodataframe for watershed polygons
        poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal mean.

    Returns:
        aggr_results (dict): data representing zonal means within the polygon.

    Notes:
        Fetches data on the individual instances of the singular dimension combinations. Consider validating polygon IDs in `validate_data` or `lat_lon` module.
    """
    poly = get_poly_3338_bbox(poly_gdf, poly_id)

    ds_list = asyncio.run(fetch_beetles_bbox_data(poly.bounds, beetle_coverage_id))

    bandname = "Gray"
    poly_mask_arr = get_poly_mask_arr(ds_list[0], poly, bandname)

    agg_results = summarize_within_poly_marr(
        ds_list[-1], poly_mask_arr, bandname
    )

    return package_beetle_data(agg_results)


@routes.route("/beetles/")
@routes.route("/beetles/abstract/")
def about_beetles():
    return render_template("seaice/abstract.html")


@routes.route("/beetles/point/")
def about_beetles_point():
    return render_template("seaice/point.html")


@routes.route("/beetles/point/<lat>/<lon>/")
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
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, beetle_coverage_id))
        beetle_risk = postprocess(package_beetle_data(rasdaman_response), "beetles")
        if request.args.get("format") == "csv":
            if type(beetle_risk) is not dict:
                # Returns errors if any are generated
                return beetle_risk
            # Returns CSV for download
            return return_csv(create_csv(
                postprocess(beetle_risk, "beetles"), None, lat, lon
            ), None, lat, lon)
        # Returns beetle risk levels
        return beetle_risk
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
        poly_pkg (dict): zonal mean of variable(s) for AOI polygon

    """

    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
        beetle_risk = run_aggregate_var_polygon(type_di[poly_type], var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    if request.args.get("format") == "csv":
        beetle_risk = nullify_and_prune(beetle_risk, "beetles")
        if beetle_risk in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        csv_data = create_csv(beetle_risk, var_ep, var_id)
        return return_csv(csv_data, var_ep, var_id)
    return postprocess(beetle_risk, "beetles")
