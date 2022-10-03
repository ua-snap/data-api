import asyncio
import io
import csv
import calendar
import numpy as np
from math import floor
from flask import Blueprint, render_template, request, Response
from shapely.geometry import Point

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import *
from fetch_data import *
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
beetle_coverage_id = "beetle_risk"

dim_encodings = {
    "model": {
        0: "NCAR-CCSM4",
        1: "GFDL-ESM2M",
        2: "HadGEM2-ES",
        3: "MRI-CGCM3",
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
    "snowpack": {0: "low", 1: "medium"},
    "beetle_risk": {0: "no data", 1: "low", 2: "moderate", 3: "high"},
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


def create_csv(packaged_data, place_id, lat=None, lon=None):
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
    metadata += (
        "# Values shown are for risk level for spruce beetle spread in the area.\n"
    )
    output.write(metadata)

    # If this is an area, we include percentages into the CSV fields.
    if (
        "percent-low-risk"
        in packaged_data["1988-2017"]["Daymet"]["Historical"]["low"].keys()
    ):
        fieldnames = [
            "era",
            "model",
            "scenario",
            "snowpack level",
            "beetle risk",
            "percent low risk",
            "percent moderate risk",
            "percent high risk",
        ]
    else:
        fieldnames = [
            "era",
            "model",
            "scenario",
            "snowpack level",
            "beetle risk",
        ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()

    for snowpack in dim_encodings["snowpack"].values():
        try:
            historical = packaged_data["1988-2017"]["Daymet"]["Historical"][snowpack]
            if (
                "percent-low-risk"
                in packaged_data["1988-2017"]["Daymet"]["Historical"][snowpack].keys()
            ):

                writer.writerow(
                    {
                        "era": "1988-2017",
                        "model": "Daymet",
                        "scenario": "Historical",
                        "snowpack level": snowpack,
                        "beetle risk": historical["beetle-risk"],
                        "percent low risk": f"{int(historical['percent-low-risk'])}%",
                        "percent moderate risk": f"{int(historical['percent-medium-risk'])}%",
                        "percent high risk": f"{int(historical['percent-high-risk'])}%",
                    }
                )
            else:
                writer.writerow(
                    {
                        "era": "1988-2017",
                        "model": "Daymet",
                        "scenario": "Historical",
                        "snowpack level": snowpack,
                        "beetle risk": historical["beetle-risk"],
                    }
                )
        except KeyError:
            # if single var query, just ignore attempts to
            # write the non-chosen var
            pass

    for era in dim_encodings["era"].values():
        for model in dim_encodings["model"].values():
            for scenario in dim_encodings["scenario"].values():
                for snowpack in dim_encodings["snowpack"].values():
                    try:
                        projected = packaged_data[era][model][scenario][snowpack]
                        if (
                            "percent-low-risk"
                            in packaged_data[era][model][scenario][snowpack].keys()
                        ):
                            writer.writerow(
                                {
                                    "era": era,
                                    "model": model,
                                    "scenario": scenario,
                                    "snowpack level": snowpack,
                                    "beetle risk": projected["beetle-risk"],
                                    "percent low risk": f"{int(projected['percent-low-risk'])}%",
                                    "percent moderate risk": f"{int(projected['percent-medium-risk'])}%",
                                    "percent high risk": f"{int(projected['percent-high-risk'])}%",
                                }
                            )
                        else:
                            writer.writerow(
                                {
                                    "era": era,
                                    "model": model,
                                    "scenario": scenario,
                                    "snowpack level": snowpack,
                                    "beetle risk": projected["beetle-risk"],
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


def package_beetle_data(beetle_resp, beetle_percents=None):
    """Package the beetle risk data into a nested JSON-like dict.

    Arguments:
        beetle_resp -- the response(s) from the WCS GetCoverage request(s).

    Returns:
        di -- a nested dictionary of all beetle risk values
    """
    # initialize the output dict
    di = dict()

    # Gather historical risk levels
    di["1988-2017"] = dict()
    di["1988-2017"]["Daymet"] = dict()
    di["1988-2017"]["Daymet"]["Historical"] = dict()
    for sni in range(len(beetle_resp[0][0][0])):
        snowpack = dim_encodings["snowpack"][sni]
        di["1988-2017"]["Daymet"]["Historical"][snowpack] = dict()
        di["1988-2017"]["Daymet"]["Historical"][snowpack][
            "beetle-risk"
        ] = dim_encodings["beetle_risk"][int(beetle_resp[0][0][0][sni])]
        if beetle_percents is not None:
            di["1988-2017"]["Daymet"]["Historical"][snowpack][
                "percent-low-risk"
            ] = beetle_percents[0][0][0][sni][1]
            di["1988-2017"]["Daymet"]["Historical"][snowpack][
                "percent-medium-risk"
            ] = beetle_percents[0][0][0][sni][2]
            di["1988-2017"]["Daymet"]["Historical"][snowpack][
                "percent-high-risk"
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
                    di[era][model][scenario][snowpack]["beetle-risk"] = dim_encodings[
                        "beetle_risk"
                    ][int(risk_level)]
                    if beetle_percents is not None:
                        di[era][model][scenario][snowpack][
                            "percent-low-risk"
                        ] = beetle_percents[ei + 1][mi + 1][si + 1][sni][1]
                        di[era][model][scenario][snowpack][
                            "percent-medium-risk"
                        ] = beetle_percents[ei + 1][mi + 1][si + 1][sni][2]
                        di[era][model][scenario][snowpack][
                            "percent-high-risk"
                        ] = beetle_percents[ei + 1][mi + 1][si + 1][sni][3]

    return di


def summarize_within_poly_marr(ds, poly_mask_arr, bandname="Gray"):
    """Summarize a single Data Variable of a xarray.DataSet within a polygon.
    Return the results as a nested dict.

    Args:
        ds (xarray.DataSet): DataSet with "Gray" as variable of
            interest
        poly_mask_arr (numpy.ma.core.MaskedArray): a masked array masking the cells intersecting
            the polygon of interest
        dim_encodings (dict): nested dictionary of thematic key value pairs that characterize the
            data and map integer data coordinates to models, scenarios, eras, snowpacks, etc.
        bandname (str): name of variable in ds, defaults to "Gray" for Rasdaman coverages where
            the name is not given at ingest

    Returns:
        Nested dict of results for all non-X/Y axis combinations,
    """

    # operates on underlying DataArray
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

    # Adds one to each value to generate correct shape and
    # iterates through the data below.
    eras = sel_di["era"] + 1
    models = sel_di["model"] + 1
    scenarios = sel_di["scenario"] + 1
    snowpacks = sel_di["snowpack"] + 1

    return_arr = np.zeros((eras, models, scenarios, snowpacks))

    # Percentages of each slice of the data. Has a last dimension that are
    # integer values representing 1 = low risk, 2 = moderate risk, 3 = high risk
    return_percentages = np.zeros((eras, models, scenarios, snowpacks, 4))
    for era in range(eras):
        for model in range(models):
            for scenario in range(scenarios):
                for snowpack in range(snowpacks):
                    # Generates an index out of the current iteration
                    # for use with the data_arr that has a flat shape
                    # that matches the below code.
                    index = (
                        (era * models * scenarios * snowpacks)
                        + (model * scenarios * snowpacks)
                        + (scenario * snowpacks)
                        + snowpack
                    )
                    slice = data_arr[index]

                    # Generates a data array that has all NAN values removed
                    rm_nan_slice = slice[~np.isnan(slice)]

                    # If there is no data in this slice, set the
                    # value to 0 and continue onto the next data slice.
                    if len(rm_nan_slice) == 0:
                        return_arr[era][model][scenario][snowpack] = 0
                        continue

                    # Generates counts for all 1, 2, or 3 values for beetle risk
                    uniques = np.unique(rm_nan_slice, return_counts=True)

                    # Sets the "mode" to the first of the unique values
                    # Must have at least one value to have gotten to this part
                    # of the script.
                    mode = uniques[0][0]
                    mode_count = uniques[1][0]

                    # Sets the return percentage of the first count value
                    # If the mode is 1 above, it will take the count for all 1's
                    # and divide that by the total size of the array to get the
                    # percentage of the area that is low risk
                    return_percentages[era][model][scenario][snowpack][
                        int(mode)
                    ] = round(mode_count / len(rm_nan_slice) * 100)

                    # If the uniques variable has more than one value, we need to
                    # check to see if this value should actually be the mode of the
                    # dataset.
                    if len(uniques[0]) > 1:
                        for i in range(1, len(uniques[0])):
                            # Sets the percentage for this risk value
                            return_percentages[era][model][scenario][snowpack][
                                int(uniques[0][i])
                            ] = round(uniques[1][i] / len(rm_nan_slice) * 100)

                            # If the count of the uniques for the next value is higher
                            # than the current mode count, change the mode and count of
                            # the mode.
                            if uniques[1][i] > mode_count:
                                mode = uniques[0][i]
                                mode_count = uniques[1][i]
                    return_arr[era][model][scenario][snowpack] = int(mode)
    return return_arr, return_percentages


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
        stats=["mean"],
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

    agg_results, risk_percentages = summarize_within_poly_marr(
        ds_list[-1], poly_mask_arr, bandname
    )

    return package_beetle_data(agg_results, risk_percentages)


@routes.route("/beetles/")
@routes.route("/beetles/abstract/")
def about_beetles():
    return render_template("beetles/abstract.html")


@routes.route("/beetles/point/")
def about_beetles_point():
    return render_template("beetles/point.html")


@routes.route("/beetles/area/")
def about_beetles_area():
    return render_template("beetles/area.html")


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
            place_id = request.args.get("community")
            return return_csv(
                create_csv(postprocess(beetle_risk, "beetles"), None, lat, lon),
                place_id,
                lat,
                lon,
            )
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
        poly_pkg (dict): zonal mode of beetle risk and percentages for AOI polygon

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

        csv_data = create_csv(beetle_risk, var_id)
        return return_csv(csv_data, var_id)
    return postprocess(beetle_risk, "beetles")
