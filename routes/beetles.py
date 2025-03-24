import asyncio
import numpy as np
import itertools
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url
from generate_requests import generate_netcdf_wcs_getcov_str
from fetch_data import (
    fetch_bbox_netcdf_list,
    fetch_wcs_point_data,
    describe_via_wcps,
    generate_nested_dict,
    get_poly,
    get_all_possible_dimension_combinations,
)
from zonal_stats import interpolate_and_compute_zonal_stats
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
    validate_var_id,
    get_coverage_encodings,
)
from postprocessing import (
    nullify_and_prune,
    prune_nulls_with_max_intensity,
)
from . import routes
from config import WEST_BBOX, EAST_BBOX

beetles_api = Blueprint("beetles_api", __name__)

var_ep_lu = {
    "beetles": {
        "cov_id_str": "beetle_risk",
        "dim_encodings": None,  # populated below
        "bandnames": ["Gray"],
        "label": None,
    },
}


async def get_beetles_metadata(var_ep_lu):
    """Get the coverage metadata and encodings for ALFRESCO coverages and populate the lookup."""
    beetles_metadata = await describe_via_wcps(var_ep_lu["beetles"]["cov_id_str"])
    var_ep_lu["beetles"]["dim_encodings"] = get_coverage_encodings(beetles_metadata)

    return var_ep_lu


# populate the encodings
var_ep_lu = asyncio.run(get_beetles_metadata(var_ep_lu))

# capitalize "daymet" and "historical" in the dim_encodings dict
var_ep_lu["beetles"]["dim_encodings"]["model"][0] = "Daymet"
var_ep_lu["beetles"]["dim_encodings"]["scenario"][0] = "Historical"

# dict to map the "risk level" integer values of the data to the "protection level" strings
protection_levels_dict = {
    1.0: {
        "pct_label": "percent-high-protection",
        "protection_level": "high",
    },
    2.0: {
        "pct_label": "percent-minimal-protection",
        "protection_level": "minimal",
    },
    3.0: {
        "pct_label": "percent-no-protection",
        "protection_level": "none",
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
    request_strs = []
    request_strs.append(generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id_str))
    urls = [generate_wcs_query_url(request_str) for request_str in request_strs]
    bbox_ds_list = await fetch_bbox_netcdf_list(urls)
    bbox_ds = bbox_ds_list[
        0
    ]  # there is only ever one dataset in the list for this endpoint
    return bbox_ds


def run_aggregate_var_polygon(poly_id):
    """Get data summary (e.g. zonal mean) of single variable in polygon. Fetches data on
    the individual instances of the singular dimension combinations.
    Args:
        poly_id (str or int): the unique `id` used to identify the Polygon for which to compute the zonal stats.
    Returns:
        aggr_results (dict): data representing zonal stats within the polygon.
    """
    polygon = get_poly(poly_id)
    bandname = var_ep_lu["beetles"]["bandnames"][0]
    ds = asyncio.run(
        fetch_beetles_bbox_data(
            polygon.total_bounds, var_ep_lu["beetles"]["cov_id_str"]
        )
    )

    # using non-XY dimension names from the dataset and dim_encodings from the coverage
    # create the nested dict to hold results
    all_dims = ds[bandname].dims
    dimnames = [dim for dim in all_dims if dim not in ["X", "Y"]]
    dim_encodings = var_ep_lu["beetles"]["dim_encodings"]
    iter_coords = list(
        itertools.product(*[dim_encodings[dim].keys() for dim in dimnames])
    )
    dim_combos = get_all_possible_dimension_combinations(
        iter_coords, dimnames, dim_encodings
    )
    aggr_results = generate_nested_dict(dim_combos)

    # fetch each dim combo from the dataset and calculate zonal stats, adding to the results dict
    for coords, dim_combo in zip(iter_coords, dim_combos):
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        combo_ds = ds.sel(sel_di)
        combo_zonal_stats_dict = interpolate_and_compute_zonal_stats(polygon, combo_ds)
        vals_counts_dict = combo_zonal_stats_dict["unique_values_and_counts"]

        # if nan is the only value in the subset, set all null results for this dim combo (will get pruned)
        if len(vals_counts_dict) == 1 and np.isnan(list(vals_counts_dict.keys())).any():
            results = None

        # otherwise, populate results dict using only non-nan values
        # with the counts from vals_counts_dict expressed as percentages
        else:
            results = dict()
            non_nan_vals_counts_dict = {
                k: v for k, v in vals_counts_dict.items() if np.isnan(k) == False
            }
            total_cells = sum(non_nan_vals_counts_dict.values())

            for key in non_nan_vals_counts_dict:
                results[key] = round(non_nan_vals_counts_dict[key] / total_cells * 100)

            # if any protection levels were not found in the polygon area, set the percentages to 0
            for level in protection_levels_dict.keys():
                if level not in results.keys():
                    results[level] = 0

            # get the climate protection level to the protection level with the highest percentage (i.e., the mode)
            highest_pct_key = max(results, key=results.get)

            # replace the results keys with the full string from the protection_levels_dict
            for key in results.keys():
                results[protection_levels_dict[key]["pct_label"]] = results.pop(key)

            # and finally add the protection level to the results dict
            results["climate-protection"] = protection_levels_dict[highest_pct_key][
                "protection_level"
            ]

        # use the dim_combo to index into the results dict (era, model, scenario, snowpack)
        aggr_results[dim_combo[0]][dim_combo[1]][dim_combo[2]][dim_combo[3]] = results

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

        # using the dimension names and dim_encodings, create the nested dict to hold results
        dim_encodings = var_ep_lu["beetles"]["dim_encodings"]
        dimnames = [
            "era",
            "model",
            "scenario",
            "snowpack",
        ]  # we could get these directly from the encodings, but the encodings dict includes "risk" dimension which is actually not present in the coverage ... so we define it explicitly here
        iter_coords = list(
            itertools.product(*[dim_encodings[dim].keys() for dim in dimnames])
        )
        dim_combos = get_all_possible_dimension_combinations(
            iter_coords, dimnames, dim_encodings
        )
        results = generate_nested_dict(dim_combos)

        # populate the results dict with the fetched data
        # using the coords to index into the rasdaman response
        for coords, dim_combo in zip(iter_coords, dim_combos):
            protection_level = rasdaman_response[coords[0]][coords[1]][coords[2]][
                coords[3]
            ]
            if protection_level is not None:
                results[dim_combo[0]][dim_combo[1]][dim_combo[2]][dim_combo[3]] = {
                    "climate-protection": protection_levels_dict[protection_level][
                        "protection_level"
                    ]
                }

        results = nullify_and_prune(results, "beetles")
        results = prune_nulls_with_max_intensity(results)

        if results in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        if request.args.get("format") == "csv":
            return create_csv(results, "beetles", lat=lat, lon=lon)

        else:
            return results

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
        results = run_aggregate_var_polygon(var_id)
        results = nullify_and_prune(results, "beetles")
        results = prune_nulls_with_max_intensity(results)

        if results in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        if request.args.get("format") == "csv":
            return create_csv(results, "beetles", var_id)

        return results

    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
