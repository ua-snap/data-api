import asyncio
import numpy as np
from flask import Blueprint, render_template
import rasterio as rio
import rioxarray
import xarray

# local imports
from generate_requests import generate_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_geoserver_data,
    fetch_bbox_geotiff_from_gs,
    get_poly,
)
from zonal_stats import interpolate_and_compute_zonal_stats
from validate_request import (
    validate_latlon,
    validate_var_id,
)
from postprocessing import postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

elevation_api = Blueprint("elevation_api", __name__)

wms_targets = ["astergdem_min_max_avg"]
wfs_targets = {}
target_crs = (
    "EPSG:3338"  # hard coded for now, since metadata is not fetched from GeoServer
)


def package_astergdem(astergdem_resp):
    """Package ASTER GDEM data in dict"""
    title = "ASTER Global Digital Elevation Model"
    if astergdem_resp[0]["features"] == []:
        return None
    elevation_m = astergdem_resp[0]["features"][0]["properties"]

    di = {
        "title": title,
        "units": "meters difference from sea level",
        "res": "1 kilometer",
    }
    di.update({"max": elevation_m["elevation_max"]})
    di.update({"mean": elevation_m["elevation_avg"]})
    di.update({"min": elevation_m["elevation_min"]})
    return di


@routes.route("/elevation/")
@routes.route("/elevation/abstract/")
@routes.route("/elevation/point/")
@routes.route("/elevation/area/")
def elevation_about():
    return render_template("documentation/elevation.html")


@routes.route("/elevation/point/<lat>/<lon>")
def run_fetch_elevation(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/elevation/60.606/-143.345
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
    try:
        results = asyncio.run(
            fetch_geoserver_data(GS_BASE_URL, "dem", wms_targets, wfs_targets, lat, lon)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data"), 404
        return render_template("500/server_error.html"), 500

    elevation = package_astergdem(results)
    return postprocess(elevation, "elevation")


@routes.route("/elevation/area/<var_id>")
def run_area_fetch_all_elevation(var_id):
    """Endpoint to fetch elevation data within an AOI polygon area.

    Args:
        var_id (str): ID of AOI polygon area, e.g. "NPS7"

    Returns:
        poly_pkg (dict): JSON-like object of aggregated elevation data.
    """
    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
        polygon = get_poly(var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    xstr = f"{polygon.total_bounds[0]},{polygon.total_bounds[2]}"
    ystr = f"{polygon.total_bounds[1]},{polygon.total_bounds[3]}"

    request_str = generate_wcs_getcov_str(
        xstr,
        ystr,
        "astergdem_min_max_avg",
        var_coord=None,
        encoding="GeoTIFF",
        projection=target_crs,
    )

    url = generate_wcs_query_url(request_str, GS_BASE_URL)
    # get the geotiff as a dataset, bands will be order: min, max, and mean
    da = rioxarray.open_rasterio(asyncio.run(fetch_bbox_geotiff_from_gs([url])))
    ds = da.to_dataset(dim="band").rename({1: "min", 2: "max", 3: "mean"})

    # fetch each band from the dataset and calculate zonal stats, adding to the results dict
    results = {
        "title": "ASTER Global Digital Elevation Model Zonal Statistics",
        "units": "meters difference from sea level",
        "res": "1 kilometer",
    }
    for band in list(ds.data_vars):
        # There are no dimensions for this dataset
        dimension_combinations = [{}]

        band_results = interpolate_and_compute_zonal_stats(
            polygon,
            ds[band].to_dataset(name="Gray"),
            target_crs,
            dimension_combinations,
            var_name="Gray",
            x_dim="x",
            y_dim="y",
            compute_full_stats=True,
        )

        combo_zonal_stats_dict = band_results[0][1]

        if band == "min":
            if combo_zonal_stats_dict["min"] is None:
                results[band] = -9999
            else:
                results[band] = int(combo_zonal_stats_dict["min"])
        elif band == "max":
            if combo_zonal_stats_dict["max"] is None:
                results[band] = -9999
            else:
                results[band] = int(combo_zonal_stats_dict["max"])
        elif band == "mean":
            if combo_zonal_stats_dict["mean"] is None:
                results[band] = -9999
            else:
                results[band] = int(combo_zonal_stats_dict["mean"])

    return postprocess(results, "elevation")
