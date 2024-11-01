import asyncio
from flask import Blueprint, render_template
import rasterio as rio

# local imports
from generate_requests import generate_wcs_getcov_str, get_wcs_xy_str_from_bbox_bounds
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_geoserver_data,
    fetch_bbox_geotiff_from_gs,
    geotiff_zonal_stats,
    get_poly_3338_bbox,
)
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


def package_zonal_stats(src, poly):
    transform = src.transform
    min_band = src.read(1)
    max_band = src.read(2)
    mean_band = src.read(3)
    zonal_mu = geotiff_zonal_stats(poly, mean_band, src.nodata, transform, ["mean"])
    zonal_mu[0]["mean"] = int(zonal_mu[0]["mean"])
    zonal_min = geotiff_zonal_stats(poly, min_band, src.nodata, transform, ["min"])
    zonal_max = geotiff_zonal_stats(poly, max_band, src.nodata, transform, ["max"])

    di = {
        "title": "ASTER Global Digital Elevation Model Zonal Statistics",
        "units": "meters difference from sea level",
        "res": "1 kilometer",
    }
    di.update(zonal_min[0])
    di.update(zonal_max[0])
    di.update(zonal_mu[0])
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
        poly = get_poly_3338_bbox(var_id)
    except:
        return render_template("422/invalid_area.html"), 422

    wcsxy = get_wcs_xy_str_from_bbox_bounds(poly)

    request_str = generate_wcs_getcov_str(
        wcsxy.xstr,
        wcsxy.ystr,
        "astergdem_min_max_avg",
        var_coord=None,
        encoding="GeoTIFF",
    )

    url = generate_wcs_query_url(request_str, GS_BASE_URL)
    with rio.open(asyncio.run(fetch_bbox_geotiff_from_gs([url]))) as src:
        poly_pkg = package_zonal_stats(src, poly)

    return postprocess(poly_pkg, "elevation")
