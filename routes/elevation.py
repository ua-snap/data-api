import asyncio
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)
import numpy as np
import rasterio as rio

# local imports
from generate_requests import generate_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_data,
    fetch_data_api,
    fetch_bbox_geotiff_from_gs,
    geotiff_zonal_stats,
)
from validate_request import (
    validate_latlon,
    validate_huc8,
    validate_akpa,
    project_latlon,
)
from validate_data import get_poly_3338_bbox, nullify_nodata, postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from luts import huc8_gdf, akpa_gdf
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
        "z": elevation_m,
        "units": "meters difference from sea level",
        "res": "1 kilometer",
    }
    return di


@routes.route("/elevation/")
@routes.route("/elevation/abstract/")
def elevation_about():
    return render_template("elevation/abstract.html")


@routes.route("/elevation/point/")
def elevation_about_point():
    return render_template("elevation/point.html")


@routes.route("/elevation/huc/")
def z_about_huc():
    return render_template("elevation/huc.html")


@routes.route("/elevation/protectedarea/")
def z_about_protectedarea():
    return render_template("elevation/protectedarea.html")


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
            fetch_data_api(GS_BASE_URL, "dem", wms_targets, wfs_targets, lat, lon)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data"), 404
        return render_template("500/server_error.html"), 500

    elevation = package_astergdem(results)
    return elevation


@routes.route("/elevation/huc/<huc_id>")
def run_huc_fetch_all_elevation(huc_id):
    """Endpoint to fetch elevation data within a HUC.

    Args: huc_id (int): 8-digit HUC ID.

    Returns:
        huc_pkg (dict): JSON-like object containing aggregated data.
    """
    validation = validate_huc8(huc_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = get_poly_3338_bbox(huc8_gdf, huc_id)
    except:
        return render_template("422/invalid_huc.html"), 422

    bounds = poly.bounds
    (x1, y1, x2, y2) = bounds
    x = f"{x1},{x2}"
    y = f"{y1},{y2}"
    request_str = generate_wcs_getcov_str(
        x, y, "astergdem_min_max_avg", var_coord=None, encoding="GeoTIFF"
    )

    url = generate_wcs_query_url(request_str, GS_BASE_URL)
    with rio.open(asyncio.run(fetch_bbox_geotiff_from_gs([url]))) as src:

        transform = src.transform
        min_band = src.read(1)
        max_band = src.read(2)
        mean_band = src.read(3)
        zonal_mu = geotiff_zonal_stats(poly, mean_band, transform, ["mean"])
        zonal_mu[0]["mean"] = int(zonal_mu[0]["mean"])
        zonal_min = geotiff_zonal_stats(poly, min_band, transform, ["min"])
        zonal_max = geotiff_zonal_stats(poly, max_band, transform, ["max"])

    di = dict(zonal_min[0])
    di.update(zonal_max[0])
    di.update(zonal_mu[0])

    return di
