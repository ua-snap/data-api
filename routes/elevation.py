import asyncio
import math

from flask import Blueprint, render_template
import rioxarray

# local imports
from generate_requests import generate_netcdf_wcs_getcov_str, generate_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_data,
    fetch_bbox_geotiff_from_gs,
    fetch_bbox_netcdf,
    fetch_geoserver_data,
    get_poly,
)
from zonal_stats import (
    calculate_zonal_stats,
    get_scale_factor,
    interpolate,
    interpolate_and_compute_zonal_stats,
    rasterize_polygon,
)
from validate_request import (
    project_latlon,
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

era5_4km_elevation_coverage_id = "era5_4km_elevation"
# below hardcoded to match the pattern for the ASTER GDEM endpoint
ERA5_4KM_ELEVATION_METADATA = {
    "title": "WRF Dynamically Downscaled ERA5 Reanalysis 4 km Elevation Grid",
    "units": "meters above sea level",
    "res": "4 kilometer",
}


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


def package_era5_4km_elevation(era5_resp):
    """Package ERA5 4km elevation point data in dict.
    Args:
        era5_resp (list): Response from Rasdaman
    Returns:
        dict: JSON-like object of ERA5 4km elevation point data
    """
    elevation_value = era5_resp
    if isinstance(elevation_value, (list, tuple)):
        if len(elevation_value) == 0:
            return None
        elevation_value = elevation_value[0]

    if elevation_value is None or isinstance(elevation_value, (list, tuple)):
        return None

    try:
        # want integer precision on elevation value (meters)
        elevation_value = int(round(float(elevation_value)))
    except (TypeError, ValueError):
        return None

    return {
        **ERA5_4KM_ELEVATION_METADATA,
        "elevation": elevation_value,
    }


async def fetch_era5_4km_elevation_area_data(polygon):
    """Fetch ERA5 4km elevation bbox data for a polygon.

    Args:
        polygon (GeoDataFrame): Polygon for which to compute zonal statistics

    Returns:
        xarray.Dataset: netCDF dataset for the bbox
    """
    bbox_bounds = polygon.total_bounds  # (xmin, ymin, xmax, ymax)
    request_str = generate_netcdf_wcs_getcov_str(
        bbox_bounds, era5_4km_elevation_coverage_id
    )
    url = generate_wcs_query_url(request_str)
    ds = await fetch_bbox_netcdf([url])
    return ds


def process_era5_4km_elevation_zonal_stats(polygon, ds):
    """Process zonal statistics for ERA5 4km elevation.

    Args:
        polygon (GeoDataFrame): Target polygon
        ds (xarray.Dataset): Dataset with elevation variable

    Returns:
        dict: Zonal stats with min, mean, max
    """
    spatial_resolution = ds.rio.resolution()
    grid_cell_area_m2 = abs(spatial_resolution[0]) * abs(spatial_resolution[1])
    polygon_area_m2 = polygon.area
    scale_factor = get_scale_factor(grid_cell_area_m2, polygon_area_m2)

    da_i = interpolate(ds, "elevation", "X", "Y", scale_factor, method="nearest")
    rasterized_polygon_array = rasterize_polygon(da_i, "X", "Y", polygon)
    return calculate_zonal_stats(
        da_i, rasterized_polygon_array, "X", "Y", compute_full_stats=True
    )


def package_era5_4km_elevation_area(zonal_stats):
    """Package ERA5 4km elevation area data in dict.
    Args:
        zonal_stats (dict): Zonal statistics for the area
    Returns:
        dict: JSON-like object of ERA5 4km elevation area data
    """
    results = dict(ERA5_4KM_ELEVATION_METADATA)
    for stat in ["min", "mean", "max"]:
        value = zonal_stats.get(stat)
        if value is None:
            results[stat] = -9999
            continue
        try:
            if isinstance(value, float) and math.isnan(value):
                results[stat] = -9999
            else:
                results[stat] = int(value)
        except (TypeError, ValueError):
            results[stat] = -9999
    return results


@routes.route("/elevation/")
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


@routes.route("/elevation/point/era5_4km/<lat>/<lon>")
def run_fetch_era5_4km_elevation(lat, lon):
    """Fetch ERA5 4km elevation point data from Rasdaman.
    Args:
        lat (float): latitude
        lon (float): longitude
    Returns:
        JSON-like object of ERA5 4km elevation data
    """
    # below will check same geotiff as era5wrf routes do
    validation = validate_latlon(lat, lon, ["era5_4km"])
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 404:
        return render_template("404/no_data.html"), 404
    # could render the same template as era5wrf routes do, but this is fine for now
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    x, y = project_latlon(lat, lon, 3338)

    try:
        request_str = generate_wcs_getcov_str(x, y, era5_4km_elevation_coverage_id)
        url = generate_wcs_query_url(request_str)
        rasdaman_response = asyncio.run(fetch_data([url]))
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    elevation = package_era5_4km_elevation(rasdaman_response)
    return postprocess(elevation, "era5wrf_4km_elevation")


@routes.route("/elevation/area/era5_4km/<var_id>")
def run_area_fetch_era5_4km_elevation(var_id):
    """Fetch ERA5 4km elevation zonal statistics within an AOI polygon area.
    Args:
        var_id (str): ID of AOI polygon area, e.g. "NPS7"
    Returns:
        JSON-like object of ERA5 4km elevation zonal statistics
    """
    poly_type = validate_var_id(var_id)

    if type(poly_type) is tuple:
        return poly_type

    try:
        polygon = get_poly(var_id, crs=3338)
    except Exception:
        return render_template("422/invalid_area.html"), 422

    try:
        ds = asyncio.run(fetch_era5_4km_elevation_area_data(polygon))
        zonal_stats = process_era5_4km_elevation_zonal_stats(polygon, ds)
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    elevation = package_era5_4km_elevation_area(zonal_stats)
    return postprocess(elevation, "era5wrf_4km_elevation")
