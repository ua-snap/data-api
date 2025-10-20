"""A module to interpolate a dataset to a higher resolution and compute zonal statistics for a polygon.
Read more about the Zonal Oversampling Process (ZOP) here: https://github.com/ua-snap/zonal_stats
"""

import logging
import time


import numpy as np
from rasterio.features import rasterize
from rasterio.crs import CRS
from flask import render_template
import xarray as xr

logger = logging.getLogger(__name__)


def get_scale_factor(grid_cell_area, polygon_area):
    """Calculate the scale factor for a given grid cell area and polygon area. Inputs must be in the same units.
    Args:
        grid_cell_area (float): area of a grid cell
        polygon_area (float): area of a polygon
    Returns:
        int: scale factor, rounded up to the nearest integer
    """

    if grid_cell_area <= 0:
        return render_template("500/server_error.html"), 500

    def hyp_function(x, m, b, c, h):
        y = (m * x + b) / (x - c) + h
        return y

    x = polygon_area / grid_cell_area
    m = 0
    b = 350
    c = -24
    h = 1

    scale_factor = np.ceil(hyp_function(x, m, b, c, h))[0]
    return int(scale_factor)


def interpolate(ds, var_name, x_dim, y_dim, scale_factor, method):
    """Interpolate the array for a single variable from an xarray dataset to a higher resolution.

    Args:
        ds (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        var_name (str): name of the variable to interpolate
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
        scale_factor (int): multiplier to increase the resolution by
        method (str): method to use for interpolation

    Returns:
        da_i (xarray.DataArray): xarray data array interpolated to higher resolution
    """
    x = x_dim
    y = y_dim

    new_lon = np.linspace(ds[x][0].item(), ds[x][-1].item(), ds.sizes[x] * scale_factor)
    new_lat = np.linspace(ds[y][0].item(), ds[y][-1].item(), ds.sizes[y] * scale_factor)

    da_i = ds[var_name].interp(method=method, coords={x: new_lon, y: new_lat})
    da_i = da_i.rio.set_spatial_dims(x_dim, y_dim, inplace=True)

    return da_i


def rasterize_polygon(da_i, x_dim, y_dim, polygon):
    """Rasterize a polygon to the same shape as the dataset.
    Args:
        da_i (xarray.DataArray): xarray data array, probably interpolated
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
        polygon (shapely.Polygon): polygon to rasterize. Must be in the same CRS as the dataset.
    Returns:
        rasterized_polygon_array (numpy.ndarray): 2D numpy array with the rasterized polygon
    """
    rasterized_polygon_array = rasterize(
        [(polygon.geometry.iloc[0], 1)],
        out_shape=(
            da_i[y_dim].values.shape[0],
            da_i[x_dim].values.shape[0],
        ),  # must be YX order for numpy array!
        transform=da_i.rio.transform(
            recalc=True
        ),  # must recalc since we interpolated, otherwise the old stored transform is used and rasterized polygon is not aligned
        fill=0,
        all_touched=False,
    )

    return rasterized_polygon_array


def calculate_zonal_stats(da_i, polygon_array, x_dim, y_dim):
    """Calculate zonal statistics for an xarray data array and a rasterized polygon array of the same shape.
    Args:
        da_i (xarray.DataArray): xarray data array, probably interpolated
        polygon_array (numpy.ndarray): 2D numpy array with the rasterized polygon
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
    Returns:
        zonal_stats (dict): dictionary of zonal statistics
    """
    zonal_stats = {}

    # transpose to match numpy array YX order and get values that overlap the polygon
    arr = da_i.transpose(y_dim, x_dim).values
    values = arr[polygon_array == 1].tolist()

    if values:
        zonal_stats["count"] = len(values)
        zonal_stats["mean"] = np.nanmean(values)
        zonal_stats["min"] = np.nanmin(values)
        zonal_stats["max"] = np.nanmax(values)
        # the following stat can be used to compute a mode
        # mode is not computed directly here because same datasets (e.g. beetles) need to drop nan values first
        # and the np.mode function does not support dropping nans, and does not return percentages of unique values (as required by beetles)
        unique_vals, counts = np.unique(values, return_counts=True)
        zonal_stats["unique_values_and_counts"] = dict(zip(unique_vals, counts))

    else:
        zonal_stats["count"] = 0
        zonal_stats["mean"] = np.nan
        zonal_stats["min"] = np.nan
        zonal_stats["max"] = np.nan
        zonal_stats["unique_values_and_counts"] = {}

    return zonal_stats


def interpolate_and_compute_zonal_stats(
    polygon, dataset, crs, var_name="Gray", x_dim="X", y_dim="Y"
):
    """Interpolate a dataset to a higher resolution and compute polygon zonal statistics for a single variable.
    Args:
        polygon (geopandas.GeoDataFrame): polygon to compute zonal statistics for. Must be in the same CRS as the dataset.
        dataset (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        crs (str): coordinate reference system of the dataset. Must be in the same CRS as the polygon.
        var_name (str): name of the variable to interpolate. Default is "Gray", the default name used when ingesting into Rasdaman.
        x_dim (str): name of the x dimension. Default is "X".
        y_dim (str): name of the y dimension. Default is "Y".
    Returns:
        zonal_stats_dict (dict): dictionary of zonal statistics
    """

    # test if the polygon is in the same CRS as the dataset
    if str(polygon.crs) != crs:
        logger.debug("Polygon and dataset CRS do not match")
        return render_template("500/server_error.html"), 500

    # make sure dataset CRS is projected, not geographic
    if not CRS.from_string(crs).is_projected:
        logger.debug("Dataset CRS is not projected")
        return render_template("500/server_error.html"), 500

    # confirm spatial info
    dataset.rio.set_spatial_dims(x_dim, y_dim)
    dataset.rio.write_crs(crs, inplace=True)

    # calculate the scale factor, assuming square pixels and projection in meters
    spatial_resolution = dataset.rio.resolution()
    grid_cell_area_m2 = abs(spatial_resolution[0]) * abs(spatial_resolution[1])
    polygon_area_m2 = polygon.area
    scale_factor = get_scale_factor(grid_cell_area_m2, polygon_area_m2)

    # interpolate the dataset and rasterize the polygon
    da_i = interpolate(dataset, var_name, x_dim, y_dim, scale_factor, method="nearest")

    rasterized_polygon_array = rasterize_polygon(da_i, x_dim, y_dim, polygon)

    # calculate zonal statistics
    zonal_stats_dict = calculate_zonal_stats(
        da_i, rasterized_polygon_array, x_dim, y_dim
    )

    return zonal_stats_dict


def vectorized_zonal_means_nd(
    polygon,
    dataset,
    crs,
    var_name="Gray",
    x_dim="X",
    y_dim="Y",
    method="nearest",
):
    """Compute vectorized zonal means over all non-spatial dims.

    Args:
        polygon (geopandas.GeoDataFrame): polygon to compute zonal statistics for. Must be in the same CRS as the dataset.
        dataset (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        crs (str): coordinate reference system of the dataset. Must be in the same CRS as the polygon.
        var_name (str): name of the variable to interpolate. Default is "Gray".
        x_dim (str): name of the x dimension. Default is "X".
        y_dim (str): name of the y dimension. Default is "Y".
        method (str): interpolation method for spatial dims. Default is "nearest".

    Returns:
        xarray.DataArray | tuple: means over spatial dims for each non-spatial coordinate, or (template, code) on error.
    """
    time_start = time.time()
    # Validate CRS alignment
    
    if str(polygon.crs) != crs:
        logger.debug("Polygon and dataset CRS do not match")
        return render_template("500/server_error.html"), 500

    if not CRS.from_string(crs).is_projected:
        logger.debug("Dataset CRS is not projected")
        return render_template("500/server_error.html"), 500

    # Ensure spatial info
    dataset.rio.set_spatial_dims(x_dim, y_dim)
    dataset.rio.write_crs(crs, inplace=True)

    # Compute scale factor, assuming square pixels and projected CRS in meters
    spatial_resolution = dataset.rio.resolution()
    grid_cell_area_m2 = abs(spatial_resolution[0]) * abs(spatial_resolution[1])
    polygon_area_m2 = polygon.area
    scale_factor = get_scale_factor(grid_cell_area_m2, polygon_area_m2)

    # Interpolate spatial dimensions once, broadcasting across all other dims
    x = x_dim
    y = y_dim
    new_lon = np.linspace(dataset[x][0].item(), dataset[x][-1].item(), dataset.sizes[x] * scale_factor)
    new_lat = np.linspace(dataset[y][0].item(), dataset[y][-1].item(), dataset.sizes[y] * scale_factor)

    da = dataset[var_name]
    da_i = da.interp(method=method, coords={x: new_lon, y: new_lat})
    da_i = da_i.rio.set_spatial_dims(x_dim, y_dim, inplace=True)

    # Rasterize polygon once at interpolated resolution
    mask_np = rasterize(
        [(polygon.geometry.iloc[0], 1)],
        out_shape=(da_i[y_dim].shape[0], da_i[x_dim].shape[0]),
        transform=da_i.rio.transform(recalc=True),
        fill=0,
        all_touched=False,
    ).astype(bool)

    # Make mask a DataArray so it broadcasts over all non-spatial dims
    mask = xr.DataArray(mask_np, dims=(y_dim, x_dim), coords={y_dim: da_i[y_dim], x_dim: da_i[x_dim]})

    # Apply mask and compute mean across spatial dims, retaining non-spatial dims
    masked = da_i.where(mask)
    means = masked.mean(dim=(y_dim, x_dim), skipna=True)

    time_end = time.time()
    logger.info(f"Vectorized zonal means for {var_name} completed in {time_end - time_start:.2f} seconds")
    return means
