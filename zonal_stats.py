"""A module to interpolate a dataset to a higher resolution and compute zonal statistics for a polygon.
Read more about the Zonal Oversampling Process (ZOP) here: https://github.com/ua-snap/zonal_stats
"""

import logging
import warnings
import os
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import numpy as np
from rasterio.features import rasterize
from rasterio.crs import CRS
from flask import render_template

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


def calculate_zonal_stats(da_i, polygon_array, x_dim, y_dim, compute_full_stats=False):
    """Calculate zonal statistics for an xarray data array and a rasterized polygon array of the same shape.

    Args:
        da_i (xarray.DataArray): xarray data array, interpolated
        polygon_array (numpy.ndarray): 2D numpy array with the rasterized polygon
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
        compute_full_stats (bool): if True, compute all stats; if False, only compute mean
    Returns:
        zonal_stats (dict): dictionary with zonal statistics
    """
    zonal_stats = {}

    # transpose to match numpy array YX order and get values that overlap the polygon
    arr = da_i.transpose(y_dim, x_dim).values
    values = arr[polygon_array == 1]

    if values.size > 0:
        # Suppress warnings for all-NaN slices
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=RuntimeWarning)
            # Convert to float() to ensure JSON serializable Python float, not numpy float32
            mean_val = np.nanmean(values)
            zonal_stats["mean"] = float(mean_val) if not np.isnan(mean_val) else np.nan

            # ALFRESCO and indicators only need the mean value
            # So don't compute the other stats unless requested
            if compute_full_stats:
                zonal_stats["count"] = int(len(values))
                min_val = np.nanmin(values)
                max_val = np.nanmax(values)
                zonal_stats["min"] = float(min_val) if not np.isnan(min_val) else np.nan
                zonal_stats["max"] = float(max_val) if not np.isnan(max_val) else np.nan
                unique_vals, counts = np.unique(values, return_counts=True)
                # Convert numpy types to Python types for JSON serialization
                zonal_stats["unique_values_and_counts"] = {
                    float(k): int(v) for k, v in zip(unique_vals, counts)
                }
    else:
        zonal_stats["mean"] = np.nan
        if compute_full_stats:
            zonal_stats["count"] = 0
            zonal_stats["min"] = np.nan
            zonal_stats["max"] = np.nan
            zonal_stats["unique_values_and_counts"] = {}

    return zonal_stats


def calculate_zonal_means_vectorized(da_i, polygon_array, x_dim, y_dim):
    """
    Calculate zonal means for a 3D xarray data array (time, y, x)
    and a 2D rasterized polygon array of the same shape.
    Args:
        da_i (xarray.DataArray): 3D xarray data array, probably interpolated
        polygon_array (numpy.ndarray): 2D numpy array with the rasterized polygon
        x_dim (str): name of the x dimension
        y_dim (str): name of the y dimension
    Returns:
        time_series_means (list): list of zonal means for each time slice
    """
    # ensure correct dimension order
    arr = da_i.transpose("time", y_dim, x_dim).values

    # create a boolean mask from the polygon array
    mask = polygon_array == 1

    # broadcast the 2D mask to the 3D array, selecting pixels within the polygon for all time slices
    masked_arr = arr[:, mask]

    # compute the mean for each time slice, ignoring NaNs
    time_series_means = np.nanmean(masked_arr, axis=1)

    return time_series_means.tolist()


def interpolate_and_compute_zonal_stats(
    polygon,
    dataset,
    crs,
    dimension_combinations,
    var_name="Gray",
    x_dim="X",
    y_dim="Y",
    compute_full_stats=False,
):
    """Changed to do bulk processing: interpolate once, rasterize once, compute stats for all combinations in parallel.

    Args:
        polygon (geopandas.GeoDataFrame): polygon to compute zonal statistics for. Must be in the same CRS as the dataset.
        dataset (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        crs (str): coordinate reference system of the dataset. Must be in the same CRS as the polygon.
        dimension_combinations (list): list of dicts, each dict maps dimension names to coordinate values
            e.g., [{'era': 0, 'model': 1, 'scenario': 2}, ...]
        var_name (str): name of the variable to interpolate. Default is "Gray", the default name used when ingesting into Rasdaman.
        x_dim (str): name of the x dimension. Default is "X".
        y_dim (str): name of the y dimension. Default is "Y".
        compute_full_stats (bool): if True, compute all stats; if False, only mean
    Returns:
        list: list of tuples (dimension combo, zonal_stats_dict) for each dimension combination
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

    # Use threading with a small number of workers for parallel
    # processing of the zonal statistics
    workers = min(4, max(1, cpu_count() // 2))

    # Only use parallelization if we have enough combinations
    # to justify overhead of starting them up
    use_parallel = workers > 1 and len(dimension_combinations) > 20

    if use_parallel:
        with ThreadPoolExecutor(max_workers=workers) as threads:
            results = list(
                threads.map(
                    lambda combo: (
                        combo,
                        calculate_zonal_stats(
                            da_i.sel(combo),
                            rasterized_polygon_array,
                            x_dim,
                            y_dim,
                            compute_full_stats,
                        ),
                    ),
                    dimension_combinations,
                )
            )
    else:
        results = [
            (
                combo,
                calculate_zonal_stats(
                    da_i.sel(combo),
                    rasterized_polygon_array,
                    x_dim,
                    y_dim,
                    compute_full_stats,
                ),
            )
            for combo in dimension_combinations
        ]

    return results
