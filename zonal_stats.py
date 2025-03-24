import numpy as np
from rasterio.features import rasterize


def get_scale_factor(grid_cell_area, polygon_area):
    """Calculate the scale factor for a given grid cell area and polygon area. Inputs must be in the same units.
    Args:
        grid_cell_area (float): area of a grid cell
        polygon_area (float): area of a polygon
    Returns:
        int: scale factor, rounded up to the nearest integer
    """

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
        ds_new (xarray.DataArray): xarray data array interpolated to higher resolution
    """
    X = x_dim
    Y = y_dim

    new_lon = np.linspace(ds[X][0].item(), ds[X][-1].item(), ds.sizes[X] * scale_factor)
    new_lat = np.linspace(ds[Y][0].item(), ds[Y][-1].item(), ds.sizes[Y] * scale_factor)

    da_i = ds[var_name].interp(method=method, coords={X: new_lon, Y: new_lat})
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
        poly_array (numpy.ndarray): 2D numpy array with the rasterized polygon
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
        # mode is not computed here because same datasets (e.g. beetles) need to drop nan values first
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
    polygon, dataset, var_name="Gray", x_dim="X", y_dim="Y"
):
    """Interpolate a dataset to a higher resolution and compute polygon zonal statistics for a single variable.
    Args:
        polygon (shapely.Polygon): polygon to compute zonal statistics for. Must be in the same CRS as the dataset.
        dataset (xarray.DataSet): xarray dataset returned from fetching a bbox from a coverage
        var_name (str): name of the variable to interpolate. Default is "Gray", the default name used when ingesting into Rasdaman.
        x_dim (str): name of the x dimension. Default is "X".
        y_dim (str): name of the y dimension. Default is "Y".
    Returns:
        zonal_stats_dict (dict): dictionary of zonal statistics
    """
    # confirm spatial info
    dataset.rio.set_spatial_dims(x_dim, y_dim)
    dataset.rio.write_crs("EPSG:3338", inplace=True)

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
