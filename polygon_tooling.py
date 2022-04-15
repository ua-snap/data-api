"""This module contains small functions that facilitate polygon "area-of-interest" 
queries against various backend spatial databases. The functions here perform spatial packaging and summarzing
of the various repsonses."""

import itertools
import numpy as np
from rasterstats import zonal_stats
from collections import namedtuple
from fetch_data import generate_nested_dict, get_from_dict


def get_all_combinations_of_decoded_dim_values(
    xr_data_array, xr_data_set, dim_encodings
):
    """Get all possible combinations of a decoded dimensional values (i.e. models / scenarios / decades) for a single DataArray.
    This function gets used to summarize data by polygons."""
    all_dims = xr_data_array.dims
    dimnames = [dimname for dimname in all_dims if dimname not in ("X", "Y")]
    iter_coords = list(
        itertools.product(*[list(xr_data_set[dimname].values) for dimname in dimnames])
    )

    # generate all combinations of decoded coordinate values
    dim_combos = []
    for coords in iter_coords:
        map_list = [
            dim_encodings[dimname][coord] for coord, dimname in zip(coords, dimnames)
        ]
        dim_combos.append(map_list)
    DimInfo = namedtuple("DimInfo", "all_dims dimnames iter_coords dim_combos")
    dim_info = DimInfo(all_dims, dimnames, iter_coords, dim_combos)
    return dim_info


def create_nparray_for_zonal_stats(da, all_dims, dimnames, iter_coords, dim_combos):
    data_arr = []
    for coords, map_list in zip(iter_coords, dim_combos):
        sel_di = {dimname: int(coord) for dimname, coord in zip(dimnames, coords)}
        data_arr.append(da.sel(sel_di).values)
    data_arr = np.array(data_arr)

    return data_arr


def mask_and_crop_zonal_stats_arr(data_arr, poly_mask_arr):
    # crop_shape = data_arr[0].shape
    # cropped_poly_mask = poly_mask_arr[0:crop_shape[0], 0:crop_shape[1]]
    data_arr_mask = np.broadcast_to(poly_mask_arr.mask, data_arr.shape)
    data_arr[data_arr_mask] = np.nan

    # Set any remaining nodata values to nan if they snuck through the mask.
    data_arr[data_arr == -9.223372e18] = np.nan
    return data_arr


def transpose_2d_spatial_slices_if_X_is_rows(all_dims, data_arr):
    # need to transpose the 2D spatial slices if X is the "rows" dimension
    # this is used to summarize data by polygons
    if all_dims.index("X") < all_dims.index("Y"):
        data_arr = data_arr.transpose(0, 2, 1)
    return data_arr


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


def summarize_within_poly(ds, poly_mask_arr, dim_encodings, varname="Gray"):
    """Summarize a single Data Variable of a xarray.DataSet within a polygon.
    Return the results as a nested dict.

    Args:
        ds (xarray.DataSet): DataSet with "Gray" as variable of
            interest
        poly (shapely.Polygon): polygon within which to summarize
        poly_mask_arr (numpy.ma.core.MaskedArra): a masked array masking the cells intersecting the polygon of interest
        varname (str): name of variable represented by ds
        roundkey (str): variable key that will fetch an integer that determines rounding precision (e.g. 1 for a single decimal place)

    Returns:
        Nested dict of results for all non-X/Y axis combinations,

    Notes:
        This default "Gray" is used because it is the default name for ingesting into Rasdaman from GeoTIFFs. Othwerwise it should be the name of a xarray.DataSet DataVariable, i.e. something in `list(ds.keys())`
    """
    da = ds[varname]
    dim_info = get_all_combinations_of_decoded_dim_values(ds, da, dim_encodings)
    data_arr = create_nparray_for_zonal_stats(da, *dim_info)
    data_arr = transpose_2d_spatial_slices_if_X_is_rows(da.dims, data_arr)
    data_arr = mask_and_crop_zonal_stats_arr(data_arr, poly_mask_arr)
    results = np.nanmean(data_arr, axis=(1, 2)).astype(float)
    return results, dim_info.dim_combos


def generic_polygon_summary_aggregation(dim_combos, results, dim_encodings, roundkey):

    aggr_results = generate_nested_dict(dim_combos)

    for map_list, result in zip(dim_combos, results):
        get_from_dict(aggr_results, map_list[:-1])[map_list[-1]] = round(
            result, dim_encodings["rounding"][roundkey]
        )
    return aggr_results


def geotiff_zonal_stats(poly, arr, transform, stat_list):
    """Wrapper to get the basic zonal statistics for a single, single-band GeoTIFF.
    Typical use case for this function is a single GeoTIFF (e.g., see the elevation end point)
    that lives on GeoServer."""
    poly_mask_arr = zonal_stats(
        poly,
        arr,
        affine=transform,
        nodata=np.nan,
        stats=stat_list,
    )
    return poly_mask_arr
