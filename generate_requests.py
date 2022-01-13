"""A module to generate specific request strings."""


def generate_wcs_getcov_str(x, y, cov_id, var_coord=None, encoding="json"):
    """Generate a WCS GetCoverage request for fetching a
    subset of a coverage over X and Y axes.

    Args:
        x (float or str): x-coordinate for point query (float), or string
        composed as "x1,x2" for bbox query, where x1 and x2 are
        lower and upper bounds of bbox
        y (float or str): y-coordinate for point query (float), or string
        composed as "y1,y2" for bbox query, where y1 and y2 are
        lower and upper bounds of bbox
        cov_id (str): Rasdaman coverage ID
        var_coord (int): coordinate value corresponding to variable name to query, default=None will include all variables
        encoding (str): currently supports either "json" or "netcdf"
        for point or bbox queries, respectively
    Returns:
        wcs_getcov_str (str): WCS GetCoverage Request to append to a query URL
    """

    # if var_coord is specified, subsetting using the specific variable
    if var_coord is not None:
        var_subset_str = f"&SUBSET=varname({var_coord})"
    else:
        var_subset_str = ""
    wcs_getcov_str = (
        f"GetCoverage&COVERAGEID={cov_id}"
        f"&SUBSET=X({x})&SUBSET=Y({y}){var_subset_str}"
        f"&FORMAT=application/{encoding}"
    )
    return wcs_getcov_str


def generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id, var_coord=None):
    """Generate a WCS GetCoverage request for netCDF data over an area.

    Args:
        bbox_bounds (tuple): 4-tuple of bounding polygon extent (xmin, ymin, xmax, ymax)
        cov_id (str): Rasdaman coverage ID
        var_coord (int): coordinate value corresponding to variable name to query, default=None will include all variables
    Returns:
        netcdf_wcs_getcov_str (str): WCS GetCoverage Request to append to a query URL
    """

    (x1, y1, x2, y2) = bbox_bounds
    x = f"{x1},{x2}"
    y = f"{y1},{y2}"
    netcdf_wcs_getcov_str = generate_wcs_getcov_str(x, y, cov_id, var_coord, "netcdf")
    return netcdf_wcs_getcov_str
