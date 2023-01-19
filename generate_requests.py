"""A module to generate specific request strings."""
from collections import namedtuple
from urllib.parse import quote


def get_wcs_xy_str_from_bbox_bounds(poly):
    """Helper function to get WCS-formatted XY strings from Polygon.
    Args:
        poly (object): shapely.Polygon with 4-tuple bounding box (xmin, ymin, xmax, ymax).
    Returns:
        xy (tuple): 2-tuple of coordinate strings formatted for WCS requests.
            Instantiated as a namedtuple for access convenience and s
            elf-documentation when used in service endpoints.
    """
    WCS_xy = namedtuple("WCS_xy", "xstr ystr")
    (x1, y1, x2, y2) = poly.bounds
    x = f"{x1},{x2}"
    y = f"{y1},{y2}"
    xy = WCS_xy(x, y)
    return xy


def generate_mmm_wcs_getcov_str(x, y, cov_id, model, scenario, encoding="json"):
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
        model (int): Model number defined in Rasdaman coverage
            - 0: CRU-TS 4.0
            - 2: GFDL-CM3
            - 3: GISS-E2-R
            - 4: IPSL-CM5A-LR
            - 5: MRI-CGCM3
            - 6: NCAR-CCSM4
        scenario (int): Scenario number defined in Rasdaman coverage
            - 0: historical
            - 1: RCP 4.5
            - 2: RCP 6.0
            - 3: RCP 8.5
    Returns:
        wcs_getcov_str (str): WCS GetCoverage Request to append to a query URL
    """

    var_subset_str = f"&SUBSET=model({model})&SUBSET=scenario({scenario})"

    wcs_getcov_str = (
        f"GetCoverage&COVERAGEID={cov_id}"
        f"&SUBSET=X({x})&SUBSET=Y({y}){var_subset_str}"
        f"&FORMAT=application/{encoding}"
    )
    return wcs_getcov_str


def generate_wcs_getcov_str(
    x, y, cov_id, var_coord=None, time_slice=None, encoding="json"
):
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
        time_slice (tuple): two-tuple of the time axis name (e.g., `year`) and the ISO time-string used to slice the data
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
    if time_slice is not None:
        time_axis, slice_string = time_slice
        time_slice_str = f"&SUBSET={time_axis}({slice_string})"
    else:
        time_slice_str = ""
    wcs_getcov_str = (
        f"GetCoverage&COVERAGEID={cov_id}"
        f"&SUBSET=X({x})&SUBSET=Y({y}){var_subset_str}{time_slice_str}"
        f"&FORMAT=application/{encoding}"
    )
    return wcs_getcov_str


def generate_netcdf_wcs_getcov_str(bbox_bounds, cov_id, var_coord=None):
    """Generate a WCS GetCoverage request for netCDF data over an area.

    Args:
        bbox_bounds (tuple): 4-tuple of bounding polygon extent (xmin, ymin, xmax, ymax)
        cov_id (str): Rasdaman coverage ID
        var_coord (int): coordinate value corresponding to variable name to query,
            default=None will include all variables
    Returns:
        netcdf_wcs_getcov_str (str): WCS GetCoverage Request to append to a query URL
    """

    (x1, y1, x2, y2) = bbox_bounds
    x = f"{x1},{x2}"
    y = f"{y1},{y2}"
    netcdf_wcs_getcov_str = generate_wcs_getcov_str(x, y, cov_id, var_coord, "netcdf")
    return netcdf_wcs_getcov_str


def generate_average_wcps_str(
    x, y, cov_id, axis_name, axis_coords, slice_di=None, encoding="json"
):
    """Generates a WCPS request string for computing
    the average over specified axes.

    Args:
        x (float or str): x-coordinate for point query, or string
            composed as "x1:x2" for bbox query, where x1 and x2 are
            lower and upper bounds of bbox
        y (float or str): y-coordinate for point query, or string
            composed as "y1:y2" for bbox query, where y1 and y2 are
            lower and upper bounds of bbox
        cov_id (str): Rasdaman coverage ID
        axis_name (str): name of the axis to take average over
        axis_coords (tuple): 2-tuple of coordinates to average over
            of the form (start, stop)
        slice_di (dict): dict with axis names for keys and
            coordinates for the values to be used in further
            subsetting in WCPS query. E.g., {"varname": 0}
        encoding (str): currently supports either "json" or "netcdf"
            for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """
    if slice_di is not None:
        subset_str = "".join([f",{k}({v})" for k, v in slice_di.items()])
    else:
        subset_str = ""
    c1, c2 = axis_coords
    summary_str = f"{axis_name}({c1}:{c2})"
    n = len(range(c1, c2 + 1))
    wcps_request_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({cov_id}) "
            f"let $a := (condense + over $t {summary_str} "
            f"using $c[{axis_name}($t),X({x}),Y({y}){subset_str}] ) / {n} "
            f'return encode( $a , "application/{encoding}")'
        )
    )
    return wcps_request_str


def generate_netcdf_average_wcps_str(bbox_bounds, generate_average_wcps_str_kwargs):
    """Generate a WCPS GetCoverage request for netCDF data over an area.

    Args:
        bbox_bounds (tuple): 4-tuple of bounding polygon extent (xmin, ymin, xmax, ymax)
        generate_average_wcps_str_kwargs (dict): Args to pass on to generate_average_wcps_str_kwargs

    Returns:
        netcdf_avg_wcps_str (str): WCPS GetCoverage Request to append to a query URL
    """
    (x1, y1, x2, y2) = bbox_bounds
    x = f"{x1}:{x2}"
    y = f"{y1}:{y2}"
    netcdf_avg_wcps_str = generate_average_wcps_str(
        x,
        y,
        **generate_average_wcps_str_kwargs,
    )
    return netcdf_avg_wcps_str
