"""A module to validate fetched data values."""
from luts import huc8_gdf

nodata_message = "No data exists at this location."


def check_for_nodata(di, varname, varval, nodata):
    """Evaluate if a specific "no data" value (e.g. -9999) is returned and replace with explanatory text."""
    if int(varval) == int(nodata):
        di.update({varname: nodata_message})


def get_huc_3338_bbox(huc_id):
    """Get the Polygon Object corresponding to the HUC ID.

    Args:
        huc_id (int): 8-digit HUC ID
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within. Inlcudes a 4-tuple (poly.bounds) of the bounding box enclosing the HUC polygon. Format is (xmin, ymin, xmax, ymax).
    """
    poly_gdf = huc8_gdf.loc[[huc_id]][["geometry"]].to_crs(3338)
    poly = poly_gdf.iloc[0]["geometry"]
    return poly
