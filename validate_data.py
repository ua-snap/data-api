"""A module to validate fetched data values."""
from luts import huc8_gdf, akpa_gdf

nodata_message = "No data exists at this location."


def check_for_nodata(di, varname, varval, nodata):
    """Evaluate if a specific "no data" value (e.g. -9999) is returned and replace with explanatory text."""
    if int(varval) == int(nodata):
        di.update({varname: nodata_message})


def get_poly_3338_bbox(gdf, poly_id):
    """Get the Polygon Object corresponding to the the ID for a GeoDataFrame

    Args:
        gdf (geopandas.GeoDataFrame object): polygon features
        polyid (str or int): ID of polygon e.g. "FWS12", or a HUC code (int).
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within. Inlcudes a 4-tuple (poly.bounds) of the bounding box enclosing the polygon. Format is (xmin, ymin, xmax, ymax).
    """
    poly_gdf = gdf.loc[[poly_id]][["geometry"]].to_crs(3338)
    poly = poly_gdf.iloc[0]["geometry"]
    return poly


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


def get_akpa_3338_bbox(akpa_id):
    """Get the Polygon Object corresponding to the protected area ID.

    Args:
        akpa_id (str): ID of protected area, e.g. "FWS12"
    Returns:
        poly (shapely.Polygon): Polygon object used to summarize data within. Inlcudes a 4-tuple (poly.bounds) of the bounding box enclosing the polygon. Format is (xmin, ymin, xmax, ymax).
    """
    poly_gdf = akpa_gdf.loc[[akpa_id]][["geometry"]].to_crs(3338)
    poly = poly_gdf.iloc[0]["geometry"]
    return poly
