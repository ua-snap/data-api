import asyncio
import geopandas as gpd
from aiohttp import ClientSession
from flask import abort, Blueprint, render_template, current_app as app
from . import routes

iem_api = Blueprint("iem_api", __name__)

huc_gdf = gpd.read_file("data/shapefiles/hydrologic_units\wbdhu8_a_ak.shp").set_index(
    "huc8"
)


@routes.route("/huc/")
@routes.route("/huc/abstract/")
def huc_about():
    return render_template("huc/abstract.html")


@routes.route("/huc/huc8")
def huc8_about():
    return render_template("huc/huc8.html")


@routes.route("/huc/huc8/<huc8_id>")
def run_fetch_huc_poly(huc8_id):
    """Run the async IEM data requesting for a single point
    and return data as json

    Args:
        huc8_id (int): HUC-8 ID

    Returns:
        GeoJSON of the HUC-8 polygon

    Notes:
        example request: http://localhost:5000/huc/huc8/19070506
    """
    poly = huc_gdf.loc[[huc8_id]]
    poly_geojson = poly.to_json()

    return poly_geojson
