import asyncio
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)

# local imports
from . import routes
from luts import huc8_gdf


huc_api = Blueprint("huc_api", __name__)


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
    poly = huc8_gdf.loc[[huc8_id]]
    poly_geojson = poly.to_json()

    return poly_geojson
