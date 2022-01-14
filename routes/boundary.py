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
from luts import huc8_gdf, akpa_gdf

boundary_api = Blueprint("boundary_api", __name__)


@routes.route("/boundary/")
@routes.route("/boundary/abstract/")
def boundary_about():
    return render_template("boundary/abstract.html")


@routes.route("/boundary/protectedarea/")
@routes.route("/boundary/protectedarea/abstract/")
def protectedarea_about():
    return render_template("boundary/protectedarea.html")


@routes.route("/boundary/huc/")
@routes.route("/boundary/huc/abstract/")
@routes.route("/boundary/watershed/")
@routes.route("/boundary/watershed/abstract/")
def huc_about():
    return render_template("boundary/huc/abstract.html")


@routes.route("/boundary/watershed/huc8/")
@routes.route("/boundary/huc/huc8/")
def huc8_about():
    return render_template("boundary/huc/huc8.html")


@routes.route("/boundary/huc/huc8/<huc8_id>")
@routes.route("/boundary/watershed/huc8/<huc8_id>")
def run_fetch_huc_poly(huc8_id):
    """Run the async IEM data requesting for a single point
    and return data as json

    Args:
        huc8_id (int): HUC-8 ID

    Returns:
        GeoJSON of the HUC-8 polygon

    Notes:
        example: http://localhost:5000/boundary/huc/huc8/19070506
    """
    poly = huc8_gdf.loc[[huc8_id]]
    poly_geojson = poly.to_json()
    return poly_geojson


@routes.route("/boundary/protectedarea/<akpa_id>")
def run_fetch_akprotectedarea_poly(akpa_id):
    """Run the async IEM data requesting for a single point
    and return data as json

    Args:
        pa_id (str): ID for polygon, e.g. `NPS12` or `FWS7`

    Returns:
        GeoJSON of the protected area polygon

    Notes:
        example: http://localhost:5000/boundary/protectedarea/NPS12
    """
    poly = akpa_gdf.loc[[akpa_id]]
    poly_geojson = poly.to_json()
    return poly_geojson
