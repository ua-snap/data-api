from flask import (
    Blueprint,
    render_template,
)
import json

# local imports
from luts import type_di
from validate_request import validate_var_id
from validate_data import recursive_rounding
from . import routes

boundary_api = Blueprint("boundary_api", __name__)


@routes.route("/boundary/")
@routes.route("/boundary/abstract/")
def boundary_about():
    return render_template("boundary/abstract.html")


@routes.route("/boundary/area/")
@routes.route("/boundary/area/abstract/")
def area_about():
    return render_template("boundary/area.html")


@routes.route("/boundary/area/<var_id>")
def run_fetch_area_poly(var_id):
    """Run async requesting for a climate division polygon.

    Args:
        cd_id (str): ID for polygon, e.g. `CD2`

    Returns:
        GeoJSON of the polygon

    example: http://localhost:5000/boundary/climatedivisision/CD2
    """

    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        return poly_type

    try:
        poly = type_di[poly_type].loc[[var_id]].to_crs(4326)
    except:
        return render_template("422/invalid_area.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())
