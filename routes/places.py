from flask import (
    Blueprint,
    render_template,
)
import json

# local imports
from validate_request import validate_var_id
from postprocessing import recursive_rounding
from fetch_data import get_poly_3338_bbox
from . import routes

places_api = Blueprint("places_api", __name__)


@routes.route("/places/")
@routes.route("/places/abstract/")
# @routes.route("/boundary/area/")
def places_about():
    return render_template("documentation/places.html")


# @routes.route("/boundary/area/<var_id>")
# def run_fetch_area_poly(var_id):
#     """Run async requesting for a climate division polygon.

#     Args:
#         cd_id (str): ID for polygon, e.g. `CD2`

#     Returns:
#         GeoJSON of the polygon

#     example: http://localhost:5000/boundary/climatedivisision/CD2
#     """

#     poly_type = validate_var_id(var_id)

#     # This is only ever true when it is returning an error template
#     if type(poly_type) is tuple:
#         return poly_type

#     try:
#         poly = get_poly_3338_bbox(var_id, 4326)
#     except:
#         return render_template("422/invalid_area.html"), 422
#     poly_geojson = poly.to_json()
#     poly_geojson = json.loads(poly_geojson)["features"][0]
#     return recursive_rounding(poly_geojson.keys(), poly_geojson.values())
