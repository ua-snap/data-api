from flask import (
    Blueprint,
    render_template,
)
import json
import logging
import time

# local imports
from validate_request import validate_var_id
from postprocessing import recursive_rounding
from fetch_data import get_poly
from . import routes

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

boundary_api = Blueprint("boundary_api", __name__)


@routes.route("/boundary/")
@routes.route("/boundary/abstract/")
@routes.route("/boundary/area/")
def boundary_about():
    start_time = time.time()
    logger.info(f"Boundary about endpoint accessed: {render_template.__name__}")
    response = render_template("documentation/boundary.html")
    elapsed = time.time() - start_time
    logger.info(f"Boundary about endpoint response in {elapsed:.3f} seconds")
    return response


@routes.route("/boundary/area/<var_id>")
def run_fetch_area_poly(var_id):
    """Run async requesting for a climate division polygon.

    Args:
        cd_id (str): ID for polygon, e.g. `CD2`

    Returns:
        GeoJSON of the polygon

    example: http://localhost:5000/boundary/climatedivisision/CD2
    """

    start_time = time.time()
    logger.info(f"Boundary area endpoint accessed: var_id={var_id}")
    poly_type = validate_var_id(var_id)

    # This is only ever true when it is returning an error template
    if type(poly_type) is tuple:
        elapsed = time.time() - start_time
        logger.warning(
            f"Invalid var_id for boundary area: var_id={var_id} (in {elapsed:.3f} seconds)"
        )
        return poly_type

    try:
        poly = get_poly(var_id, 4326)
    except Exception as exc:
        elapsed = time.time() - start_time
        logger.error(
            f"Error in boundary area fetch: var_id={var_id}, error={exc} (in {elapsed:.3f} seconds)"
        )
        return render_template("422/invalid_area.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    elapsed = time.time() - start_time
    logger.info(
        f"Boundary area fetch returned GeoJSON: var_id={var_id} (in {elapsed:.3f} seconds)"
    )
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())
