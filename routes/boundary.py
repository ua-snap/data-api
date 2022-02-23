from flask import (
    Blueprint,
    render_template,
)
import json

# local imports
from luts import huc_gdf, akpa_gdf, akco_gdf, aketh_gdf, akclim_gdf, akfire_gdf
from validate_request import validate_polyid, validate_huc
from validate_data import recursive_rounding
from . import routes

boundary_api = Blueprint("boundary_api", __name__)


@routes.route("/boundary/")
@routes.route("/boundary/abstract/")
def boundary_about():
    return render_template("boundary/abstract.html")


@routes.route("/boundary/climatedivision/")
@routes.route("/boundary/climatedivision/abstract/")
def climatedivision_about():
    return render_template("boundary/climatedivision.html")


@routes.route("/boundary/corporation/")
@routes.route("/boundary/corporation/abstract/")
def corporation_about():
    return render_template("boundary/corporation.html")


@routes.route("/boundary/ethnolinguistic/")
@routes.route("/boundary/ethnolinguistic/abstract/")
def ethnolinguistic_about():
    return render_template("boundary/ethnolinguistic.html")


@routes.route("/boundary/firemanagement/")
@routes.route("/boundary/firemanagement/abstract/")
def firemanagement_about():
    return render_template("boundary/firemanagement.html")


@routes.route("/boundary/protectedarea/")
@routes.route("/boundary/protectedarea/abstract/")
def protectedarea_about():
    return render_template("boundary/protectedarea.html")


@routes.route("/boundary/huc/")
@routes.route("/boundary/huc/abstract/")
def huc_about():
    return render_template("boundary/huc.html")


@routes.route("/boundary/climatedivision/<cd_id>")
def run_fetch_climatedivision_poly(cd_id):
    """Run async requesting for a climate division polygon.

    Args:
        cd_id (str): ID for polygon, e.g. `CD2`

    Returns:
        GeoJSON of the polygon

    example: http://localhost:5000/boundary/climatedivisision/CD2
    """
    validation = validate_polyid(cd_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akclim_gdf.loc[[cd_id]].to_crs(4326)
    except:
        return render_template("422/invalid_climatedivision.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())


@routes.route("/boundary/corporation/<co_id>")
def run_fetch_corporation_poly(co_id):
    """Run async requesting for a corporation polygon.

    Args:
        co_id (str): ID for polygon, e.g. `NC3`

    Returns:
        GeoJSON of the polygon

    example: http://localhost:5000/boundary/corporation/NC3
    """
    validation = validate_polyid(co_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akco_gdf.loc[[co_id]].to_crs(4326)
    except:
        return render_template("422/invalid_corporation.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())


@routes.route("/boundary/ethnolinguistic/<el_id>")
def run_fetch_ethnolinguistic_poly(el_id):
    """Run async requesting for a ethnolinguistic polygon.

    Args:
        el_id (str): ID for polygon, e.g. `EL4`

    Returns:
        GeoJSON of the polygon

    example: http://localhost:5000/boundary/ethnolinguistic/EL4
    """
    validation = validate_polyid(el_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = aketh_gdf.loc[[el_id]].to_crs(4326)
    except:
        return render_template("422/invalid_ethnolinguistic.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())


@routes.route("/boundary/firemanagement/<fire_id>")
def run_fetch_firemanagement_poly(fire_id):
    """Run async requesting for a firemanagement polygon.

    Args:
        el_id (str): ID for polygon, e.g. `FIRE7`

    Returns:
        GeoJSON of the polygon

    example: http://localhost:5000/boundary/firemanagement/FIRE7
    """
    validation = validate_polyid(fire_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akfire_gdf.loc[[fire_id]].to_crs(4326)
    except:
        return render_template("422/invalid_firemanagement.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())


@routes.route("/boundary/huc/<huc_id>")
def run_fetch_huc_poly(huc_id):
    """Run the async requesting for a HUC polygon and return the GeoJSON.

    Args:
        huc_id (int): THUC-8 or HUC-12 code.

    Returns:
        GeoJSON of the HUC polygon

    Notes:
        example: http://localhost:5000/boundary/huc/19070506
    """
    validation = validate_huc(huc_id)
    if validation is not True:
        return validation

    try:
        poly = huc_gdf.loc[[huc_id]].to_crs(4326)
    except:
        return render_template("422/invalid_huc.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())


@routes.route("/boundary/protectedarea/<akpa_id>")
def run_fetch_akprotectedarea_poly(akpa_id):
    """Run the async requesting for a protected area polygon and return the GeoJSON.

    Args:
        pa_id (str): ID for polygon, e.g. `NPS12` or `FWS7`

    Returns:
        GeoJSON of the protected area polygon

    Notes:
        example: http://localhost:5000/boundary/protectedarea/NPS12
    """
    validation = validate_polyid(akpa_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akpa_gdf.loc[[akpa_id]].to_crs(4326)
    except:
        return render_template("422/invalid_protected_area.html"), 422
    poly_geojson = poly.to_json()
    poly_geojson = json.loads(poly_geojson)["features"][0]
    return recursive_rounding(poly_geojson.keys(), poly_geojson.values())
