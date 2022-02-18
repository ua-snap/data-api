from flask import (
    Blueprint,
    render_template,
)

# local imports
from luts import huc8_gdf, akpa_gdf, akco_gdf, aketh_gdf, akclim_gdf, akfire_gdf
from validate_request import validate_huc8, validate_akpa
from . import routes

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
def huc_about():
    return render_template("boundary/huc/abstract.html")


@routes.route("/boundary/huc/huc8/")
def huc8_about():
    return render_template("boundary/huc/huc8.html")


@routes.route("/boundary/huc/huc8/<huc8_id>")
def run_fetch_huc_poly(huc8_id):
    """Run the async requesting for a HUC polygon and return the GeoJSON.

    Args:
        huc8_id (int): HUC-8 ID

    Returns:
        GeoJSON of the HUC-8 polygon

    Notes:
        example: http://localhost:5000/boundary/huc/huc8/19070506
    """
    validation = validate_huc8(huc8_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = huc8_gdf.loc[[huc8_id]].to_crs(4326)
    except:
        return render_template("422/invalid_huc.html"), 422
    poly_geojson = poly.to_json()
    return poly_geojson


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
    validation = validate_akpa(akpa_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akpa_gdf.loc[[akpa_id]].to_crs(4326)
    except:
        return render_template("422/invalid_protected_area.html"), 422
    poly_geojson = poly.to_json()
    return poly_geojson


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
    validation = validate_akpa(akpa_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akpa_gdf.loc[[akpa_id]].to_crs(4326)
    except:
        return render_template("422/invalid_protected_area.html"), 422
    poly_geojson = poly.to_json()
    return poly_geojson


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
    validation = validate_akpa(akpa_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akpa_gdf.loc[[akpa_id]].to_crs(4326)
    except:
        return render_template("422/invalid_protected_area.html"), 422
    poly_geojson = poly.to_json()
    return poly_geojson


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
    validation = validate_akpa(akpa_id)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    try:
        poly = akpa_gdf.loc[[akpa_id]].to_crs(4326)
    except:
        return render_template("422/invalid_protected_area.html"), 422
    poly_geojson = poly.to_json()
    return poly_geojson
