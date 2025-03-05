from flask import (
    Blueprint,
    render_template,
)

# local imports
from . import routes

places_api = Blueprint("places_api", __name__)


@routes.route("/places/")
def places_about():
    return render_template("documentation/places.html")
