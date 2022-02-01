from flask import (
    Blueprint,
    render_template
)

# local imports
from . import routes
from pull_aoi_data import update_data

dataupdate_api = Blueprint("dataupdate_api", __name__)

@routes.route("/update/")
def update_json_data():
    update_data()
    return render_template("dataupdate/updated.html")


