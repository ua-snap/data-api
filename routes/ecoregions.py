import asyncio
import logging
import time
from flask import (
    Blueprint,
    render_template,
)

# local imports
from fetch_data import fetch_geoserver_data
from validate_request import validate_latlon
from postprocessing import postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from . import routes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ecoregions_api = Blueprint("ecoregions_api", __name__)

wms_targets = []
wfs_targets = {"ak_level3_ecoregions": "ECOREGION"}


def package_epaecoreg(eco_resp):
    """Package ecoregions data in dict"""
    title = "EPA Level III Ecoregions of Alaska"
    if eco_resp[0]["features"] == []:
        return None
    di = {}
    ecoreg = eco_resp[0]["features"][0]["properties"]["ECOREGION"]
    di.update({"title": title, "name": ecoreg})
    return di


@routes.route("/ecoregions/")
@routes.route("/ecoregions/abstract/")
@routes.route("/ecoregions/point/")
def phys_about():
    start_time = time.time()
    logger.info("Accessed /ecoregions/ documentation endpoint")
    try:
        response = render_template("documentation/ecoregions.html")
        logger.info(
            "Successfully returned /ecoregions/ documentation "
            f"(elapsed: {time.time() - start_time:.3f}s)"
        )
        return response
    except Exception as exc:
        logger.error(
            f"Server error in /ecoregions/: {exc} "
            f"(elapsed: {time.time() - start_time:.3f}s)"
        )
        return render_template("500/server_error.html"), 500


@routes.route("/ecoregions/point/<lat>/<lon>")
def run_fetch_ecoregions(lat, lon):
    start_time = time.time()
    logger.info(
        f"Accessed /ecoregions/point/{{lat}}/{{lon}} with lat={{lat}}, lon={{lon}}"
    )
    validation = validate_latlon(lat, lon)
    if validation == 400:
        logger.warning(
            f"Bad request for /ecoregions/point/{{lat}}/{{lon}}: 400 "
            f"(elapsed: {time.time() - start_time:.3f}s)"
        )
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        logger.warning(
            f"Invalid lat/lon for /ecoregions/point/{{lat}}/{{lon}}: 422 "
            f"(elapsed: {time.time() - start_time:.3f}s)"
        )
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    try:
        results = asyncio.run(
            fetch_geoserver_data(
                GS_BASE_URL, "physiography", wms_targets, wfs_targets, lat, lon
            )
        )
        physio = package_epaecoreg(results)
        logger.info(
            f"Successfully returned ecoregion for /ecoregions/point/{{lat}}/{{lon}} "
            f"(elapsed: {time.time() - start_time:.3f}s)"
        )
        return postprocess(physio, "ecoregions")
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            logger.warning(
                f"No data for /ecoregions/point/{{lat}}/{{lon}}: 404 "
                f"(elapsed: {time.time() - start_time:.3f}s)"
            )
            return render_template("404/no_data.html"), 404
        logger.error(
            f"Server error in /ecoregions/point/{{lat}}/{{lon}}: {exc} "
            f"(elapsed: {time.time() - start_time:.3f}s)"
        )
        return render_template("500/server_error.html"), 500
