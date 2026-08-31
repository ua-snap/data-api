"""CUSP near-surface permafrost observation endpoints."""

import asyncio
import logging

from flask import render_template, request

from config import GS_BASE_URL
from csv_functions import cusp_observations_csv
from fetch_data import fetch_data
from generate_urls import generate_cusp_observations_url
from validate_request import latlon_is_numeric_and_in_geodetic_range

from . import routes

logger = logging.getLogger(__name__)

CUSP_SEARCH_HALF_BOX_DEGREES = 0.25
CUSP_RECORD_LIMIT = 100


def request_allows_only_csv_format() -> bool:
    """Return whether the request has no arguments or only ``format=csv``."""
    return not request.args or (
        set(request.args) == {"format"} and request.args.get("format") == "csv"
    )


def is_cusp_feature_collection(data: object) -> bool:
    """Return whether an upstream response has the expected GeoJSON shape."""
    return (
        isinstance(data, dict)
        and data.get("type") == "FeatureCollection"
        and isinstance(data.get("features"), list)
    )


@routes.route("/cusp/")
@routes.route("/cusp/point/")
def cusp_about():
    """Render CUSP endpoint documentation."""
    return render_template("documentation/cusp.html")


@routes.route("/cusp/point/<lat>/<lon>")
def cusp_observations_point(lat: str, lon: str):
    """Return CUSP observations within the nearby box around a point.

    The query uses a 0.5 degree by 0.5 degree EPSG:4326 bounding box centered
    on the requested longitude and latitude, and returns at most 100 records.
    Append ``?format=csv`` for a flat CSV export of the same records.
    """
    if not request_allows_only_csv_format():
        return render_template("400/bad_request.html"), 400

    if latlon_is_numeric_and_in_geodetic_range(lat, lon) != True:
        return render_template("400/bad_request.html"), 400

    try:
        observations = asyncio.run(
            fetch_data(
                [
                    generate_cusp_observations_url(
                        GS_BASE_URL,
                        lat,
                        lon,
                        half_box_degrees=CUSP_SEARCH_HALF_BOX_DEGREES,
                        record_limit=CUSP_RECORD_LIMIT,
                    )
                ]
            )
        )
    except Exception:
        logger.exception("Unable to retrieve CUSP observations from GeoServer")
        return render_template("502/upstream_unreachable.html"), 502

    if not is_cusp_feature_collection(observations):
        logger.error("GeoServer returned an unexpected CUSP response shape")
        return render_template("502/upstream_unreachable.html"), 502

    if request.args.get("format") == "csv":
        return cusp_observations_csv(
            observations,
            lat,
            lon,
            CUSP_SEARCH_HALF_BOX_DEGREES,
            CUSP_RECORD_LIMIT,
        )

    return observations
