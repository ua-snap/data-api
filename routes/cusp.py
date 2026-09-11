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
QUALITY_FLAG_DEFINITIONS = {
    "LB": {
        "category": "censoring",
        "description": "Permafrost was not reached within the reported probe profile or observation limit.",
    },
    "UB": {
        "category": "censoring",
        "description": "Permafrost was confirmed but exact thaw or permafrost depth is only bounded.",
    },
    "OA": {
        "category": "censoring",
        "description": "Observation limit was assigned from protocol, source interpretation, or processor convention rather than a row-specific measured limit.",
    },
    "PB": {
        "category": "censoring",
        "description": "Permafrost absence is interpreted to the bottom of an observed soil or profile record.",
    },
    "DA": {
        "category": "temporal",
        "description": "Date was assigned from a reported year, a campaign midpoint, or a representative thaw-season convention.",
    },
    "DS": {
        "category": "temporal",
        "description": "Source flags the observation date as approximate, range-based, or otherwise imprecise.",
    },
    "CS": {
        "category": "spatial",
        "description": "Site or event coordinate is repeated for observations that are more spatially granular than the coordinate.",
    },
    "CI": {
        "category": "spatial",
        "description": "Coordinates were assigned from lookup tables or interpolated along transects.",
    },
    "CF": {
        "category": "spatial",
        "description": "Source flags coordinate uncertainty, likely coordinate problems, or georeferenced coordinates.",
    },
    "DR": {
        "category": "measurement",
        "description": "Depth is measured from a source reference surface that differs from the usual ground or moss surface and was not converted.",
    },
    "SS": {
        "category": "aggregation",
        "description": "Canonical observation value is a mean annual site summary or other source or processor summary statistic.",
    },
    "TI": {
        "category": "method",
        "description": "Permafrost state or depth was inferred from temperature profile logic.",
    },
    "GI": {
        "category": "method",
        "description": "Depth was inferred from GPR, ERT, or other geophysical interpretation rather than direct probing or coring.",
    },
    "VI": {
        "category": "method",
        "description": "Permafrost presence or absence was assigned from visual field or mapped-feature interpretation rather than a subsurface instrument measurement; coordinates may be representative samples from mapped areas.",
    },
    "ME": {
        "category": "derivation",
        "description": "Source or CUSP processor uses modeled, estimated, extrapolated, or reconstructed depth.",
    },
    "FE": {
        "category": "derivation",
        "description": "Value or coordinate was extracted, georeferenced, or digitized from a figure.",
    },
    "PA": {
        "category": "interpretation",
        "description": "Permafrost presence state was assigned from context, a threshold, aggregation, or proxy coding rather than an explicit row-level detection or state.",
    },
    "MU": {
        "category": "method",
        "description": "Observation method is unknown or mapped approximately into the CUSP method vocabulary.",
    },
    "RC": {
        "category": "source_cleanup",
        "description": "Units, sentinels, coordinate signs, or source codes were recoded to make the row usable.",
    },
    "VA": {
        "category": "source_cleanup",
        "description": "Reported depth was bounded, approximate, or ranged, and CUSP retained one representative numeric value.",
    },
    "DO": {
        "category": "duplication",
        "description": "Known potential overlap with another source remains, or source-specific deduplication affected interpretation.",
    },
    "RO": {
        "category": "field_condition",
        "description": "Field notes indicate rock, gravel, obstruction, bottoming out, road, water, or similar ambiguity.",
    },
}


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


def normalize_cusp_observation_dates(
    feature_collection: dict[str, object],
) -> None:
    """Normalize GeoServer CUSP dates to ``YYYY-MM-DD`` strings in place."""
    for feature in feature_collection["features"]:
        if not isinstance(feature, dict):
            continue

        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue

        observation_date = properties.get("observation_date")
        if isinstance(observation_date, str):
            properties["observation_date"] = observation_date.removesuffix("Z")


def add_cusp_quality_flag_definitions(
    feature_collection: dict[str, object],
) -> None:
    """Add CUSP's quality-flag lookup table to an observation response."""
    feature_collection["quality_flag_definitions"] = QUALITY_FLAG_DEFINITIONS


@routes.route("/cusp/")
@routes.route("/cusp/point/")
def cusp_about():
    """Render CUSP endpoint documentation."""
    return render_template("documentation/cusp.html")


@routes.route("/cusp/point/<lat>/<lon>")
def cusp_observations_point(lat: str, lon: str):
    """Return CUSP observations within the nearby box around a point.

    The query uses a 0.5 degree by 0.5 degree EPSG:4326 bounding box centered
    on the requested longitude and latitude, and returns every matching record.
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

    normalize_cusp_observation_dates(observations)
    add_cusp_quality_flag_definitions(observations)

    if request.args.get("format") == "csv":
        return cusp_observations_csv(
            observations,
            lat,
            lon,
            CUSP_SEARCH_HALF_BOX_DEGREES,
            None,
        )

    return observations
