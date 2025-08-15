import asyncio

import numpy as np
from flask import Blueprint, render_template, request, current_app as app, jsonify

# local imports
from fetch_data import (
    fetch_wcs_point_data,
    describe_via_wcps,
)
from validate_request import (
    validate_latlon,
    project_latlon,
    get_coverage_encodings,
)
from postprocessing import postprocess
from csv_functions import create_csv
from config import WEST_BBOX, EAST_BBOX
from . import routes

hydrology_api = Blueprint("hydrology_api", __name__)
hydrology_coverage_id = "hydrology"


async def get_hydrology_metadata():
    """Get the coverage metadata and encodings for hydrology coverage"""
    metadata = await describe_via_wcps(hydrology_coverage_id)
    return metadata


hydrology_meta = asyncio.run(get_hydrology_metadata())
hydro_dim_encodings = get_coverage_encodings(hydrology_meta)

# default to min-max temporal range of coverage
years_lu = {
    "historical": {"min": 1950, "max": 2009},
    "projected": {"min": 2006, "max": 2100},
}

# hard-coded eras for Arctic-EDS client
eds_eras_meta = {
    "historical": [0, 5],
    "early_century": [6, 8],
    "mid_century": [9, 11],
    "late_century": [12, 14],
}


def run_fetch_hydrology_point_data(lat, lon):
    """Fetch all hydrology data for a given latitude and longitude.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and longitude for all variables
    """
    x, y = project_latlon(lat, lon, 3338)

    rasdaman_response = asyncio.run(
        fetch_wcs_point_data(
            x,
            y,
            hydrology_coverage_id,
            var_coord=None,
        )
    )

    point_pkg = dict()
    for model_coord in hydro_dim_encodings["model"].keys():
        model_name = hydro_dim_encodings["model"][model_coord]
        point_pkg[model_name] = dict()

        for scenario_coord in hydro_dim_encodings["scenario"].keys():
            scenario_name = hydro_dim_encodings["scenario"][scenario_coord]
            point_pkg[model_name][scenario_name] = dict()

            for month_coord in hydro_dim_encodings["month"].keys():
                month_name = hydro_dim_encodings["month"][month_coord]
                point_pkg[model_name][scenario_name][month_name] = dict()

                for era_coord in hydro_dim_encodings["era"].keys():
                    era_name = hydro_dim_encodings["era"][era_coord]
                    point_pkg[model_name][scenario_name][month_name][era_name] = dict()

                    for var_coord, var_name in enumerate(
                        hydrology_meta["rangeType"]["field"]
                    ):
                        point_pkg[model_name][scenario_name][month_name][era_name][
                            var_name["name"]
                        ] = rasdaman_response[model_coord][scenario_coord][month_coord][
                            era_coord
                        ].split(
                            " "
                        )[
                            var_coord
                        ]

    return point_pkg


def run_fetch_hydrology_point_data_mmm(lat, lon, summarize=None):
    """Fetch hydrology data for a
       given latitude and longitude, and summarize over eras.

    Args:
        lat (float): latitude
        lon (float): longitude
        summarize (boolean): If running the summary for EDS, set to True

    Returns:
        JSON-like dict of data at provided latitude and
        longitude for all variables
    """
    # get standard point data package
    point_pkg = run_fetch_hydrology_point_data(lat, lon)

    # repackage point data with mmm values computed across eras
    point_pkg_mmm = dict()
    for model_coord in hydro_dim_encodings["model"].keys():
        model_name = hydro_dim_encodings["model"][model_coord]
        point_pkg_mmm[model_name] = dict()
        for scenario_coord in hydro_dim_encodings["scenario"].keys():
            scenario_name = hydro_dim_encodings["scenario"][scenario_coord]
            point_pkg_mmm[model_name][scenario_name] = dict()
            for month_coord in hydro_dim_encodings["month"].keys():
                month_name = hydro_dim_encodings["month"][month_coord]
                point_pkg_mmm[model_name][scenario_name][month_name] = dict()
                for var_coord, var_name in enumerate(
                    hydrology_meta["rangeType"]["field"]
                ):
                    var_name = var_name["name"]

                    # If summarizing for ArcticEDS, we want the min-mean-max for each month for the given era. i.e. 1950-2009 for historical.
                    if summarize:
                        if var_name == "evap" or var_name == "runoff":
                            point_pkg_mmm[model_name][scenario_name][month_name][
                                var_name
                            ] = dict()

                            for era_title in eds_eras_meta.keys():
                                values = list()
                                point_pkg_mmm[model_name][scenario_name][month_name][
                                    var_name
                                ][era_title] = dict()
                                # get list representing the min/max era numbers
                                eds_eras_coords = eds_eras_meta[era_title]

                                for era_coord in range(
                                    eds_eras_coords[0], eds_eras_coords[1] + 1
                                ):
                                    era_name = hydro_dim_encodings["era"][era_coord]

                                    values.append(
                                        float(
                                            point_pkg[model_name][scenario_name][
                                                month_name
                                            ][era_name][var_name]
                                        )
                                    )

                                min_value, mean_value, max_value = (
                                    min(values),
                                    round(np.nanmean(values), 2),
                                    max(values),
                                )

                                if np.isnan(min_value):
                                    min_value = "nan"
                                if np.isnan(mean_value):
                                    mean_value = "nan"
                                if np.isnan(max_value):
                                    max_value = "nan"

                                point_pkg_mmm[model_name][scenario_name][month_name][
                                    var_name
                                ][era_title]["min"] = min_value
                                point_pkg_mmm[model_name][scenario_name][month_name][
                                    var_name
                                ][era_title]["mean"] = mean_value
                                point_pkg_mmm[model_name][scenario_name][month_name][
                                    var_name
                                ][era_title]["max"] = max_value

                    else:
                        point_pkg_mmm[model_name][scenario_name][month_name][
                            var_name
                        ] = dict()
                        # If we get here, we are going through the normal min-mean-max calculations over
                        # each and every era sequentially i.e. 1950-1959, 1960-1969, etc.
                        values = list()

                        for era_coord in hydro_dim_encodings["era"].keys():
                            era_name = hydro_dim_encodings["era"][era_coord]

                            values.append(
                                float(
                                    point_pkg[model_name][scenario_name][month_name][
                                        era_name
                                    ][var_name]
                                )
                            )

                        min_value, mean_value, max_value = (
                            min(values),
                            round(np.nanmean(values), 2),
                            max(values),
                        )

                        # reformat NaN stats to string "nan" to avoid "is not valid JSON" error

                        if np.isnan(min_value):
                            min_value = "nan"
                        if np.isnan(mean_value):
                            mean_value = "nan"
                        if np.isnan(max_value):
                            max_value = "nan"

                        point_pkg_mmm[model_name][scenario_name][month_name][var_name][
                            "min"
                        ] = min_value
                        point_pkg_mmm[model_name][scenario_name][month_name][var_name][
                            "mean"
                        ] = mean_value
                        point_pkg_mmm[model_name][scenario_name][month_name][var_name][
                            "max"
                        ] = max_value

            # Generates the seasonal totals for NCR & annual min-mean-max for ArcticEDS
            if summarize:
                # Generate seasonal totals for NCR
                seasons = {
                    "Spring": [0, 7, 8],
                    "Summer": [1, 5, 6],
                    "Fall": [9, 10, 11],
                    "Winter": [2, 3, 4],
                }
                for season, season_months in seasons.items():
                    point_pkg_mmm[model_name][scenario_name][season] = dict()
                    for era_title in eds_eras_meta.keys():
                        for var_coord, var_name in enumerate(
                            hydrology_meta["rangeType"]["field"]
                        ):
                            var_name = var_name["name"]
                            if var_name == "evap" or var_name == "runoff":
                                values = list()

                                # For each month in the season, grab the mean value for that month for the given variable and era.
                                for month_coord in season_months:
                                    month_name = hydro_dim_encodings["month"][
                                        month_coord
                                    ]
                                    values.append(
                                        float(
                                            point_pkg_mmm[model_name][scenario_name][
                                                month_name
                                            ][var_name][era_title]["mean"]
                                        )
                                    )

                                # Get the sum of the three months for the season
                                total_value = round(np.sum(values), 0)

                                # reformat NaN stats to string "nan" to avoid "is not valid JSON" error
                                if np.isnan(total_value):
                                    total_value = "nan"

                                # This is required so that we don't overwrite the previous run for an era.
                                # For example, after running the historical era, if we didn't check for the
                                # existence of the var_name for the seasonal key, it would erase the historical
                                # era and only the last era (late_century) would exist in this key-value pair.
                                if (
                                    var_name
                                    not in point_pkg_mmm[model_name][scenario_name][
                                        season
                                    ]
                                ):
                                    point_pkg_mmm[model_name][scenario_name][season][
                                        var_name
                                    ] = dict()

                                point_pkg_mmm[model_name][scenario_name][season][
                                    var_name
                                ][era_title] = dict()
                                point_pkg_mmm[model_name][scenario_name][season][
                                    var_name
                                ][era_title]["total"] = total_value

                point_pkg_mmm[model_name][scenario_name]["Annual"] = dict()
                for era_title in eds_eras_meta.keys():
                    for var_coord, var_name in enumerate(
                        hydrology_meta["rangeType"]["field"]
                    ):
                        var_name = var_name["name"]
                        if var_name == "evap" or var_name == "runoff":
                            values = list()

                            # We have to pull the data in this way to ensure we are getting the mean for each variable for each month.
                            for month_coord in hydro_dim_encodings["month"].keys():
                                month_name = hydro_dim_encodings["month"][month_coord]
                                values.append(
                                    float(
                                        point_pkg_mmm[model_name][scenario_name][
                                            month_name
                                        ][var_name][era_title]["mean"]
                                    )
                                )

                            # This will be the min, mean, and max taken from the mean monthly values to find the min-mean-max of the annual values for a given time period such as 1950-2009 for historical.
                            min_value, mean_value, max_value = (
                                min(values),
                                round(np.nanmean(values), 2),
                                max(values),
                            )

                            # reformat NaN stats to string "nan" to avoid "is not valid JSON" error
                            if np.isnan(min_value):
                                min_value = "nan"
                            if np.isnan(mean_value):
                                mean_value = "nan"
                            if np.isnan(max_value):
                                max_value = "nan"

                            # This is required so that we don't overwrite the previous run for an era.
                            # For example, after running the historical era, if we didn't check for the
                            # existence of the var_name for the Annual key, it would erase the historical
                            # era and only the last era (late_century) would exist in this key-value pair.
                            if (
                                var_name
                                not in point_pkg_mmm[model_name][scenario_name][
                                    "Annual"
                                ]
                            ):
                                point_pkg_mmm[model_name][scenario_name]["Annual"][
                                    var_name
                                ] = dict()

                            point_pkg_mmm[model_name][scenario_name]["Annual"][
                                var_name
                            ][era_title] = dict()

                            point_pkg_mmm[model_name][scenario_name]["Annual"][
                                var_name
                            ][era_title]["min"] = min_value
                            point_pkg_mmm[model_name][scenario_name]["Annual"][
                                var_name
                            ][era_title]["mean"] = mean_value
                            point_pkg_mmm[model_name][scenario_name]["Annual"][
                                var_name
                            ][era_title]["max"] = max_value

    return point_pkg_mmm


@routes.route("/hydrology/")
@routes.route("/hydrology/point/")
def hydro_about():
    return render_template("documentation/hydrology.html")


@routes.route("/hydrology/point/<lat>/<lon>")
def run_get_hydrology_point_data(lat, lon, summarize=None, preview=None):
    validation = validate_latlon(lat, lon, [hydrology_coverage_id])
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 404:
        return (
            render_template("404/no_data.html"),
            404,
        )
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    # validate request arguments if they exist, otherwise run the fetch
    if len(request.args) == 0 and (summarize is None and preview is None):
        try:
            point_pkg = run_fetch_hydrology_point_data(lat, lon)
            return postprocess(point_pkg, "hydrology")
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500

    # if args exist, check if they are allowed
    allowed_args = ["summarize", "format", "community"]
    if not all(key in allowed_args for key in request.args.keys()):
        return render_template("400/bad_request.html"), 400
    else:
        # if args exist and are allowed, return the appropriate response
        if ("summarize" in request.args) and ("format" in request.args):
            try:
                point_pkg = run_fetch_hydrology_point_data_mmm(lat, lon)
                place_id = request.args.get("community")
                if place_id:
                    return create_csv(
                        point_pkg, "hydrology_mmm", place_id=place_id, lat=lat, lon=lon
                    )
                else:
                    return create_csv(point_pkg, "hydrology_mmm", lat=lat, lon=lon)
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500

        elif "summarize" in request.args or summarize:
            try:
                return run_fetch_hydrology_point_data_mmm(lat, lon, summarize)
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500

        elif "format" in request.args or preview:
            try:
                point_pkg = run_fetch_hydrology_point_data(lat, lon)
                place_id = request.args.get("community")
                if place_id:
                    return create_csv(
                        point_pkg, "hydrology", place_id=place_id, lat=lat, lon=lon
                    )
                else:
                    return create_csv(
                        point_pkg, "hydrology", place_id=None, lat=lat, lon=lon
                    )
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500


@routes.route("/eds/hydrology/<lat>/<lon>")
@routes.route("/eds/hydrology/point/<lat>/<lon>")
def eds_hydrology_data(lat, lon):
    hydrology = dict()

    summary = run_get_hydrology_point_data(lat, lon, summarize=True)
    # Check for error response from summary response
    if isinstance(summary, tuple):
        return summary

    hydrology["summary"] = summary

    preview = run_get_hydrology_point_data(lat, lon, preview=True)
    # Check for error responses in the preview
    if isinstance(preview, tuple):
        # Returns error template that was generated for invalid request
        return preview

    hydrology_csv = preview.data.decode("utf-8")
    first = "\n".join(hydrology_csv.split("\n")[20:26]) + "\n"
    last = "\n".join(hydrology_csv.split("\n")[-6:])

    hydrology["preview"] = first + last

    return jsonify(hydrology)
