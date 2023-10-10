import asyncio
import numpy as np
from flask import (
    Blueprint,
    render_template,
    request,
    current_app as app,
    jsonify
)

# local imports
from fetch_data import fetch_wcs_point_data
from validate_request import (
    validate_latlon,
    project_latlon,
)
from validate_data import *
from postprocessing import postprocess
from csv_functions import create_csv
from config import WEST_BBOX, EAST_BBOX
from . import routes

hydrology_api = Blueprint("hydrology_api", __name__)
hydrology_coverage_id = "hydrology"

dim_encodings = {
    "varnames": {
        0: "evap",
        1: "glacier_melt",
        2: "iwe",
        3: "pcp",
        4: "runoff",
        5: "sm1",
        6: "sm2",
        7: "sm3",
        8: "snow_melt",
        9: "swe",
        10: "tmax",
        11: "tmin",
    },
    "models": {
        0: "ACCESS1-3",
        1: "CCSM4",
        2: "CSIRO-Mk3-6-0",
        3: "CanESM2",
        4: "GFDL-ESM2M",
        5: "HadGEM2-ES",
        6: "MIROC5",
        7: "MPI-ESM-MR",
        8: "MRI-CGCM3",
        9: "inmcm4",
    },
    "scenarios": {
        0: "rcp45",
        1: "rcp85",
    },
    "months": {
        0: "apr",
        1: "aug",
        2: "dec",
        3: "feb",
        4: "jan",
        5: "jul",
        6: "jun",
        7: "mar",
        8: "may",
        9: "nov",
        10: "oct",
        11: "sep",
    },
    "eras": {
        0: "1950-1959",
        1: "1960-1969",
        2: "1970-1979",
        3: "1980-1989",
        4: "1990-1999",
        5: "2000-2009",
        6: "2010-2019",
        7: "2020-2029",
        8: "2030-2039",
        9: "2040-2049",
        10: "2050-2059",
        11: "2060-2069",
        12: "2070-2079",
        13: "2080-2089",
        14: "2090-2099",
    },
    "eds_eras": {
        "historical": [0, 5],
        "early_century": [6, 8],
        "mid_century": [9, 11],
        "late_century": [12, 14]
    },
}


def run_fetch_hydrology_point_data(lat, lon):
    """Fetch all hydrology data for a
       given latitude and longitude.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of data at provided latitude and
        longitude for all variables
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

    # package point data with decoded coord values (names)
    point_pkg = dict()
    for model_coord in dim_encodings["models"].keys():
        model_name = dim_encodings["models"][model_coord]
        point_pkg[model_name] = dict()
        for scenario_coord in dim_encodings["scenarios"].keys():
            scenario_name = dim_encodings["scenarios"][scenario_coord]
            point_pkg[model_name][scenario_name] = dict()
            for month_coord in dim_encodings["months"].keys():
                month_name = dim_encodings["months"][month_coord]
                point_pkg[model_name][scenario_name][month_name] = dict()
                for era_coord in dim_encodings["eras"].keys():
                    era_name = dim_encodings["eras"][era_coord]
                    point_pkg[model_name][scenario_name][month_name][era_name] = dict()
                    for var_coord in dim_encodings["varnames"].keys():
                        var_name = dim_encodings["varnames"][var_coord]
                        point_pkg[model_name][scenario_name][month_name][era_name][
                            var_name
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
    for model_coord in dim_encodings["models"].keys():
        model_name = dim_encodings["models"][model_coord]
        point_pkg_mmm[model_name] = dict()
        for scenario_coord in dim_encodings["scenarios"].keys():
            scenario_name = dim_encodings["scenarios"][scenario_coord]
            point_pkg_mmm[model_name][scenario_name] = dict()
            for month_coord in dim_encodings["months"].keys():
                month_name = dim_encodings["months"][month_coord]
                point_pkg_mmm[model_name][scenario_name][month_name] = dict()
                for var_coord in dim_encodings["varnames"].keys():
                    var_name = dim_encodings["varnames"][var_coord]
                    point_pkg_mmm[model_name][scenario_name][month_name][
                        var_name
                    ] = dict()

                    # If summarizing for ArcticEDS, we want to get the min-mean-max for each
                    # month for the given era i.e. 1950-2009 for historical period.
                    if summarize:
                        for era_title in dim_encodings["eds_eras"].keys():
                            values = list()
                            point_pkg_mmm[model_name][scenario_name][month_name][var_name][era_title] = dict()

                            # Pull the list from dim_encodings representing the min and max
                            # era numbers for this era
                            eds_eras = dim_encodings["eds_eras"][era_title]
                            for era_coord in range(eds_eras[0], eds_eras[1] + 1):
                                era_name = dim_encodings["eras"][era_coord]

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

                            if np.isnan(min_value):
                                min_value = "nan"
                            if np.isnan(mean_value):
                                mean_value = "nan"
                            if np.isnan(max_value):
                                max_value = "nan"

                            point_pkg_mmm[model_name][scenario_name][month_name][var_name][era_title][
                                "min"
                            ] = min_value
                            point_pkg_mmm[model_name][scenario_name][month_name][var_name][era_title][
                                "mean"
                            ] = mean_value
                            point_pkg_mmm[model_name][scenario_name][month_name][var_name][era_title][
                                "max"
                            ] = max_value

                    else:
                        # If we get here, we are going through the normal min-mean-max calculations over
                        # each and every era sequentially i.e. 1950-1959, 1960-1969, etc.
                        values = list()

                        for era_coord in dim_encodings["eras"].keys():
                            era_name = dim_encodings["eras"][era_coord]

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

            # Generates the annual min-mean-max for ArcticEDS
            if summarize:
                point_pkg_mmm[model_name][scenario_name]['Annual'] = dict()
                for era_title in dim_encodings["eds_eras"].keys():
                    for var_coord in dim_encodings["varnames"].keys():
                        var_name = dim_encodings["varnames"][var_coord]
                        values = list()

                        # We have to pull the data in this way to ensure we are getting
                        # the mean for each variable for each month.
                        for month_coord in dim_encodings["months"].keys():
                            month_name = dim_encodings["months"][month_coord]
                            values.append(
                                float(
                                    point_pkg_mmm[model_name][scenario_name][month_name][
                                        var_name
                                    ][era_title]['mean']
                                )
                            )

                        # This will be the min, mean, and max taken from the mean monthly values
                        # to find the min-mean-max of the annual values for a given time period such
                        # as 1950-2009 for historical.
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
                        if var_name not in point_pkg_mmm[model_name][scenario_name]['Annual']:
                            point_pkg_mmm[model_name][scenario_name]['Annual'][var_name] = dict()

                        point_pkg_mmm[model_name][scenario_name]['Annual'][var_name][era_title] = dict()

                        point_pkg_mmm[model_name][scenario_name]['Annual'][var_name][era_title][
                            "min"
                        ] = min_value
                        point_pkg_mmm[model_name][scenario_name]['Annual'][var_name][era_title][
                            "mean"
                        ] = mean_value
                        point_pkg_mmm[model_name][scenario_name]['Annual'][var_name][era_title][
                            "max"
                        ] = max_value



    return point_pkg_mmm


@routes.route("/hydrology/")
@routes.route("/hydrology/abstract/")
@routes.route("/hydrology/point/")
def hydro_about():
    return render_template("documentation/hydrology.html")


@routes.route("/hydrology/point/<lat>/<lon>")
def run_get_hydrology_point_data(lat, lon, summarize=None, preview=None):
    validation = validate_latlon(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )

    # validate request arguments if they exist; set summarize argument accordingly
    if len(request.args) == 0 and (summarize is None and preview is None):
        try:
            point_pkg = run_fetch_hydrology_point_data(lat, lon)
            return postprocess(point_pkg, "hydrology")

        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500

    elif all(key in request.args for key in ["summarize", "format"]):
        if (request.args.get("summarize") == "mmm") & (
            request.args.get("format") == "csv"
        ):
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

        else:
            return render_template("400/bad_request.html"), 400

    elif "summarize" in request.args or summarize:
        if request.args.get("summarize") == "mmm" or summarize:
            try:
                return run_fetch_hydrology_point_data_mmm(lat, lon, summarize)
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500
        else:
            return render_template("400/bad_request.html"), 400

    elif "format" in request.args or preview:
        if request.args.get("format") == "csv" or preview:
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
        else:
            return render_template("400/bad_request.html"), 400

    else:
        return render_template("400/bad_request.html"), 400


@routes.route("/eds/hydrology/<lat>/<lon>")
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

