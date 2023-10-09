import asyncio
import io
import numpy as np
import xarray as xr
from urllib.parse import quote
from flask import (
    Blueprint,
    render_template,
    request,
    current_app as app,
)

# local imports
from fetch_data import (
    fetch_wcs_point_data,
    get_poly_3338_bbox,
    summarize_within_poly,
    summarize_within_poly_sum,
)
from validate_request import (
    validate_latlon,
    project_latlon,
)
from generate_urls import generate_wcs_query_url, generate_wfs_intersection_url
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
    "rounding": {
        "all": 1,
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


def run_fetch_hydrology_point_data_mmm(lat, lon):
    """Fetch hydrology data for a
       given latitude and longitude, and summarize over eras.

    Args:
        lat (float): latitude
        lon (float): longitude

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

    return point_pkg_mmm


def fetch_poly_bbox_netcdf(poly_id):
    """Get hydrology coverage within bounding box of a polygon.

    Args:
        poly_id: ID of the HUC polygon to use to extract netCDF data

    Returns:
        tuple of:
            poly (geodataframe): geodataframe of HUC polygon
            ds (xarray dataset): xarray dataset of the hydrology coverage clipped to HUC polygon bbox extent

    """
    # get polygon bounding box, and create XY indices
    poly = get_poly_3338_bbox(poly_id)

    x = f"{poly.bounds[0]}:{poly.bounds[2]}"
    y = f"{poly.bounds[1]}:{poly.bounds[3]}"
    # create & run WCPS request that returns a smaller coverage encoded as netcdf
    wcps_request_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({hydrology_coverage_id}) "
            f"return encode($c[X({x}), Y({y})],"
            f'"application/netcdf")'
        )
    )
    wcps_request_url = generate_wcs_query_url(wcps_request_str)
    netcdf_bytes = asyncio.run(fetch_data([wcps_request_url]))
    # create xarray.DataSet from bytestring
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes))

    return poly, ds


def run_fetch_hydrology_zonal_stats(poly_id):
    """Get data summary (e.g. zonal mean or zonal sum) of all variables in a polygon.

    Args:
        poly_id: ID of the HUC polygon to use to extract netCDF data

    Returns:
        JSON-like dict of data for all variables, averaged over the HUC polygon.
    """
    # get poly gdf and xarray dataset
    poly, ds = fetch_poly_bbox_netcdf(poly_id)

    zonal_stats_results_dict = dict()

    # summarize the dataset by polygon and store results in dictionary with varname keys
    varnames = []
    for var_coord in dim_encodings["varnames"].keys():
        varnames.append(dim_encodings["varnames"][var_coord])

    for varname in varnames:
        # compute zonal mean for vars explicitly listed below
        if varname in ["sm1", "sm2", "sm3", "iwe", "swe", "pcp", "tmin", "tmax"]:
            print(varname)
            var_zonal_mean_dict = summarize_within_poly(
                ds=ds,
                poly=poly,
                dim_encodings=dim_encodings,
                varname=varname,
                roundkey="all",
            )

            zonal_stats_results_dict[varname] = var_zonal_mean_dict

        # compute zonal sum for vars explicitly listed below
        elif varname in ["evap", "runoff", "glacier_melt", "snow_melt"]:
            print(varname)
            var_zonal_sum_dict = summarize_within_poly_sum(
                ds=ds,
                poly=poly,
                dim_encodings=dim_encodings,
                varname=varname,
                roundkey="all",
            )

            zonal_stats_results_dict[varname] = var_zonal_sum_dict

    # package zonal results dict with decoded coord values, using varnames to call aggregated values from zonal stats dicts
    zonal_pkg = dict()
    for model_coord in dim_encodings["models"].keys():
        model_name = dim_encodings["models"][model_coord]
        zonal_pkg[model_name] = dict()
        for scenario_coord in dim_encodings["scenarios"].keys():
            scenario_name = dim_encodings["scenarios"][scenario_coord]
            zonal_pkg[model_name][scenario_name] = dict()
            for month_coord in dim_encodings["months"].keys():
                month_name = dim_encodings["months"][month_coord]
                zonal_pkg[model_name][scenario_name][month_name] = dict()
                for era_coord in dim_encodings["eras"].keys():
                    era_name = dim_encodings["eras"][era_coord]
                    zonal_pkg[model_name][scenario_name][month_name][era_name] = dict()
                    for var_coord in dim_encodings["varnames"].keys():
                        var_name = dim_encodings["varnames"][var_coord]
                        value = zonal_stats_results_dict[var_name][model_name][
                            scenario_name
                        ][month_name][era_name]

                        # reformat any NaN stats to string "nan" to avoid "is not valid JSON" error

                        if np.isnan(value):
                            value = "nan"

                        zonal_pkg[model_name][scenario_name][month_name][era_name][
                            var_name
                        ] = value

    return zonal_pkg


def run_fetch_hydrology_zonal_stats_mmm(poly_id):
    """Get data summary (e.g. zonal mean or zonal sum) of all variables in a polygon, then summarize over eras.

    Args:
        poly_id: ID of the HUC polygon to use to extract netCDF data

    Returns:
        JSON-like dict of data for all variables; values are the means of previously aggregated (averaged or summed) values over the HUC polygon.
    """
    # get standard zonal stats package
    zonal_pkg = run_fetch_hydrology_zonal_stats(poly_id)

    # repackage zonal stats data with mmm values computed across eras
    zonal_pkg_mmm = dict()
    for model_coord in dim_encodings["models"].keys():
        model_name = dim_encodings["models"][model_coord]
        zonal_pkg_mmm[model_name] = dict()
        for scenario_coord in dim_encodings["scenarios"].keys():
            scenario_name = dim_encodings["scenarios"][scenario_coord]
            zonal_pkg_mmm[model_name][scenario_name] = dict()
            for month_coord in dim_encodings["months"].keys():
                month_name = dim_encodings["months"][month_coord]
                zonal_pkg_mmm[model_name][scenario_name][month_name] = dict()
                for var_coord in dim_encodings["varnames"].keys():
                    var_name = dim_encodings["varnames"][var_coord]
                    zonal_pkg_mmm[model_name][scenario_name][month_name][
                        var_name
                    ] = dict()

                    values = list()

                    for era_coord in dim_encodings["eras"].keys():
                        era_name = dim_encodings["eras"][era_coord]

                        values.append(
                            float(
                                zonal_pkg[model_name][scenario_name][month_name][
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

                    zonal_pkg_mmm[model_name][scenario_name][month_name][var_name][
                        "min"
                    ] = min_value
                    zonal_pkg_mmm[model_name][scenario_name][month_name][var_name][
                        "mean"
                    ] = mean_value
                    zonal_pkg_mmm[model_name][scenario_name][month_name][var_name][
                        "max"
                    ] = max_value

    return zonal_pkg_mmm


@routes.route("/hydrology/")
@routes.route("/hydrology/abstract/")
@routes.route("/hydrology/point/")
def hydro_about():
    return render_template("documentation/hydrology.html")


@routes.route("/hydrology/point/<lat>/<lon>")
def run_get_hydrology_point_data(lat, lon):
    """Point data endpoint for hydrology data - returns a summary of data at the provided lat/lon coordinate.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested hydrology data.
    """
    # validate lat / lon
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
    if len(request.args) == 0:
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

    elif "summarize" in request.args:
        if request.args.get("summarize") == "mmm":
            try:
                return run_fetch_hydrology_point_data_mmm(lat, lon)
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500
        else:
            return render_template("400/bad_request.html"), 400

    elif "format" in request.args:
        if request.args.get("format") == "csv":
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


@routes.route("/hydrology/local/<lat>/<lon>")
def run_get_hydrology_local_data(lat, lon):
    """ "Local" endpoint for hydrology data - finds the HUC that intersects
    the request lat/lon and returns a summary of data within that polygon.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested hydrology data.
    """
    # validate lat / lon
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

    # create request for vector features that intersect, and return all features
    features = asyncio.run(fetch_data([generate_wfs_intersection_url(lat, lon)]))[
        "features"
    ]

    if len(features) < 1:
        return render_template("404/no_data.html"), 404

    # check features for hucs and list them
    huc = []
    for feature in features:
        if (feature["properties"]["type"] == "huc") & (
            # huc8s:
            # len(feature["properties"]["id"])== 8
            # huc10s
            len(feature["properties"]["id"])
            == 10
        ):
            huc.append(feature)

    if len(huc) < 1:
        return render_template("404/no_data.html"), 404
    if len(huc) > 1:
        # condition is maybe not a server error, but if query returns more than one HUC8/10 than we likely have a problem with our HUC layer
        return render_template("500/server_error.html"), 500

    huc_id = huc[0]["properties"]["id"]

    # validate request arguments if they exist
    if len(request.args) == 0:
        try:
            zonal_pkg = run_fetch_hydrology_zonal_stats(int(huc_id))
            return postprocess(zonal_pkg, "hydrology")

        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500

    elif all(key in request.args for key in ["summarize", "format"]):
        if (request.args.get("summarize") == "mmm") & (
            request.args.get("format") == "csv"
        ):
            try:
                zonal_pkg = run_fetch_hydrology_zonal_stats_mmm(int(huc_id))
                place_id = request.args.get("community")
                if place_id:
                    return create_csv(
                        zonal_pkg, "hydrology_mmm", place_id=place_id, lat=lat, lon=lon
                    )
                else:
                    return create_csv(zonal_pkg, "hydrology_mmm", lat=lat, lon=lon)
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500

        else:
            return render_template("400/bad_request.html"), 400

    elif "summarize" in request.args:
        if request.args.get("summarize") == "mmm":
            try:
                return run_fetch_hydrology_zonal_stats_mmm(int(huc_id))
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500
        else:
            return render_template("400/bad_request.html"), 400

    elif "format" in request.args:
        if request.args.get("format") == "csv":
            try:
                zonal_pkg = run_fetch_hydrology_zonal_stats(int(huc_id))
                place_id = request.args.get("community")
                if place_id:
                    return create_csv(
                        zonal_pkg, "hydrology", place_id=place_id, lat=lat, lon=lon
                    )
                else:
                    return create_csv(
                        zonal_pkg, "hydrology", place_id=None, lat=lat, lon=lon
                    )
            except Exception as exc:
                if hasattr(exc, "status") and exc.status == 404:
                    return render_template("404/no_data.html"), 404
                return render_template("500/server_error.html"), 500
        else:
            return render_template("400/bad_request.html"), 400

    else:
        return render_template("400/bad_request.html"), 400
