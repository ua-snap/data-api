import asyncio
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from fetch_data import (
    describe_via_wcps,
)
from csv_functions import create_csv
from validate_request import (
    get_coverage_encodings,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
)
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from postprocessing import merge_dicts, postprocess, prune_nulls_with_max_intensity
from . import routes

temperature_anomaly_api = Blueprint("temperature_anomalies_api", __name__)

anomaly_coverage_id = "temperature_anomaly_anomalies"
baseline_coverage_id = "temperature_anomaly_baselines"

anomaly_metadata = asyncio.run(describe_via_wcps(anomaly_coverage_id))
baseline_metadata = asyncio.run(describe_via_wcps(baseline_coverage_id))

anomaly_dim_encodings = get_coverage_encodings(anomaly_metadata)
baseline_dim_encodings = get_coverage_encodings(baseline_metadata)


def package_anomaly_data(point_data_list):
    """
    Package the temperature anomaly values into human-readable JSON format

    Args:
        point_data_list (list): nested list of data from Rasdaman WCS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from anomaly_dim_encodings global variable
    """
    di = dict()
    years = list(range(1850, 2101))
    for mi, model_li in enumerate(point_data_list):
        model = anomaly_dim_encodings["model"][mi]
        if model not in di:
            di[model] = {
                "temperature_anomalies": dict(),
            }
        for si, scenario_li in enumerate(model_li):
            scenario = anomaly_dim_encodings["scenario"][si]
            if scenario not in di[model]:
                di[model]["temperature_anomalies"][scenario] = dict()
            for yi, value in enumerate(scenario_li):
                year = years[yi]
                if value is not None:
                    value = round(value, 2)
                di[model]["temperature_anomalies"][scenario][year] = value
    return di


def package_baseline_data(point_data_list):
    """
    Package the temperature baseline values into human-readable JSON format

    Args:
        point_data_list (list): nested list of data from Rasdaman WCS query

    Returns:
        di (dict): dictionary mirroring structure of nested list with keys derived from baseline_dim_encodings global variable
    """
    di = dict()
    for mi, value in enumerate(point_data_list):
        model = anomaly_dim_encodings["model"][mi]
        di[model] = dict()
        di[model]["temperature_baseline"] = round(value, 2)
    return di


def package_temperature_anomalies_data(point_data_list, cov_id):
    """
    Package the temperature anomalies & temperature anomaly baselines

    Args:
        point_data_list (list): nested list of data from Rasdaman WCS query
        cov_id (str): coverage ID of the data being processed

    Returns:
        di (dict): dictionary mirroring structure of nested list for corresponding cov_id
    """
    if cov_id == anomaly_coverage_id:
        return package_anomaly_data(point_data_list)
    elif cov_id == baseline_coverage_id:
        return package_baseline_data(point_data_list)


@routes.route("/temperature_anomalies/")
@routes.route("/temperature_anomalies/point/")
def about_temperature_anomalies():
    return render_template("documentation/temperature_anomalies.html")


@routes.route("/temperature_anomalies/point/<lat>/<lon>/")
def run_fetch_temperature_anomalies_point_data(lat, lon):
    """
    Query both the temperature anomalies & baselines coverages

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of requested temperature anomalies & baselines data

    Notes:
        example request: http://localhost:5000/temperature_anomalies/point/63.73/-166.32

    """
    # Validate the lat/lon values. Anomaly and baseline coverages have the same
    # BBOX, so use only one of them to validate the lat/lon.
    anomaly_bbox = construct_latlon_bbox_from_coverage_bounds(anomaly_metadata)
    within_bounds = validate_latlon_in_bboxes(lat, lon, [anomaly_bbox])

    if within_bounds == 400:
        return render_template("400/bad_request.html"), 400
    if within_bounds == 404:
        return (
            render_template("404/no_data.html"),
            404,
        )
    if within_bounds == 422:
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html", bboxes=[anomaly_bbox]
            ),
            422,
        )

    try:
        merged_data = dict()
        for cov_id in [baseline_coverage_id, anomaly_coverage_id]:
            wcs_str = generate_wcs_getcov_str(
                lon,
                lat,
                cov_id=cov_id,
                projection="EPSG:4326",
            )

            # Generate the URL for the WCS query
            url = generate_wcs_query_url(wcs_str)

            # Fetch the data
            point_data_list = asyncio.run(fetch_data([url]))
            data_di = package_temperature_anomalies_data(point_data_list, cov_id)
            merged_data = merge_dicts(merged_data, data_di)

        data = prune_nulls_with_max_intensity(
            postprocess(merged_data, "temperature_anomalies")
        )

        if request.args.get("format") == "csv":
            place_id = request.args.get("community")
            return create_csv(data, "temperature_anomalies", place_id, lat, lon)
        return data
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
