import asyncio
from math import floor
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
    validate_latlon,
)
from generate_urls import generate_wcs_query_url
from generate_requests import generate_wcs_getcov_str
from fetch_data import fetch_data, describe_via_wcps
from postprocessing import merge_dicts, postprocess, prune_nulls_with_max_intensity
from . import routes
from config import WEST_BBOX, EAST_BBOX

seaice_api = Blueprint("temperature_anomalies_api", __name__)

anomaly_coverage_id = "air_freezing_index_Fdays"
baseline_coverage_id = "air_freezing_index_Fdays"


async def get_temperature_anomalies_metadata(coverage_id):
    metadata = await describe_via_wcps(coverage_id)
    return get_coverage_encodings(metadata)


anomaly_dim_encodings = asyncio.run(
    get_temperature_anomalies_metadata(anomaly_coverage_id)
)
baseline_dim_encodings = asyncio.run(
    get_temperature_anomalies_metadata(baseline_coverage_id)
)


def package_anomaly_data(point_data_list):
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
                di[model]["temperature_anomalies"][scenario][year] = round(value, 2)
    return di


def package_baseline_data(point_data_list):
    di = dict()
    for mi, value in enumerate(point_data_list):
        model = anomaly_dim_encodings["model"][mi]
        di[model] = dict()
        di[model]["temperature_baseline"] = round(value, 2)
    return di


def package_temperature_anomalies_data(point_data_list, cov_id):
    if cov_id == anomaly_coverage_id:
        return package_anomaly_data(point_data_list)
    elif cov_id == baseline_coverage_id:
        return package_baseline_data(point_data_list)


@routes.route("/temperature_anomalies/point/<lat>/<lon>/")
def run_point_fetch_all_temperature_anomalies(lat, lon):
    # validation = validate_latlon(lat, lon, [anomaly_coverage_id])
    # if validation == 400:
    #     return render_template("400/bad_request.html"), 400
    # if validation == 404:
    #     return (
    #         render_template("404/no_data.html"),
    #         404,
    #     )
    # if validation == 422:
    #     return (
    #         render_template(
    #             "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
    #         ),
    #         422,
    #     )

    # # try:
    # merged_data = dict()
    # for cov_id in [baseline_coverage_id, anomaly_coverage_id]:
    #     wcs_str = generate_wcs_getcov_str(
    #         lon,
    #         lat,
    #         cov_id=cov_id,
    #         projection="EPSG:4326",
    #     )

    #     # Generate the URL for the WCS query
    #     url = generate_wcs_query_url(wcs_str)

    #     # Fetch the data
    #     point_data_list = asyncio.run(fetch_data([url]))
    #     data_di = package_temperature_anomalies_data(point_data_list, cov_id)
    #     merged_data = merge_dicts(merged_data, data_di)

    # data = prune_nulls_with_max_intensity(
    #     postprocess(merged_data, "temperature_anomalies")
    # )
    data = {
        "Berkeley-Earth": {
            "temperature_anomalies": {
                "historical": {
                    "1850": -0.73,
                    "1851": 0.13,
                    "1852": -0.19,
                    "1853": -0.12,
                    "1854": -0.11,
                }
            },
            "temperature_baseline": -6.04,
        },
        "CESM2": {
            "temperature_anomalies": {
                "ssp126": {
                    "2025": 3.44,
                    "2026": 1.51,
                    "2027": 2.24,
                    "2028": 1.37,
                    "2029": 1.19,
                }
            },
            "temperature_baseline": -4.18,
        },
    }
    if request.args.get("format") == "csv":
        place_id = request.args.get("community")
        return create_csv(data, "temperature_anomalies", place_id, lat, lon)
    return data
    # except Exception as exc:
    #     if hasattr(exc, "status") and exc.status == 404:
    #         return render_template("404/no_data.html"), 404
    #     return render_template("500/server_error.html"), 500
