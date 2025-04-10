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
from postprocessing import postprocess, prune_nulls_with_max_intensity
from . import routes
from config import WEST_BBOX, EAST_BBOX

seaice_api = Blueprint("temperature_anomalies_api", __name__)
coverage_id = "temperature_anomalies"


async def get_temperature_anomalies_metadata():
    metadata = await describe_via_wcps(coverage_id)
    return get_coverage_encodings(metadata)


dim_encodings = asyncio.run(get_temperature_anomalies_metadata())


def package_temperature_anomalies_data(point_data_list):
    di = dict()
    years = list(range(1850, 2101))
    for mi, model_li in enumerate(point_data_list):
        model = dim_encodings["model"][mi]
        if model not in di:
            di[model] = dict()
        for si, scenario_li in enumerate(model_li):
            scenario = dim_encodings["scenario"][si]
            if scenario not in di[model]:
                di[model][scenario] = dict()
            for yi, value in enumerate(scenario_li):
                year = years[yi]
                di[model][scenario][year] = value
    return di


@routes.route("/temperature_anomalies/point/<lat>/<lon>/")
def run_point_fetch_all_temperature_anomalies(lat, lon):
    validation = validate_latlon(lat, lon, [coverage_id])
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

    # try:
    wcs_str = generate_wcs_getcov_str(
        lon,
        lat,
        cov_id=coverage_id,
        projection="EPSG:4326",
    )

    # Generate the URL for the WCS query
    url = generate_wcs_query_url(wcs_str)

    # Fetch the data
    point_data_list = asyncio.run(fetch_data([url]))

    temperature_anomalies_data = postprocess(
        package_temperature_anomalies_data(point_data_list), "temperature_anomalies"
    )
    # if request.args.get("format") == "csv":
    #     if type(temperature_anomalies_data) is not dict:
    #         # Returns errors if any are generated
    #         return temperature_anomalies_data
    #     # Returns CSV for download
    #     data = postprocess(
    #         package_temperature_anomalies_data(rasdaman_response), "temperature_anomalies"
    #     )
    #     return create_csv(data, "temperature_anomalies", lat=lat, lon=lon)
    # # Returns sea ice concentrations across years & months
    return prune_nulls_with_max_intensity(temperature_anomalies_data)
    # # except Exception as exc:
    # #     if hasattr(exc, "status") and exc.status == 404:
    # #         return render_template("404/no_data.html"), 404
    # #     return render_template("500/server_error.html"), 500
