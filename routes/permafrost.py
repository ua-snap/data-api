import asyncio
import pandas as pd
from urllib.parse import quote
from flask import Blueprint, render_template, request, jsonify, Response

# local imports
from generate_urls import generate_wcs_query_url
from validate_data import place_name_and_type
from fetch_data import (
    fetch_data,
    fetch_data_api,
    fetch_wcs_point_data,
    get_dim_encodings,
    generate_wcs_getcov_str,
    deepflatten,
)
from csv_functions import csv_metadata, create_csv

from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import nullify_and_prune, nullify_nodata, postprocess
from . import routes
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX

# rasdaman coverage
gipl_1km_coverage_id = "crrel_gipl_outputs"

gipl1km_dim_encodings = asyncio.run(
    get_dim_encodings(gipl_1km_coverage_id, scrape=("time", "gmlrgrid:coefficients", 4))
)

permafrost_api = Blueprint("permafrost_api", __name__)

# geoserver layers
wms_targets = [
    "obu_2018_magt",
]
wfs_targets = {
    "jorgenson_2008_pf_extent_ground_ice_volume": "GROUNDICEV,PERMAFROST",
    "obu_pf_extent": "PFEXTENT",
}

titles = {
    "gipl_1km": "GIPL 2.0 1km Model Output: Mean Annual Ground Temperature (deg C) at Permafrost Base and Permafrost Top; Talik Thickness (m)",
    "jorg": "Jorgenson et al. (2008) Permafrost Extent and Ground Ice Volume",
    "obupfx": "Obu et al. (2018) Permafrost Extent",
}


# packaging functions unique to each query
def package_obu_magt(obu_magt_resp):
    """Package Obu MAGT raster data."""
    if obu_magt_resp["features"] == []:
        return None
    depth = "Top of Permafrost"
    year = "2000-2016"
    titles["obu_magt"] = (
        f"Obu et al. (2018) {year} Mean Annual {depth} Ground Temperature (deg C)"
    )
    temp = obu_magt_resp["features"][0]["properties"]["GRAY_INDEX"]
    if temp is None:
        return None
    temp = round(temp, 1)

    nullified_data = nullify_nodata(temp, "permafrost")
    if nullified_data is not None:
        di = {"year": year, "depth": depth, "temp": temp}
        return di

    return None


def package_jorgenson(jorgenson_resp):
    """Package Jorgenson vector data."""
    if jorgenson_resp["features"] == []:
        return None
    ice = jorgenson_resp["features"][0]["properties"]["GROUNDICEV"]
    pfx = jorgenson_resp["features"][0]["properties"]["PERMAFROST"]
    di = {"ice": ice, "pfx": pfx}
    return di


def package_obu_vector(obu_vector_resp):
    """Package Obu permafrost extent vector data."""
    if obu_vector_resp["features"] == []:
        return None
    pfx = obu_vector_resp["features"][0]["properties"]["PFEXTENT"]
    di = {"pfx": pfx}
    return di


def make_ncr_gipl1km_wcps_request_str(x, y, years, model, scenario, summary_operation):
    """Generate a WCPS query string specific the to GIPL 1 km coverage.

    Arguments:
        x -- (float) x-coordinate for the point query
        y -- (float) y-coordinate for the point query
        years -- (str) colon-separated ISO date-time,= e.g., "\"2040-01-01T00:00:00.000Z\":\"2069-01-01T00:00:00.000Z\""
        model(int) - Integer representing model (0 = 5ModelAvg, 1 = GFDL-CM3, 2 = NCAR-CCSM4
        scenario(int) - Integer representing scenario (0 = RCP 4.5, 1 = RCP 8.5)
        summary_operation -- (str) one of 'min', 'avg', or 'max'
    Returns:
        gipl1km_wcps_str -- (str) fragment used to construct the WCPS request
    """
    gipl1km_wcps_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({gipl_1km_coverage_id}) "
            f"  return encode (coverage summary over $v variable(0:9)"
            f"  values {summary_operation}( $c[variable($v),model({model}),scenario({scenario}),year({years}),X({x}),Y({y})] )"
            f', "application/json")'
        )
    )
    return gipl1km_wcps_str


def package_ncr_gipl1km_wcps_data(gipl1km_wcps_resp):
    """Package a min-mean-max summary of ten GIPL 1 km variables for a given year range and model type. Values are rounded to one decimal place because units are either meters or degrees C.

    Arguments:
        gipl1km_wcps_resp -- (list) nested 3-level list of the WCPS response values. The response order must be min-mean-max: [[min], [mean], [max]]

    Returns:
        gipl1km_wcps_point_pkg -- (dict) min-mean-max summarized results for all ten variables
    """
    gipl1km_wcps_point_pkg = dict()
    models = ["5ModelAvg", "GFDL-CM3", "NCAR-CCSM4"]
    for all_resp, model in zip(gipl1km_wcps_resp, models):
        gipl1km_wcps_point_pkg[model] = dict()
        scenarios = ["rcp45", "rcp85"]
        for scenario_resp, scenario in zip(all_resp, scenarios):
            gipl1km_wcps_point_pkg[model][scenario] = dict()
            summary_methods = ["min", "mean", "max"]
            for resp, stat_type in zip(scenario_resp, summary_methods):
                gipl1km_wcps_point_pkg[model][scenario][f"gipl1km{stat_type}"] = dict()
                for k, v in zip(gipl1km_dim_encodings["variable"].values(), resp):
                    gipl1km_wcps_point_pkg[model][scenario][f"gipl1km{stat_type}"][
                        k
                    ] = round(v, 1)
    return gipl1km_wcps_point_pkg


def make_gipl1km_wcps_request_str(x, y, years, summary_operation):
    """Generate a WCPS query string specific the to GIPL 1 km coverage.

    Arguments:
        x -- (float) x-coordinate for the point query
        y -- (float) y-coordinate for the point query
        years -- (str) colon-separated ISO datetime, e.g., "\"2040-01-01T00:00:00.000Z\":\"2069-01-01T00:00:00.000Z\""
        summary_operation -- (str) one of 'min', 'avg', or 'max'
    Returns:
        gipl1km_wcps_str -- (str) fragment used to construct WCPS request
    """
    gipl1km_wcps_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({gipl_1km_coverage_id}) "
            f"  return encode (coverage summary over $v variable(0:9)"
            f"  values {summary_operation}( $c[variable($v),year({years}),X({x}),Y({y})] )"
            f', "application/json")'
        )
    )
    return gipl1km_wcps_str


def package_gipl1km_wcps_data(gipl1km_wcps_resp):
    """Package the min-mean-max summary of ten GIPL variables. Summary is done across all models and scenarios for a given year range. Values rounded to one decimal place because units are either meters or degrees C.

    Arguments:
        gipl1km_wcps_resp -- (list) nested list of WCPS response values. The response order must be min-mean-max: [[min], [mean], [max]]

    Returns:
        gipl1km_wcps_point_pkg -- (dict) min-mean-max summarized results for all ten variables
    """
    gipl1km_wcps_point_pkg = dict()

    for summary_op_resp, stat_type in zip(gipl1km_wcps_resp, ["min", "mean", "max"]):
        gipl1km_wcps_point_pkg[f"gipl1km{stat_type}"] = dict()

        for k, v in zip(gipl1km_dim_encodings["variable"].values(), summary_op_resp):
            gipl1km_wcps_point_pkg[f"gipl1km{stat_type}"][k] = round(v, 1)

    return gipl1km_wcps_point_pkg


def generate_gipl1km_time_index():
    """Generate a time index for annual GIPL 1km outputs.

    Returns:
        dt_range (pandas DatetimeIndex): a time index with annual frequency
    """
    timestamps = [x[1:-2] for x in gipl1km_dim_encodings["time"].split(" ")]
    date_index = pd.DatetimeIndex(timestamps)
    return date_index


def package_gipl1km_point_data(gipl1km_point_resp, time_slice=None):
    """Package the response for full set of point data. The native structure of the response is nested as follows: model (0 1 2), year, scenario (0 1), variable (0 9). Values are rounded to one decimal place because units are either meters or degrees C.

    Arguments:
        gipl1km_point_resp -- (list) deeply nested list of WCS response values.
        time_slice -- (tuple) 2-tuple of (start_year, end_year)
    Returns:
        gipl1km_point_pkg -- (dict) results for all ten variables, all models, all scenario. defaults to the entire time range (time_slice=None).
    """

    # must match length of time index when it is sliced
    flat_list = list(deepflatten(gipl1km_point_resp))
    i = 0

    gipl1km_point_pkg = {}
    for model_name in gipl1km_dim_encodings["model"].values():
        gipl1km_point_pkg[model_name] = {}
        if time_slice is not None:
            start, stop = time_slice
            tx = generate_gipl1km_time_index()
            tx = tx[tx.slice_indexer(f"{start}-01-01", f"{stop}-01-01")]
        else:
            tx = generate_gipl1km_time_index()
        for t in tx:
            year = t.date().strftime("%Y")
            gipl1km_point_pkg[model_name][year] = {}
            for scenario_name in gipl1km_dim_encodings["scenario"].values():
                gipl1km_point_pkg[model_name][year][scenario_name] = {}
                for gipl_var_name in gipl1km_dim_encodings["variable"].values():
                    gipl1km_point_pkg[model_name][year][scenario_name][
                        gipl_var_name
                    ] = round(flat_list[i], 1)
                    i += 1

    return gipl1km_point_pkg


@routes.route("/permafrost/")
@routes.route("/permafrost/abstract/")
@routes.route("/permafrost/point/")
def pf_about():
    return render_template("documentation/permafrost.html")


@routes.route("/permafrost/point/gipl/<lat>/<lon>")
@routes.route("/permafrost/point/gipl/<lat>/<lon>/<start_year>/<end_year>")
def gipl_1km_point_data(lat, lon, start_year=None, end_year=None):
    """Run the async request for GIPL permafrost data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude
        start_year (float): start year
        end_year (float): end year

    Optional request args:
        summarize: summarization method; must be 'mmm' for min, mean, max summary (eg, '?summarize=mmm' appended to endpoint)
        format: format for data export; must be 'csv' for CSV export (eg, '?format=csv' appended to endpoint)

    Returns:
        JSON-like dict of permafrost data, or CSV of permafrost data if format=csv
    """
    # validate request arguments if they exist; set summarize argument accordingly

    if len(request.args) == 0:
        return asyncio.run(
            run_fetch_gipl_1km_point_data(
                lat, lon, start_year, end_year, summarize=None
            )
        )

    elif all(key in request.args for key in ["summarize", "format"]):
        if (request.args.get("summarize") == "mmm") & (
            request.args.get("format") == "csv"
        ):
            return asyncio.run(
                run_fetch_gipl_1km_point_data(
                    lat, lon, start_year, end_year, summarize="mmm"
                )
            )
        else:
            return render_template("400/bad_request.html"), 400

    elif "summarize" in request.args:
        if request.args.get("summarize") == "mmm":
            return asyncio.run(
                run_fetch_gipl_1km_point_data(
                    lat, lon, start_year, end_year, summarize="mmm"
                )
            )
        else:
            return render_template("400/bad_request.html"), 400

    elif "format" in request.args:
        if request.args.get("format") == "csv":
            return asyncio.run(
                run_fetch_gipl_1km_point_data(
                    lat, lon, start_year, end_year, summarize=None
                )
            )

    else:
        return render_template("400/bad_request.html"), 400


@routes.route("/permafrost/point/<lat>/<lon>")
def run_point_fetch_all_permafrost(lat, lon):
    """Run the async request for all permafrost data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Optional request args:
        format: format for data export; must be 'csv' for CSV export (eg, '?format=csv' appended to endpoint)

    Returns:
        JSON-like dict of permafrost data, or CSV of permafrost data if format=csv
    """
    # validate request arguments if they exist; allow only format=csv argument, otherwise throw an error
    if len(request.args) == 0:
        pass
    elif "format" in request.args:
        if request.args.get("format") == "csv":
            pass
        else:
            return render_template("400/bad_request.html"), 400
    else:
        return render_template("400/bad_request.html"), 400

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

    gs_results = asyncio.run(
        fetch_data_api(
            GS_BASE_URL, "permafrost_beta", wms_targets, wfs_targets, lat, lon
        )
    )

    x, y = project_latlon(lat, lon, 3338)

    try:
        rasdaman_results = asyncio.run(fetch_wcs_point_data(x, y, gipl_1km_coverage_id))
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    data = {
        "gipl_1km": package_gipl1km_point_data(rasdaman_results),
        "obu_magt": package_obu_magt(gs_results[0]),
        "jorg": package_jorgenson(gs_results[1]),
        "obupfx": package_obu_vector(gs_results[2]),
    }

    if request.args.get("format") == "csv":
        return create_csv(data, "permafrost", lat=lat, lon=lon, source_metadata=titles)

    return postprocess(data, "permafrost", titles)


async def fetch_gipl_1km_point_data(x, y, start_year, end_year, summarize, ncr):
    if all(val != None for val in [start_year, end_year, summarize]):
        wcps_response_data = list()
        timestring = (
            f'"{start_year}-01-01T00:00:00.000Z":"{end_year}-01-01T00:00:00.000Z"'
        )

        if ncr:
            wcps_response_data = list()
            for model_num in range(3):
                model = list()
                for scenario in range(2):
                    scenario_data = list()
                    for summary_operation in ["min", "avg", "max"]:
                        wcps_request_str = make_ncr_gipl1km_wcps_request_str(
                            x, y, timestring, model_num, scenario, summary_operation
                        )
                        scenario_data.append(
                            await fetch_data([generate_wcs_query_url(wcps_request_str)])
                        )
                    model.append(scenario_data)
                wcps_response_data.append(model)

        else:
            for summary_operation in ["min", "avg", "max"]:
                wcps_request_str = make_gipl1km_wcps_request_str(
                    x, y, timestring, summary_operation
                )
                wcps_response_data.append(
                    await fetch_data([generate_wcs_query_url(wcps_request_str)])
                )

        return wcps_response_data

    if start_year is None and end_year is None and summarize is not None:
        wcps_response_data = list()
        # in lieu of start/end dates but requesting a summary, use min and max years of dataset to summarize entire dataset
        time_index = generate_gipl1km_time_index()
        minyear = str(time_index.min())[:4]
        maxyear = str(time_index.max())[:4]
        timestring = f'"{minyear}-01-01T00:00:00.000Z":"{maxyear}-01-01T00:00:00.000Z"'
        for summary_operation in ["min", "avg", "max"]:
            wcps_request_str = make_gipl1km_wcps_request_str(
                x, y, timestring, summary_operation
            )
            wcps_response_data.append(
                await fetch_data([generate_wcs_query_url(wcps_request_str)])
            )
        return wcps_response_data

    if start_year is not None and end_year is not None and summarize is None:
        timestring = (
            f'"{start_year}-01-01T00:00:00.000Z":"{end_year}-01-01T00:00:00.000Z"'
        )
        time_subset = ("year", timestring)
        gipl_request_str = generate_wcs_getcov_str(
            x, y, gipl_1km_coverage_id, time_slice=time_subset
        )
        gipl_point_data = await fetch_data([generate_wcs_query_url(gipl_request_str)])
        return gipl_point_data

    else:
        gipl_request_str = generate_wcs_getcov_str(x, y, gipl_1km_coverage_id)
        gipl_point_data = await fetch_data([generate_wcs_query_url(gipl_request_str)])
        return gipl_point_data


async def run_fetch_gipl_1km_point_data(
    lat, lon, start_year=None, end_year=None, summarize=None, preview=None, ncr=False
):
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

    x, y = project_latlon(lat, lon, 3338)

    # year validation could be moved to `validate_request` module: need a function that uses the time index from rasdaman coverage to validate min/max years... could this be an API-wide function for all rasdaman coverages?
    if start_year is not None or end_year is not None:
        try:
            time_index = generate_gipl1km_time_index()
            start_valid = pd.Timestamp(int(start_year), 1, 1) >= time_index.min()
            end_valid = pd.Timestamp(int(end_year), 1, 1) <= time_index.max()
            chronological = start_year < end_year
            years_valid = start_valid and end_valid and chronological
        except:
            return render_template("400/bad_request.html"), 400
        if years_valid != True:
            return render_template("400/bad_request.html"), 400

    try:
        gipl_1km_point_data = await asyncio.create_task(
            fetch_gipl_1km_point_data(x, y, start_year, end_year, summarize, ncr)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    if ncr is True and all(val != None for val in [start_year, end_year, summarize]):
        gipl_1km_point_package = package_ncr_gipl1km_wcps_data(gipl_1km_point_data)
    elif all(val != None for val in [start_year, end_year, summarize]):
        gipl_1km_point_package = package_gipl1km_wcps_data(gipl_1km_point_data)
    elif start_year is None and end_year is None and summarize is not None:
        gipl_1km_point_package = package_gipl1km_wcps_data(gipl_1km_point_data)

    elif start_year is not None and end_year is not None and summarize is None:
        gipl_1km_point_package = package_gipl1km_point_data(
            gipl_1km_point_data, (start_year, end_year)
        )
    else:
        gipl_1km_point_package = package_gipl1km_point_data(gipl_1km_point_data)

    if request.args.get("format") == "csv" or preview:
        point_pkg = nullify_and_prune(gipl_1km_point_package, "crrel_gipl")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        if summarize is not None:
            return create_csv(point_pkg, "gipl_summary", lat=lat, lon=lon)
        return create_csv(point_pkg, "gipl", lat=lat, lon=lon)
    return postprocess(gipl_1km_point_package, "crrel_gipl")


async def run_eds_preview(lat, lon):
    year_ranges = [{"start": 2021, "end": 2025}, {"start": 2096, "end": 2100}]
    tasks = []
    for years in year_ranges:
        tasks.append(
            asyncio.create_task(
                run_fetch_gipl_1km_point_data(
                    lat,
                    lon,
                    start_year=years["start"],
                    end_year=years["end"],
                    preview=True,
                )
            ),
        )

    results = await asyncio.gather(*tasks)
    return results


async def run_ncr_requests(lat, lon, ncr):
    year_ranges = [
        {
            "start": 2021,
            "end": 2039,
        },
        {
            "start": 2040,
            "end": 2069,
        },
        {
            "start": 2070,
            "end": 2099,
        },
    ]

    tasks = []
    for years in year_ranges:
        tasks.append(
            asyncio.create_task(
                run_fetch_gipl_1km_point_data(
                    lat,
                    lon,
                    start_year=years["start"],
                    end_year=years["end"],
                    summarize="mmm",
                    ncr=ncr,
                )
            ),
        )

    results = await asyncio.gather(*tasks)
    ncr = dict()
    for index in range(len(results)):
        key = f'{year_ranges[index]["start"]}-{year_ranges[index]["end"]}'
        ncr[key] = results[index]

    return ncr


def aggregate_csv(permafrostData):
    combined_lines = []
    metadata_captured = False
    headers_captured = False
    metadata_line_previously = False
    for key, value in permafrostData.items():
        lines = value.get_data().decode("utf-8").split("\n")
        for idx, line in enumerate(lines):
            # Grab metadata lines from only one child CSV so they are not
            # duplicated at the start of each set of era rows.
            if line.startswith("#"):
                if not metadata_captured:
                    combined_lines.append(line)
                    metadata_captured = True
                metadata_line_previously = True
            # Also grab the column header labels exactly once.
            elif metadata_line_previously:
                if not headers_captured:
                    headers = "era," + line
                    combined_lines.append(headers)
                    headers_captured = True
                metadata_line_previously = False
            # Otherwise, append the era label to the data row.
            elif line != "":
                combined_lines.append(key + "," + line)

    return "\n".join(combined_lines)


@routes.route("/eds/permafrost/<lat>/<lon>")
def permafrost_eds_request(lat, lon):
    """
    Endpoint for providing permafrost preview of GIPL 2.0 data
        Args:
            lat (float): latitude
            lon (float): longitude

        Returns:
            JSON-like dict of preview permafrost data
    """
    permafrostData = dict()

    # Get the summary and preview data
    summary = permafrost_ncr_request(lat, lon, ncr=False)

    # Check for error response from summary response
    if isinstance(summary, tuple):
        return summary

    preview = asyncio.run(run_eds_preview(lat, lon))

    # Check for error responses in the preview
    for response in preview:
        if isinstance(response, tuple):
            return response

    # If there are no error responses, include the summary and preview data in the response
    permafrostData["summary"] = summary
    # permafrostData["permafrost"]["preview"] = [r.data.decode("utf-8") for r in preview]

    preview_string = [r.data.decode("utf-8") for r in preview]

    preview_past = preview_string[0].split("\n")[3:9]
    preview_future = preview_string[1].split("\n")[-6:]
    permafrostData["preview"] = (
        "\n".join(preview_past) + "\n" + "\n".join(preview_future)
    )

    return jsonify(permafrostData)


@routes.route("/ncr/permafrost/point/<lat>/<lon>")
def permafrost_ncr_request(lat, lon, ncr=True):
    permafrostData = asyncio.run(run_ncr_requests(lat, lon, ncr))

    # Return corresponding error page if any sub-request returns error.
    for value in permafrostData.values():
        if isinstance(value, tuple):
            if value[1] == 400:
                return render_template("400/bad_request.html"), 400
            if value[1] == 404:
                return render_template("404/no_data.html"), 404
            if value[1] == 422:
                return (
                    render_template(
                        "422/invalid_latlon.html",
                        west_bbox=WEST_BBOX,
                        east_bbox=EAST_BBOX,
                    ),
                    422,
                )
            else:
                return render_template("500/server_error.html"), 500

    if request.args.get("format") == "csv":
        # Combine CSV results from multiple eras into a single CSV.
        csv_content = aggregate_csv(permafrostData)

        place_id = request.args.get("community")
        if place_id:
            place_name, place_type = place_name_and_type(place_id)
            filename = "Permafrost for " + quote(place_name) + ".csv"
            metadata = csv_metadata(place_name, place_id, place_type)
        else:
            filename = "Permafrost for " + lat + ", " + lon + ".csv"
            metadata = csv_metadata(lat=lat, lon=lon)

        csv_content = metadata + csv_content

        response = Response(
            csv_content,
            mimetype="text/csv",
            headers={
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Disposition": 'attachment; filename="'
                + filename
                + "\"; filename*=utf-8''\""
                + filename
                + '"',
            },
        )

        return response

    return permafrostData
