import asyncio
from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
)
import numpy as np
from urllib.parse import quote

from generate_urls import generate_wcs_query_url
from fetch_data import generate_wcs_getcov_str, get_dim_encodings, fetch_data
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import prune_nulls_with_max_intensity, postprocess
from config import WEST_BBOX, EAST_BBOX
from . import routes

degree_days_api = Blueprint("degree_days_api", __name__)

# all degree day coverages share common dim_encodings, so only fetch one
dd_dim_encodings = asyncio.run(get_dim_encodings("heating_degree_days_Fdays"))
# update the encoding for "historical" to be "modeled_baseline"
# this is to make the data better align with engineer expectations
dd_dim_encodings["scenario"][0] = "modeled_baseline"

var_ep_lu = {
    "heating": {"cov_id_str": "heating_degree_days_Fdays"},
    "below_zero": {"cov_id_str": "degree_days_below_zero_Fdays"},
    "thawing_index": {"cov_id_str": "air_thawing_index_Fdays"},
    "freezing_index": {"cov_id_str": "air_freezing_index_Fdays"},
}
var_label_lu = {
    "heating_degree_days": "Heating Degree Days",
    "degree_days_below_zero": "Degree Days Below Zero",
    "thawing_index": "Air Thawing Index",
    "freezing_index": "Air Freezing Index",
}
years_lu = {
    "modeled_baseline": {"min": 1980, "max": 2017},
    "projected": {"min": 1950, "max": 2099},
}
mmm_lu = {
    "modeled_baseline": {"model": 0, "scenario": 0},
    "projected": {"models": [1, 2, 3, 4, 5, 6, 7, 8, 9], "scenarios": [1, 2]},
}
n_results_lu = {
    "modeled_baseline": 1,
    "projected": len(mmm_lu["projected"]["models"])
    * len(mmm_lu["projected"]["scenarios"]),
}


def validate_years(start_year, end_year):
    """Check provided years against valid ranges for data.

    Args:
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        True if years are valid, otherwise an error page to show valid years
    """
    if None not in [start_year, end_year]:
        min_year = years_lu["projected"]["min"]
        max_year = years_lu["projected"]["max"]

        for year in [start_year, end_year]:
            if year < min_year or year > max_year:
                return (
                    render_template(
                        "422/invalid_year.html",
                        start_year=start_year,
                        end_year=end_year,
                        min_year=min_year,
                        max_year=max_year,
                    ),
                    422,
                )
    return True


def within_modeled_baseline(year):
    # if year is within the modeled_baseline range, return True
    year = int(year)
    return (
        year >= years_lu["modeled_baseline"]["min"]
        and year <= years_lu["modeled_baseline"]["max"]
    )


def make_time_slicer(min_year, max_year):
    """Create formatted string to slice along year axis.

    This is more convenient when slicing with long time strings.

    Args:
        min_year (int): minimum year for slicing
        max_year (int): maximum year for slicing
    Returns:
        time_slicer (str): string to insert in WCPS or WCS fragment
    """
    time_slicer = f"{min_year}:{max_year}"
    return time_slicer


def daymet_slice():
    """Return coordinates to slice Daymet data from model axis."""
    return (
        f"{mmm_lu['modeled_baseline']['model']}:{mmm_lu['modeled_baseline']['model']}"
    )


def modeled_baseline_scenario_slice():
    """Return coordinates to slice `modeled_baseline` data from scenario axis."""
    return f"{mmm_lu['modeled_baseline']['scenario']}:{mmm_lu['modeled_baseline']['scenario']}"


def gcms_slice():
    """Return coordinates that will slice GCM data from model axis."""
    return f"{min(mmm_lu['projected']['models'])}:{max(mmm_lu['projected']['models'])}"


def rcps_slice():
    """Return coordinates that will slice RCP data from scenario axis."""
    return f"{min(mmm_lu['projected']['scenarios'])}:{max(mmm_lu['projected']['scenarios'])}"


def get_dd_wcps_request_str(
    x,
    y,
    cov_id,
    model_slice,
    scenario_slice,
    year_slice,
    operation,
    num_results,
    encoding="json",
):
    """Generate WCPS query fragment specific to degree days min-mean-max summarization.

    Args:
        x (float or str): x-coordinate for point query, or string
            composed as "x1:x2" for bbox query, where x1 and x2 are
            lower and upper bounds of bbox
        y (float or str): y-coordinate for point query, or string
            composed as "y1:y2" for bbox query, where y1 and y2 are
            lower and upper bounds of bbox
        cov_id (str): rasdaman coverage ID
        model_slice (str): like "3:3" for a single model
        scenario_slice (str): like "1:2" for two scenarios
        year_slice (str): like "1980:2017" for modeled_baseline
        operation (str): one of "min", "mean", or "max"
        num_results (int): number of results to average over when operation is "mean"
        encoding (str): one of "json" or "netcdf" for point or bbox queries, respectively

    Returns:
        (str) WCPS query fragment
    """
    if operation == "mean":
        wcps_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := avg(condense + over $m model ({model_slice}), $s scenario ({scenario_slice}) "
                f"using $c[model($m),scenario($s),year({year_slice}),X({x}),Y({y})] / {num_results} )"
                f'return encode( $a, "application/{encoding}")'
            )
        )
    else:
        wcps_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := {operation}(condense {operation} over $m model({model_slice}), $s scenario ({scenario_slice}) "
                f"using $c[model($m),scenario($s),year({year_slice}),X({x}),Y({y})] ) "
                f'return encode( $a, "application/{encoding}")'
            )
        )
    return wcps_str


def package_unabridged_response(nested_list, start_year=None, end_year=None):
    """Convert nested list of unabridged degree day data to JSON-like dict.

    This function should only be called for requests querying all data. Summarized (`mmm`) queries will call a different packaging function.
    Args:
        nested_list (list): nested list representing the Rasdaman query response
        start_year (int): start year for WCS query or None
        end_year (int): end year for WCS query or None
    Returns:
        unabridged (dict): packaged data with proper dimensional encodings for each value, i.e. model>scenario>year>'dd': value
    """
    unabridged = {}
    if None in [start_year, end_year]:
        start = 1950
    else:
        start = int(start_year)

    for model_index, model_data in enumerate(nested_list):
        model_name = dd_dim_encodings["model"][model_index]
        unabridged[model_name] = {}

        for scenario_index, scenario_data in enumerate(model_data):
            scenario_name = dd_dim_encodings["scenario"][scenario_index]
            unabridged[model_name][scenario_name] = {}

            for year_index, dd_value in enumerate(scenario_data):
                year = start + year_index
                unabridged[model_name][scenario_name][year] = {}
                unabridged[model_name][scenario_name][year]["dd"] = dd_value

    return unabridged


def package_distilled_response(mmm_point_data, start_year=None, end_year=None):
    """Package min-mean-max degree day summary to labeled JSON-like dict.

    This function should only be called for requests querying summarized data. Unabridged queries will call a different packaging function.
    Args:
        mmm_point_data (dict): nested Rasdaman query response with summary>era keys
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None
    Returns:
        distilled (dict): response re-packaged with proper dimensional encodings for each value, i.e. era>ddmin/mean/max: value
    """
    distilled = {}
    for era in mmm_point_data["min"].keys():
        print(era)
        distilled[era] = {}
        distilled[era]["ddmin"] = mmm_point_data["min"][era]["wcps_response"]
        distilled[era]["ddmean"] = round(mmm_point_data["mean"][era]["wcps_response"])
        distilled[era]["ddmax"] = mmm_point_data["max"][era]["wcps_response"]
    return distilled


async def fetch_dd_point_data(x, y, cov_id, start_year=None, end_year=None):
    """Run the async degree days data request for a single point.

    Args:
        x (float): x-coordinate
        y (float): y-coordinate
        cov_id (str): heating_degree_days_Fdays, degree_days_below_zero_Fdays,
            air_thawing_index_Fdays, or air_freezing_index_Fdays
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        JSON-like dict of data at provided xy coordinate
    """
    if request.args.get("summarize") == "mmm":
        # first create slicers to insert in WCPS request
        time_slicers = {}
        model_slicers = {}
        scenario_slicers = {}
        # there will always gcm + rcp data because they span 1950-2099
        model_slicers.update({"projected": gcms_slice()})
        scenario_slicers.update({"projected": rcps_slice()})
        # though the below time slice may get modified by custom year requests
        future_slicer = make_time_slicer(
            years_lu["projected"]["min"], years_lu["projected"]["max"]
        )
        time_slicers.update({"projected": future_slicer})
        # modeled_baseline slices
        modeled_baseline_slicer = make_time_slicer(
            years_lu["modeled_baseline"]["min"], years_lu["modeled_baseline"]["max"]
        )
        time_slicers.update({"modeled_baseline": modeled_baseline_slicer})
        model_slicers.update({"modeled_baseline": daymet_slice()})
        scenario_slicers.update({"modeled_baseline": modeled_baseline_scenario_slice()})

        # modify slicers if start / end years provided
        if None not in [start_year, end_year]:
            custom_slicer = make_time_slicer(start_year, end_year)
            time_slicers.update({"projected": custom_slicer})
            # handling modeled_baseline slicers is a little more hairy
            if within_modeled_baseline(start_year) and within_modeled_baseline(
                end_year
            ):
                # case where start and end year within modeled_baseline range
                modeled_baseline_slicer = make_time_slicer(start_year, end_year)
                time_slicers.update({"modeled_baseline": modeled_baseline_slicer})
            elif within_modeled_baseline(start_year) and not within_modeled_baseline(
                end_year
            ):
                # case where only start year is within modeled_baseline range
                modeled_baseline_slicer = make_time_slicer(
                    start_year, years_lu["modeled_baseline"]["max"]
                )
                time_slicers.update({"modeled_baseline": modeled_baseline_slicer})
            elif not within_modeled_baseline(start_year) and within_modeled_baseline(
                end_year
            ):
                # case where only end year is within modeled_baseline range
                modeled_baseline_slicer = make_time_slicer(
                    years_lu["modeled_baseline"]["min"], end_year
                )
                time_slicers.update({"modeled_baseline": modeled_baseline_slicer})
            else:
                # no need for modeled_baseline slicers at all and we save on WCPS queries
                del time_slicers["modeled_baseline"]
                del model_slicers["modeled_baseline"]
                del scenario_slicers["modeled_baseline"]

        # making three wcps requests, one each for min-mean-max
        mmm_dispatch = {}
        for stat_function in ["min", "mean", "max"]:
            mmm_dispatch[stat_function] = {}
            # each era will have different mmm wcps request parameters
            for era in model_slicers.keys():
                mmm_dispatch[stat_function][era] = {}
                wcps_str = get_dd_wcps_request_str(
                    x,
                    y,
                    cov_id,
                    model_slicers[era],
                    scenario_slicers[era],
                    time_slicers[era],
                    stat_function,
                    n_results_lu[era],
                )
                # CP note: print below for helpful debugging
                wcps_url = generate_wcs_query_url(wcps_str)
                mmm_dispatch[stat_function][era]["wcps_url"] = wcps_url
                mmm_dispatch[stat_function][era]["wcps_response"] = await fetch_data(
                    [wcps_url]
                )
        return mmm_dispatch
    else:
        # fire off unabridged request
        point_data_list = []
        if None in [start_year, end_year]:
            request_str = generate_wcs_getcov_str(x, y, cov_id)
        else:
            ts = f"{start_year},{end_year}"
            request_str = generate_wcs_getcov_str(x, y, cov_id, time_slice=("year", ts))
        wcs_query_url = generate_wcs_query_url(request_str)
        point_data_list = await fetch_data([wcs_query_url])
        return point_data_list


@routes.route("/degree_days/")
@routes.route("/degree_days/abstract/")
@routes.route("/degree_days/heating/")
@routes.route("/degree_days/below_zero/")
@routes.route("/degree_days/thawing_index/")
@routes.route("/degree_days/freezing_index/")
def degree_days_about():
    return render_template("/documentation/degree_days.html")


@routes.route("/degree_days/<var_ep>/<lat>/<lon>")
@routes.route("/degree_days/<var_ep>/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_dd_point_data(
    var_ep, lat, lon, start_year=None, end_year=None, preview=None
):
    """Fetch degree day point data for specified lat/lon and return JSON-like dict.

    Args:
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        lat (float): latitude
        lon (float): longitude
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query
        preview (bool): for tabular data preview

    Returns:
        JSON-like dict of requested degree day data
    """
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

    if None not in [start_year, end_year]:
        valid_year = validate_years(int(start_year), int(end_year))
        if valid_year is not True:
            return valid_year

    if var_ep in var_ep_lu.keys():
        cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
        try:
            point_data = asyncio.run(
                fetch_dd_point_data(x, y, cov_id_str, start_year, end_year)
            )
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500
    else:
        return render_template("400/bad_request.html"), 400

    # validate additional args explicitly

    # if preview, return unabridged tidy package as CSV
    # the preview arg is only used for CSV generation and should never occur with additional request args
    if preview:
        dd_data_package = package_unabridged_response(point_data, start_year, end_year)
        tidy_package = prune_nulls_with_max_intensity(
            postprocess(dd_data_package, cov_id_str)
        )
        if tidy_package in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        else:
            return create_csv(tidy_package, cov_id_str, lat=lat, lon=lon)

    # if no request args, return unabridged tidy package
    elif len(request.args) == 0:
        dd_data_package = package_unabridged_response(point_data, start_year, end_year)
        tidy_package = prune_nulls_with_max_intensity(
            postprocess(dd_data_package, cov_id_str)
        )
        return tidy_package

    # if valid args for both mmm & csv, make distilled tidy package and return as CSV
    elif all(key in request.args for key in ["summarize", "format"]):
        if (request.args.get("summarize") == "mmm") & (
            request.args.get("format") == "csv"
        ):
            dd_data_package = package_distilled_response(
                point_data, start_year, end_year
            )
            tidy_package = prune_nulls_with_max_intensity(
                postprocess(dd_data_package, cov_id_str)
            )
            if tidy_package in [{}, None, 0]:
                return render_template("404/no_data.html"), 404
            else:
                return create_csv(tidy_package, cov_id_str, lat=lat, lon=lon)
        else:
            return render_template("400/bad_request.html"), 400

    # if valid args for only mmm, return distilled tidy package
    elif "summarize" in request.args:
        if request.args.get("summarize") == "mmm":
            dd_data_package = package_distilled_response(
                point_data, start_year, end_year
            )
            tidy_package = prune_nulls_with_max_intensity(
                postprocess(dd_data_package, cov_id_str)
            )
            return tidy_package
        else:
            return render_template("400/bad_request.html"), 400

    # if valid args for only csv, return unabridged tidy package as CSV
    elif "format" in request.args:
        if request.args.get("format") == "csv":
            dd_data_package = package_unabridged_response(
                point_data, start_year, end_year
            )
            tidy_package = prune_nulls_with_max_intensity(
                postprocess(dd_data_package, cov_id_str)
            )
            if tidy_package in [{}, None, 0]:
                return render_template("404/no_data.html"), 404
            else:
                return create_csv(tidy_package, cov_id_str, lat=lat, lon=lon)
        else:
            return render_template("400/bad_request.html"), 400

    # if args were > 0 but not valid, return 400
    else:
        return render_template("400/bad_request.html"), 400


@routes.route("/eds/degree_days/<var_ep>/<lat>/<lon>")
def get_dd_plate(var_ep, lat, lon):
    """
    Endpoint for requesting all data required for Heating Degree Days,
    Degree Days Below Zero, Air Thawing Index, and Air Freezing Index in the
    ArcticEDS client.
    Args:
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        lat (float): latitude
        lon (float): longitude
    Notes:
        example request: http://localhost:5000/eds/degree_days/heating/65.0628/-146.1627
    """
    dd = dict()
    eras = [
        {"start": 1980, "end": 2009},
        {"start": 2010, "end": 2039},
        {"start": 2040, "end": 2069},
        {"start": 2070, "end": 2099},
    ]

    all_data = run_fetch_dd_point_data(var_ep, lat, lon)
    # Checks if error exists from fetching DD point
    if isinstance(all_data, tuple):
        # Returns error template that was generated for invalid request
        return all_data

    summarized_data = {}
    if "modeled_baseline" not in summarized_data:
        summarized_data["modeled_baseline"] = {}

    modeled_baseline_values_to_summarize = []
    for year, value in all_data["daymet"]["modeled_baseline"].items():
        if year >= eras[0]["start"] and year <= eras[0]["end"]:
            modeled_baseline_values_to_summarize.append(value["dd"])
    summarized_data["modeled_baseline"] = {
        "ddmax": max(modeled_baseline_values_to_summarize),
        "ddmean": round(np.mean(modeled_baseline_values_to_summarize)),
        "ddmin": min(modeled_baseline_values_to_summarize),
    }

    models = list(all_data.keys())
    models.remove("daymet")
    for era in eras[1:]:
        era_label = str(era["start"]) + "-" + str(era["end"])
        if era_label not in summarized_data:
            summarized_data[era_label] = {}
            dd_values = []
            for model in all_data.keys():
                for scenario in all_data[model].keys():
                    for year, value in all_data[model][scenario].items():
                        if year >= era["start"] and year <= era["end"]:
                            dd_values.append(value["dd"])
            summarized_data[era_label] = {
                "ddmin": min(dd_values),
                "ddmean": round(np.mean(dd_values)),
                "ddmax": max(dd_values),
            }

    dd["summary"] = summarized_data

    preview = run_fetch_dd_point_data(var_ep, lat, lon, preview=True)
    # Checks if error exists from preview CSV request
    if isinstance(preview, tuple):
        # Returns error template that was generated for invalid request
        return preview

    dd_csv = preview.data.decode("utf-8")
    first = "\n".join(dd_csv.split("\n")[3:9]) + "\n"
    last = "\n".join(dd_csv.split("\n")[-6:])
    dd["preview"] = first + last

    return jsonify(dd)
