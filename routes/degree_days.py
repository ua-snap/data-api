import asyncio
from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
)
from urllib.parse import quote

# local imports
from generate_urls import generate_wcs_query_url
from fetch_data import generate_wcs_getcov_str, get_dim_encodings, fetch_data
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import nullify_and_prune, postprocess
from config import WEST_BBOX, EAST_BBOX
from . import routes

degree_days_api = Blueprint("degree_days_api", __name__)

# all degree day coverages share common dim_encodings, so only fetch one
dd_dim_encodings = asyncio.run(get_dim_encodings("heating_degree_days_Fdays"))

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
    "historical": {"min": 1980, "max": 2017},
    "projected": {"min": 1950, "max": 2099},
}
mmm_lu = {
    "historical": {"model": 0, "scenario": 0},
    "projected": {"models": [1, 2, 3, 4, 5, 6, 7, 8, 9], "scenarios": [1, 2]},
}
n_results_lu = {
    "historical": 1,
    "projected": len(mmm_lu["projected"]["models"])
    * len(mmm_lu["projected"]["scenarios"]),
}


def make_time_slicer(min_year, max_year):
    time_slicer = f"{min_year}:{max_year}"
    return time_slicer


def make_wcps_slicers(start_year, end_year):
    pass


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
    """Generate WCPS query fragment specific to the degree days min-mean-max summarization.

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
        year_slice (str): like "1980:2017" for historical
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


# CP note: called by run_fetch...then point package. basic route
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
    point_data_list = []

    if request.args.get("summarize") == "mmm":
        # create axis coordinate slicers to insert in WCPS fragment
        time_slicers = {}
        model_slicers = {}
        scenario_slicers = {}

        if start_year is not None:
            pass
            # custom_slicer = make_time_slicer(start_year, end_year)
            # time_slicers.update({"custom": custom_slicer})

        #     # case where start and and year within historical
        #     # case where only start within historical
        #     # case where only end with historical
        #     # case where neither within historical
        else:
            # historical and future slicing and summarization required
            historical_slicer = make_time_slicer(
                years_lu["historical"]["min"], years_lu["historical"]["max"]
            )
            time_slicers.update({"historical": historical_slicer})
            model_slicers.update(
                {
                    "historical": f"{mmm_lu['historical']['model']}:{mmm_lu['historical']['model']}"
                }
            )
            scenario_slicers.update(
                {
                    "historical": f"{mmm_lu['historical']['scenario']}:{mmm_lu['historical']['scenario']}"
                }
            )

            future_slicer = make_time_slicer(
                years_lu["projected"]["min"], years_lu["projected"]["max"]
            )
            time_slicers.update({"projected": future_slicer})
            model_slicers.update(
                {
                    "projected": f"{min(mmm_lu['projected']['models'])}:{max(mmm_lu['projected']['models'])}"
                }
            )
            scenario_slicers.update(
                {
                    "projected": f"{min(mmm_lu['projected']['scenarios'])}:{max(mmm_lu['projected']['scenarios'])}"
                }
            )

            # making three wcps requests, one each for min-mean-max
            mmm_dispatch = {}
            for stat_function in ["min", "mean", "max"]:
                mmm_dispatch[stat_function] = {}
                # each era will have different mmm wcps request parameters
                for era in ["historical", "projected"]:
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

                    wcps_url = generate_wcs_query_url(wcps_str)
                    print(wcps_url)
                    mmm_dispatch[stat_function][era]["wcps_url"] = wcps_url

        # mmm_dispatch[stat_function][era]["wcps_response"] = (
        #     await fetch_data([mmm_dispatch[stat_function][era]["wcps_url"]])
        # )

        # point_data_list.append(
        #     mmm_dispatch[stat_function][era]["wcps_response"]
        # )
    else:
        request_str = generate_wcs_getcov_str(x, y, cov_id)
        wcs_query_url = generate_wcs_query_url(request_str)
        point_data_list = await fetch_data([wcs_query_url])

    return point_data_list


def package_unabridged_response(nested_list, start_year=None, end_year=None):
    """Convert nested list of unabridged degree days data to JSON-like dict."""

    unabridged = {}

    for model_index, model_data in enumerate(nested_list):
        model_name = dd_dim_encodings["model"][model_index]
        unabridged[model_name] = {}

        for scenario_index, scenario_data in enumerate(model_data):
            scenario_name = dd_dim_encodings["scenario"][scenario_index]
            unabridged[model_name][scenario_name] = {}

            for year_index, dd_value in enumerate(scenario_data):
                year = 1950 + year_index
                unabridged[model_name][scenario_name][year] = dd_value

    return unabridged


def package_distilled_response(point_data, start_year=None, end_year=None):
    """Convert nested list of distilled (mean, min, max) degree days data to JSON-like dict."""

    distilled = {}
    if request.args.get("summarize") == "mmm":
        # need to see how WCPS request changes, will pick up edits here
        distilled["historical"] = {}
        distilled["projected"] = {}

        historical_max = round(point_data[0], 1)
        historical_mean = round(point_data[1], 1)
        historical_min = round(point_data[2], 1)
        distilled["historical"]["ddmin"] = round(historical_min)
        distilled["historical"]["ddmean"] = round(historical_mean)
        distilled["historical"]["ddmax"] = round(historical_max)
        projected_max = round(point_data[3], 1)
        projected_mean = round(point_data[4], 1)
        projected_min = round(point_data[5], 1)
        distilled["projected"]["ddmin"] = round(projected_min)
        distilled["projected"]["ddmean"] = round(projected_mean)
        distilled["projected"]["ddmax"] = round(projected_max)
    else:
        # CP note: this function should only get called when the summary param is mmm
        # so let's fail if this function gets called otherwise
        assert request.args.get("summarize") == "mmm"
    # else:
    #     for mi, v_li in enumerate(point_data):  # (nested list with model at dim 0)
    #         if mi == 0:
    #             min_year = years_lu["historical"]["min"]
    #             max_year = years_lu["historical"]["max"]
    #             years = range(min_year, max_year + 1)
    #         else:
    #             min_year = years_lu["projected"]["min"]
    #             max_year = years_lu["projected"]["max"]
    #             years = range(min_year, max_year + 1)

    #         model = dd_dim_encodings["model"][mi]
    #         point_pkg[model] = {}

    # Responses from Rasdaman include the same array length for both
    # historical and projected data, representing every possible year
    # (1979-2100). This means both the historical and projected data
    # arrays include nodata years populated with 0s. The code below
    # omits nodata gaps and makes sure the correct year is assigned to
    # its corresponding data in the historical and projected data
    # arrays.
    # year = years_lu["historical"]["min"]
    # year_index = 0
    # if None in [start_year, end_year]:
    #     start_year = years_lu["historical"]["min"]
    #     end_year = years_lu["projected"]["max"]
    # for value in v_li:
    #     if year in years:
    #         if year >= int(start_year) and year <= int(end_year):
    #             point_pkg[model][years[year_index]] = {"dd": round(value)}
    #         year_index += 1
    #     year += 1

    return distilled


@routes.route("/degree_days/")
@routes.route("/degree_days/abstract/")
@routes.route("/degree_days/heating/")
@routes.route("/degree_days/below_zero/")
@routes.route("/degree_days/thawing_index/")
@routes.route("/degree_days/freezing_index/")
def degree_days_about():
    return render_template("/documentation/degree_days.html")


@routes.route("/eds/degree_days/<var_ep>/<lat>/<lon>")
def get_dd_plate(var_ep, lat, lon):
    """
    Endpoint for requesting all data required for the Heating Degree Days,
    Below Zero Degree Days, Thawing Index, and Freezing Index in the
    ArcticEDS client.
    Args:
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        lat (float): latitude
        lon (float): longitude
    Notes:
        example request: http://localhost:5000/eds/degree_days/heating/65.0628/-146.1627
    """
    dd = dict()

    summarized_data = {}
    if "historical" not in summarized_data:
        summarized_data["historical"] = {}

    all_data = run_fetch_dd_point_data(var_ep, lat, lon)

    # Checks if error exists from fetching DD point
    if isinstance(all_data, tuple):
        # Returns error template that was generated for invalid request
        return all_data

    historical_values = list(map(lambda x: x["dd"], all_data["ERA-Interim"].values()))
    summarized_data["historical"] = {
        "ddmax": max(historical_values),
        "ddmean": round(np.mean(historical_values)),
        "ddmin": min(historical_values),
    }

    eras = [
        {"start": 2010, "end": 2039},
        {"start": 2040, "end": 2069},
        {"start": 2070, "end": 2099},
    ]
    models = list(all_data.keys())
    models.remove("ERA-Interim")
    for era in eras:
        era_label = str(era["start"]) + "-" + str(era["end"])
        if era_label not in summarized_data:
            summarized_data[era_label] = {}
        dd_values = []
        for model in all_data.keys():
            for year, value in all_data[model].items():
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


@routes.route("/degree_days/<var_ep>/<lat>/<lon>")
@routes.route("/degree_days/<var_ep>/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_dd_point_data(
    var_ep, lat, lon, start_year=None, end_year=None, preview=None
):
    """Degree days data endpoint. Fetch point data for
    specified lat/lon and return JSON-like dict.

    Args:
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        lat (float): latitude
        lon (float): longitude
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested degree days data

    Notes:
        example request: http://localhost:5000/degree_days/heating/all/65/-147
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
            point_data_list = asyncio.run(
                fetch_dd_point_data(x, y, cov_id_str, start_year, end_year)
            )
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500
    else:
        return render_template("400/bad_request.html"), 400

    # point_pkg = package_dd_point_data(point_data_list, start_year, end_year)
    unabridged_package = package_unabridged_response(
        point_data_list, start_year, end_year
    )

    # if request.args.get("format") == "csv" or preview:
    #     point_pkg = nullify_and_prune(point_pkg, cov_id_str)
    #     if point_pkg in [{}, None, 0]:
    #         return render_template("404/no_data.html"), 404
    #     if request.args.get("summarize") == "mmm":
    #         return create_csv(point_pkg, cov_id_str, lat=lat, lon=lon)
    #     else:
    #         return create_csv(point_pkg, cov_id_str + "_all", lat=lat, lon=lon)

    # return postprocess(point_pkg, cov_id_str)
    return unabridged_package
    # return {"a": 3442}


def validate_years(start_year, end_year):
    """Check provided years against valid ranges for historical vs. projected.

    Args:
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        True if years are valid, otherwise an error page to show valid years
    """
    if None not in [start_year, end_year]:
        min_year = years_lu["historical"]["min"]
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
