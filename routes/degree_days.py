import asyncio
from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
)

# local imports
from generate_urls import generate_wcs_query_url
from fetch_data import *
from csv_functions import create_csv
from validate_request import (
    validate_latlon,
    project_latlon,
)
from postprocessing import nullify_and_prune, postprocess
from config import WEST_BBOX, EAST_BBOX
from . import routes

degree_days_api = Blueprint("degree_days_api", __name__)

# The heating_degree_days, degree_days_below_zero, thawing_index, and
# freezing_index coverages all share the same dim_encodings
dd_dim_encodings = asyncio.run(get_dim_encodings("heating_degree_days"))

var_ep_lu = {
    "heating": {"cov_id_str": "heating_degree_days"},
    "below_zero": {"cov_id_str": "degree_days_below_zero"},
    "thawing_index": {"cov_id_str": "thawing_index"},
    "freezing_index": {"cov_id_str": "freezing_index"},
}

var_label_lu = {
    "heating_degree_days": "Heating Degree Days",
    "degree_days_below_zero": "Degree Days Below Zero",
    "thawing_index": "Thawing Index",
    "freezing_index": "Freezing Index",
}

years_lu = {
    "historical": {"min": 1980, "max": 2009},
    "projected": {"min": 2006, "max": 2100},
}


def get_dd_wcps_request_str(x, y, cov_id, models, years, tempstat, encoding="json"):
    """Generates a WCPS query specific to the
    coverages used for the degree days min-mean-max.

    Args:
        x (float or str): x-coordinate for point query, or string
            composed as "x1:x2" for bbox query, where x1 and x2 are
            lower and upper bounds of bbox
        y (float or str): y-coordinate for point query, or string
            composed as "y1:y2" for bbox query, where y1 and y2 are
            lower and upper bounds of bbox
        cov_id (str): Rasdaman coverage ID
        models (str): Comma-separated numbers of requested models
        years (str): Colon-separated full date-time i.e.
            "\"2006-01-01T00:00:00.000Z\":\"2100-01-01T00:00:00.000Z\""
        tempstat(int): Integer between 0-2 where:
            - 0 = ddmax
            - 1 = ddmean
            - 2 = ddmin
        encoding (str): currently supports either "json" or "netcdf"
            for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """
    if tempstat == 0:
        operation = "max"
    elif tempstat == 2:
        operation = "min"
    else:
        operation = "+"

    if tempstat == 0 or tempstat == 2:
        wcps_request_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := {operation}(condense {operation} over $m model({models}) "
                f"using $c[model($m),year({years}),X({x}),Y({y})] ) "
                f'return encode( $a, "application/{encoding}")'
            )
        )
        return wcps_request_str
    else:
        # Generates the mean across models

        # For projected, 2 models
        num_results = 2

        # For historical, only a single model
        if models == "0:0":
            num_results = 1

        wcps_request_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := avg(condense {operation} over $m model({models}) "
                f"using $c[model($m),year({years}),X({x}),Y({y})] / {num_results} ) "
                f'return encode( $a, "application/{encoding}")'
            )
        )
        return wcps_request_str


@routes.route("/degree_days/")
@routes.route("/degree_days/abstract/")
@routes.route("/degree_days/heating/")
@routes.route("/degree_days/below_zero/")
@routes.route("/degree_days/thawing_index/")
@routes.route("/degree_days/freezing_index/")
def degree_days_about():
    return render_template("/documentation/degree_days.html")


@routes.route("/eds/degree_days/<var_ep>/<lat>/<lon>/<preview>")
def get_dd_plate(var_ep, lat, lon, preview=None):
    """
    Endpoint for requesting all data required for the Heating Degree Days,
    Below Zero Degree Days, Thawing Index, and Freezing Index in the
    ArcticEDS client.
    Args:
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        lat (float): latitude
        lon (float): longitude
        preview (string): Generates CSV preview for degree day variable for
                          ArcticEDS.
    Notes:
        example request: http://localhost:5000/eds/degree_days/heating/65.0628/-146.1627
    """
    if preview:
        # Grabs the first and last 5 years of
        # the data for a particular variable
        first = run_fetch_dd_point_data(var_ep, lat, lon, 1980, 1984)
        last = run_fetch_dd_point_data(var_ep, lat, lon, 2096, 2100)
        combined_dict = {}

        # Error response checking for invalid input parameters
        for response in [first, last]:
            if isinstance(response, tuple):
                # Returns error template that was generated for invalid request
                return response[0]

        # Iterate through the keys in both dictionaries
        for key in first.keys() | last.keys():
            # Merge the dictionaries on the current key
            combined_dict[key] = {
                **(first.get(key, {}) or {}),
                **(last.get(key, {}) or {}),
            }

        return jsonify(combined_dict)
    else:
        summarized_data = {}
        if "historical" not in summarized_data:
            summarized_data["historical"] = {}

        all_data = run_fetch_dd_point_data(var_ep, lat, lon)

        historical_values = list(
            map(lambda x: x["dd"], all_data["ERA-Interim"].values())
        )
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

        return jsonify(summarized_data)


def package_dd_point_data(point_data, start_year=None, end_year=None):
    """Add JSON response data for heating_degree_days, below_zero_degree_days,
    thawing_index, and freezing_index coverages

    Args:
        point_data (list): nested list containing JSON results of WCPS query

    Returns:
        JSON-like dict of query results
    """
    point_pkg = {}

    if request.args.get("summarize") == "mmm":
        historical_max = round(point_data[0], 1)
        historical_mean = round(point_data[1], 1)
        historical_min = round(point_data[2], 1)

        point_pkg["historical"] = {}
        point_pkg["historical"]["ddmin"] = round(historical_min)
        point_pkg["historical"]["ddmean"] = round(historical_mean)
        point_pkg["historical"]["ddmax"] = round(historical_max)

        projected_max = round(point_data[3], 1)
        projected_mean = round(point_data[4], 1)
        projected_min = round(point_data[5], 1)

        point_pkg["projected"] = {}
        point_pkg["projected"]["ddmin"] = round(projected_min)
        point_pkg["projected"]["ddmean"] = round(projected_mean)
        point_pkg["projected"]["ddmax"] = round(projected_max)
    else:
        for mi, v_li in enumerate(point_data):  # (nested list with model at dim 0)
            if mi == 0:
                min_year = years_lu["historical"]["min"]
                max_year = years_lu["historical"]["max"]
                years = range(min_year, max_year + 1)
            else:
                min_year = years_lu["projected"]["min"]
                max_year = years_lu["projected"]["max"]
                years = range(min_year, max_year + 1)

            model = dd_dim_encodings["model"][mi]
            point_pkg[model] = {}

            # Responses from Rasdaman include the same array length for both
            # historical and projected data, representing every possible year
            # (1979-2100). This means both the historical and projected data
            # arrays include nodata years populated with 0s. The code below
            # omits nodata gaps and makes sure the correct year is assigned to
            # its corresponding data in the historical and projected data
            # arrays.
            year = years_lu["historical"]["min"]
            year_index = 0
            if None in [start_year, end_year]:
                start_year = years_lu["historical"]["min"]
                end_year = years_lu["projected"]["max"]
            for value in v_li:
                if year in years:
                    if year >= int(start_year) and year <= int(end_year):
                        point_pkg[model][years[year_index]] = {"dd": round(value)}
                    year_index += 1
                year += 1

    return point_pkg


async def fetch_dd_point_data(x, y, cov_id, start_year=None, end_year=None):
    """Run the async degree days data request for a single point
    and return data as json

    Args:
        lat (float): latitude
        lon (float): longitude
        cov_id (str): heating_degree_days, degree_days_below_zero,
            thawing_index, or freezing_index
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    point_data_list = []
    if request.args.get("summarize") == "mmm":
        min_year = years_lu["historical"]["min"]
        max_year = years_lu["historical"]["max"]
        timestring = (
            f'"{min_year}-01-01T00:00:00.000Z":"{max_year}-01-01T00:00:00.000Z"'
        )
        if start_year is not None:
            timestring = (
                f'"{start_year}-01-01T00:00:00.000Z":"{end_year}-01-01T00:00:00.000Z"'
            )
        for tempstat in range(0, 3):
            request_str = get_dd_wcps_request_str(
                x, y, cov_id, "0:0", timestring, tempstat
            )
            point_data_list.append(
                await fetch_data([generate_wcs_query_url(request_str)])
            )

        min_year = years_lu["projected"]["min"]
        max_year = years_lu["projected"]["max"]
        timestring = (
            f'"{min_year}-01-01T00:00:00.000Z":"{max_year}-01-01T00:00:00.000Z"'
        )
        if start_year is not None:
            timestring = (
                f'"{start_year}-01-01T00:00:00.000Z":"{end_year}-01-01T00:00:00.000Z"'
            )
        for tempstat in range(0, 3):
            request_str = get_dd_wcps_request_str(
                x, y, cov_id, "1:2", timestring, tempstat
            )
            point_data_list.append(
                await fetch_data([generate_wcs_query_url(request_str)])
            )
    else:
        request_str = generate_wcs_getcov_str(x, y, cov_id)
        point_data_list = await fetch_data([generate_wcs_query_url(request_str)])
    return point_data_list


@routes.route("/degree_days/<var_ep>/<lat>/<lon>")
@routes.route("/degree_days/<var_ep>/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_dd_point_data(var_ep, lat, lon, start_year=None, end_year=None):
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

    point_pkg = package_dd_point_data(point_data_list, start_year, end_year)

    if request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(point_pkg, cov_id_str)
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        if request.args.get("summarize") == "mmm":
            return create_csv(point_pkg, cov_id_str, lat=lat, lon=lon)
        else:
            return create_csv(point_pkg, cov_id_str + "_all", lat=lat, lon=lon)

    return postprocess(point_pkg, cov_id_str)


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
