import asyncio
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from generate_urls import generate_wcs_query_url
from fetch_data import *
from validate_request import (
    validate_latlon,
    project_latlon,
)
from validate_data import (
    nullify_and_prune,
    postprocess,
)
from config import WEST_BBOX, EAST_BBOX
from . import routes

degree_days_api = Blueprint("degree_days_api", __name__)

try:
    # The heating_degree_days, degree_days_below_zero, thawing_index, and
    # freezing_index coverages all share the same dim_encodings
    dim_encodings = asyncio.run(get_dim_encodings("heating_degree_days"))
except:
    print("Missing from Apollo")

var_ep_lu = {
    "heating": {"cov_id_str": "heating_degree_days"},
    "below_zero": {"cov_id_str": "degree_days_below_zero"},
    "thawing_index": {"cov_id_str": "thawing_index"},
    "freezing_index": {"cov_id_str": "freezing_index"},
}

var_label_lu = {
    "heating": "Heating Degree Days",
    "below_zero": "Degree Days Below Zero",
    "thawing_index": "Thawing Index",
    "freezing_index": "Freezing Index",
}

years_lu = {
    "historical": {"min": 1979, "max": 2015},
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


@routes.route("/mmm/degree_days/")
@routes.route("/mmm/degree_days/abstract/")
@routes.route("/mmm/degree_days/heating/")
@routes.route("/mmm/degree_days/below_zero/")
@routes.route("/mmm/degree_days/thawing_index/")
@routes.route("/mmm/degree_days/freezing_index/")
def heating_degree_days_about():
    return render_template("/mmm/degree_days.html")


def package_dd_point_data(point_data, var_ep, horp):
    """Add data for JSON response

    Args:
        point_data (list): nested list containing JSON
            results of WCPS query
        var_ep (str): variable name
        horp [Historical or Projected] (str): historical, projected, hp, or all

    Returns:
        JSON-like dict of query results
    """

    point_pkg = {}
    if horp == "all":
        for mi, v_li in enumerate(point_data):  # (nested list with model at dim 0)
            if mi == 0:
                min_year = years_lu["historical"]["min"]
                max_year = years_lu["historical"]["max"]
                years = range(min_year, max_year + 1)
            else:
                min_year = years_lu["projected"]["min"]
                max_year = years_lu["projected"]["max"]
                years = range(min_year, max_year + 1)

            model = dim_encodings["model"][mi]
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
            for value in v_li:
                if year in years:
                    point_pkg[model][years[year_index]] = {"dd": value}
                    year_index += 1
                year += 1
    else:
        if horp in ["historical", "hp"]:
            historical_max = round(point_data[0], 1)
            historical_mean = round(point_data[1], 1)
            historical_min = round(point_data[2], 1)

            point_pkg["historical"] = {}
            point_pkg["historical"]["ddmin"] = historical_min
            point_pkg["historical"]["ddmean"] = historical_mean
            point_pkg["historical"]["ddmax"] = historical_max

        if horp in ["projected", "hp"]:
            if horp == "projected":
                projected_max = round(point_data[0], 1)
                projected_mean = round(point_data[1], 1)
                projected_min = round(point_data[2], 1)
            else:
                projected_max = round(point_data[3], 1)
                projected_mean = round(point_data[4], 1)
                projected_min = round(point_data[5], 1)

            point_pkg["projected"] = {}
            point_pkg["projected"]["ddmin"] = projected_min
            point_pkg["projected"]["ddmean"] = projected_mean
            point_pkg["projected"]["ddmax"] = projected_max

    return point_pkg


async def fetch_dd_point_data(x, y, cov_id, horp, start_year, end_year):
    """Run the async degree days data request for a single point
    and return data as json

    Args:
        lat (float): latitude
        lon (float): longitude
        cov_id (list): string of heating_degree_days, degree_days_below_zero,
            thawing_index, or freezing_index
        horp [Historical or Projected] (str): historical, projected, hp, or all
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    point_data_list = []
    if horp == "all":
        request_str = generate_wcs_getcov_str(x, y, cov_id)
        point_data_list = await fetch_data([generate_wcs_query_url(request_str)])

    if horp in ["historical", "hp"]:
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

    if horp in ["projected", "hp"]:
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

    return point_data_list


def create_csv(data_pkg, var_ep, place_id=None, lat=None, lon=None):
    """Create CSV file with metadata string and location based filename.
    Args:
        data_pkg (dict): JSON-like object of data
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        place_id: place identifier (e.g., AK124)
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons
    Returns:
        CSV response object
    """

    fieldnames = [
        "model",
        "year",
        "variable",
        "value",
    ]

    csv_dicts = build_csv_dicts(
        data_pkg,
        fieldnames,
    )

    if var_ep == "heating":
        metadata = (
            "# dd is the total annual degree days below 65°F for the specified model\n"
        )
    elif var_ep == "below_zero":
        metadata = (
            "# dd is the total annual degree days below 0°F for the specified model\n"
        )
    elif var_ep == "thawing_index":
        metadata = "# dd is the total annual degree days above freezing for the specified model\n"
    elif var_ep == "freezing_index":
        metadata = "# dd is the total annual degree days below freezing for the specified model\n"

    filename = var_label_lu[var_ep] + " for " + lat + ", " + lon + ".csv"

    return write_csv(csv_dicts, fieldnames, filename, metadata)


@routes.route("/mmm/degree_days/<var_ep>/<horp>/<lat>/<lon>")
@routes.route("/mmm/degree_days/<var_ep>/<horp>/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_dd_point_data(var_ep, lat, lon, horp, start_year=None, end_year=None):
    """Point data endpoint. Fetch point data for
    specified lat/lon and return JSON-like dict.

    Args:
        var_ep (str): heating, below_zero, thawing_index, or freezing_index
        lat (float): latitude
        lon (float): longitude
        horp [Historical or Projected] (str): historical, projected, hp, or all
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested degree days data

    Notes:
        example request: http://localhost:5000/mmm/degree_days/heating/all/65/-147
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
        valid_years = validate_years(horp, int(start_year), int(end_year))
        if valid_years is not True:
            return valid_years

    if var_ep in var_ep_lu.keys():
        cov_id_str = var_ep_lu[var_ep]["cov_id_str"]
        try:
            point_data_list = asyncio.run(
                fetch_dd_point_data(x, y, cov_id_str, horp, start_year, end_year)
            )
        except Exception as exc:
            if hasattr(exc, "status") and exc.status == 404:
                return render_template("404/no_data.html"), 404
            return render_template("500/server_error.html"), 500
    else:
        return render_template("400/bad_request.html"), 400

    point_pkg = package_dd_point_data(point_data_list, var_ep, horp)

    if request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(point_pkg, "degree_days")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        return create_csv(point_pkg, var_ep, None, lat=lat, lon=lon)

    return postprocess(point_pkg, "degree_days")


def validate_years(horp, start_year, end_year):
    """Check provided years against valid ranges for historical vs. projected.

    Args:
        horp [Historical or Projected] (str): historical, projected, hp, or all
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        True if years are valid, otherwise an error page to show valid years
    """
    if None not in [start_year, end_year]:
        if horp == "historical":
            min_year = years_lu["historical"]["min"]
            max_year = years_lu["historical"]["max"]
        elif horp == "projected":
            min_year = years_lu["projected"]["min"]
            max_year = years_lu["projected"]["max"]
        elif horp == "hp":
            min_year = years_lu["historical"]["min"]
            max_year = years_lu["projected"]["max"]

        for year in [start_year, end_year]:
            if year < min_year or year > max_year:
                return (
                    render_template(
                        "422/invalid_year.html", min_year=min_year, max_year=max_year
                    ),
                    422,
                )
    return True
