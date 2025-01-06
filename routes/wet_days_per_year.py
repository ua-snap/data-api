import asyncio
import ast

from flask import (
    Blueprint,
    render_template,
    request,
    jsonify,
)
from urllib.parse import quote

# local imports
from generate_urls import generate_wcs_query_url
from fetch_data import (
    fetch_data,
    generate_wcs_getcov_str,
    describe_via_wcps,
)
from validate_request import (
    validate_latlon,
    project_latlon,
    get_coverage_encodings,
)
from csv_functions import create_csv
from postprocessing import (
    nullify_and_prune,
    postprocess,
)
from config import WEST_BBOX, EAST_BBOX
from . import routes

wet_days_per_year_api = Blueprint("wet_days_per_year_api", __name__)


async def get_wet_days_metadata():
    """Get the coverage metadata and encodings for wet days per year coverage"""
    metadata = await describe_via_wcps("wet_days_per_year")
    return get_coverage_encodings(metadata)


wet_days_per_year_dim_encodings = asyncio.run(get_wet_days_metadata())
print(wet_days_per_year_dim_encodings)

# default to min-max temporal range of coverage
years_lu = {
    "historical": {"min": 1980, "max": 2009},
    "projected": {"min": 2006, "max": 2100},
}


def generate_wcps_request_str(
    x, y, cov_id, models, years, summary_stat, encoding="json"
):
    """Generates a WCPS query specific to the
    coverages used for the min-mean-max summaries over specific axes combinations..

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
        summary_stat(int): Integer between 0-2 where:
            - 0 = mean summary
            - 1 = maximum summary
            - 2 = minimum summary
        encoding (str): currently supports either "json" or "netcdf"
            for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """
    if summary_stat == 0:
        operation = "max"
    elif summary_stat == 2:
        operation = "min"
    else:
        operation = "+"

    if summary_stat == 0 or summary_stat == 2:
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


def validate_years(horp, start_year, end_year):
    # CP: function could maybe generalized a bit and moved to `validate_request` at some point
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


def package_wet_days_per_year_point_data(point_data, horp):
    """Package JSON response data for wet_days_per_year

    Args:
        point_data (list): nested list containing JSON results of WCPS query
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

            # rasdaman returns the model encodings as strings which is a mess we unravel here
            model_ = ast.literal_eval(wet_days_per_year_dim_encodings["model"])
            model_name = model_[str(mi)]
            point_pkg[model_name] = {}

            # Responses from Rasdaman include the same array length for both
            # historical and projected data, representing every possible year
            # (1980-2100). This means both the historical and projected data
            # arrays include nodata years populated with 0s. The code below
            # omits nodata gaps and makes sure the correct year is assigned to
            # its corresponding data in the historical and projected data
            # arrays.
            year = years_lu["historical"]["min"]
            year_index = 0
            for value in v_li:
                if year in years:
                    point_pkg[model_name][years[year_index]] = {"wdpy": round(value)}
                    year_index += 1
                year += 1
    else:
        if horp in ["historical", "hp"]:
            historical_max = round(point_data[0], 1)
            historical_mean = round(point_data[1], 1)
            historical_min = round(point_data[2], 1)

            point_pkg["historical"] = {}
            point_pkg["historical"]["wdpymin"] = round(historical_min)
            point_pkg["historical"]["wdpymean"] = round(historical_mean)
            point_pkg["historical"]["wdpymax"] = round(historical_max)

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
            point_pkg["projected"]["wdpymin"] = round(projected_min)
            point_pkg["projected"]["wdpymean"] = round(projected_mean)
            point_pkg["projected"]["wdpymax"] = round(projected_max)

    return point_pkg


async def fetch_wet_days_per_year_point_data(x, y, horp, start_year, end_year):
    """Run the async degree days data request for a single point
    and return data as json

    Args:
        lat (float): latitude
        lon (float): longitude
        horp [Historical or Projected] (str): historical, projected, hp, or all
        start_year (int): start year for WCPS query or None
        end_year (int): end year for WCPS query or None

    Returns:
        JSON-like dict of data at provided latitude and longitude
    """
    point_data_list = []
    if horp == "all":
        request_str = generate_wcs_getcov_str(x, y, "wet_days_per_year")
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
        for summary_stat in range(0, 3):
            request_str = generate_wcps_request_str(
                x, y, "wet_days_per_year", "0:0", timestring, summary_stat
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
        for summary_stat in range(0, 3):
            request_str = generate_wcps_request_str(
                x, y, "wet_days_per_year", "1:2", timestring, summary_stat
            )
            point_data_list.append(
                await fetch_data([generate_wcs_query_url(request_str)])
            )

    return point_data_list


# CP: consider if routing synonyms (top-level and mmm-nested) make sense for this endpoint. another alternative could be /precip_inidcators/wet_days_per_year...
@routes.route("/wet_days_per_year/")
@routes.route("/wet_days_per_year/abstract/")
@routes.route("/mmm/wet_days_per_year/")
@routes.route("/mmm/wet_days_per_year/abstract/")
@routes.route("/mmm/wet_days_per_year/point")
@routes.route("/wet_days_per_year/point")
def wet_days_per_year_about():
    return render_template("/documentation/wet_days_per_year.html")


@routes.route("/wet_days_per_year/<horp>/point/<lat>/<lon>")
@routes.route("/wet_days_per_year/<horp>/point/<lat>/<lon>/<start_year>/<end_year>")
@routes.route("/mmm/wet_days_per_year/<horp>/point/<lat>/<lon>")
@routes.route("/mmm/wet_days_per_year/<horp>/point/<lat>/<lon>/<start_year>/<end_year>")
def run_fetch_wet_days_per_year_point_data(
    lat, lon, horp, start_year=None, end_year=None
):
    """Wet Days Per Year data endpoint. Fetch point data for
    specified lat/lon and return JSON-like dict.

    Args:
        lat (float): latitude
        lon (float): longitude
        horp [Historical or Projected] (str): historical, projected, hp, or all
        start_year (int): optional start year for WCPS query
        end_year (int): optional end year for WCPS query

    Returns:
        JSON-like dict of requested degree days data

    Notes:
        example request: http://localhost:5000/mmm/wet_days_per_year/all/65/-147
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

    try:
        point_data_list = asyncio.run(
            fetch_wet_days_per_year_point_data(x, y, horp, start_year, end_year)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    point_pkg = package_wet_days_per_year_point_data(point_data_list, horp)

    if request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(point_pkg, "wet_days_per_year")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        if horp != "all":
            return create_csv(
                point_pkg,
                "wet_days_per_year",
                lat=lat,
                lon=lon,
                start_year=start_year,
                end_year=end_year,
            )
        else:
            return create_csv(
                point_pkg,
                "wet_days_per_year_all",
                lat=lat,
                lon=lon,
                start_year=start_year,
                end_year=end_year,
            )

    return postprocess(point_pkg, "wet_days_per_year")


@routes.route("/eds/wet_days_per_year/point/<lat>/<lon>")
def get_wet_days_per_year_plate(lat, lon):
    """
    Endpoint for requesting all data required for the Wet Days Per Year plate in the
    Arctic-EDS client.
    Args:
        lat (float): latitude
        lon (float): longitude
    Notes:
        example request: http://localhost:5000/eds/wet_days_per_year/point/65.0628/-146.1627
    """
    wdpy_plate = {}

    results = run_fetch_wet_days_per_year_point_data(lat, lon, "historical")
    if isinstance(results, tuple):
        return results
    wdpy_plate["historical"] = results["historical"]

    results = run_fetch_wet_days_per_year_point_data(
        lat, lon, "projected", start_year="2010", end_year="2039"
    )
    if isinstance(results, tuple):
        return results
    wdpy_plate["2010-2039"] = results["projected"]

    results = run_fetch_wet_days_per_year_point_data(
        lat, lon, "projected", start_year="2040", end_year="2069"
    )
    if isinstance(results, tuple):
        return results
    wdpy_plate["2040-2069"] = results["projected"]

    results = run_fetch_wet_days_per_year_point_data(
        lat, lon, "projected", start_year="2070", end_year="2099"
    )
    if isinstance(results, tuple):
        return results
    wdpy_plate["2070-2099"] = results["projected"]

    return jsonify(wdpy_plate)
