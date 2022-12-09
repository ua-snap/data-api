import asyncio
import pandas as pd
from urllib.parse import quote
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
    nullify_nodata,
    nullify_and_prune,
    postprocess,
    place_name_and_type,
)
from . import routes
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from luts import permafrost_encodings  # for the Melvin 4 km (NCR) data

gipl1km_dim_encodings = asyncio.run(
    get_dim_encodings("crrel_gipl_outputs", scrape=("time", "gmlrgrid:coefficients", 4))
)

permafrost_api = Blueprint("permafrost_api", __name__)

# rasdaman targets
permafrost_coverage_id = "iem_gipl_magt_alt_4km"
gipl_1km_coverage_id = "crrel_gipl_outputs"


# geoserver targets
wms_targets = [
    "obu_2018_magt",
]
wfs_targets = {
    "jorgenson_2008_pf_extent_ground_ice_volume": "GROUNDICEV,PERMAFROST",
    "obu_pf_extent": "PFEXTENT",
}

titles = {
    "gipl": "Melvin et al. (2017) GIPL 2.0 Mean Annual Ground Temperature (°C) and Active Layer Thickness (m) 4 km Model Output",
    "gipl1km": "GIPL 2.0 Mean Annual Ground Temperature (°C), Permafrost Base, Permafrost Top, and Talik Thickness (m) 1 km Model Output",
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
    titles[
        "obu_magt"
    ] = f"Obu et al. (2018) {year} Mean Annual {depth} Ground Temperature (°C)"
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


def generate_gipl1km_time_index():
    """Generate a Pythonic time index for annual GIPL 1km outputs.

    Returns:
        dt_range (pandas DatetimeIndex): a time index with annual frequency
    """
    timestamps = [x[1:-2] for x in gipl1km_dim_encodings["time"].split(" ")]
    date_index = pd.DatetimeIndex(timestamps)
    return date_index


def package_gipl1km_point_data(gipl1km_point_resp):
    """Package the response. The native structure of the response is nested as follows: model (0 1 2), year, scenario (0 1), variable (0 9)."""

    flat_list = list(deepflatten(gipl1km_point_resp))
    i = 0

    gipl1km_point_pkg = {}
    for model_name in gipl1km_dim_encodings["model"].values():
        gipl1km_point_pkg[model_name] = {}
        for t in generate_gipl1km_time_index():
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


def create_gipl1km_csv(data_pkg, lat=None, lon=None):
    """Create CSV file with metadata string and location based filename.
    Args:
        data_pkg (dict): JSON-like object of data
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons
    Returns:
        CSV response object
    """
    fieldnames = [
        "model",
        "year",
        "scenario",
        "variable",
        "value",
    ]
    csv_dicts = build_csv_dicts(
        data_pkg,
        fieldnames,
    )
    metadata = "# GIPL model outputs for mean annual ground temperature (°C) at various depths below the surface as well as talik thickness, and depth of permafrost base and top (m)\n"
    filename = "GIPL 1 km Model Outputs" + " for " + lat + ", " + lon + ".csv"

    return write_csv(csv_dicts, fieldnames, filename, metadata)


def package_gipl(gipl_resp):
    """Package GIPL MAGT and ALT netCDF data.
    The response is a nested list object."""
    eras = list(permafrost_encodings["eras"].values())
    models = list(permafrost_encodings["models"].values())
    scenarios = list(permafrost_encodings["scenarios"].values())
    varnames = permafrost_encodings["gipl_varnames"]

    # Flatten this response (twice)
    flattened_resp = sum(sum(gipl_resp, []), [])

    # Initialize dict structure
    di = {
        era: {
            m: {sc: {var: "value" for var in varnames} for sc in scenarios}
            for m in models
        }
        for era in eras
    }

    i = 0
    for era in di.keys():
        for model in di[era].keys():
            for scenario in di[era][model]:
                values = flattened_resp[i].split(" ")
                magt_value = round(float(values[0]), 1)
                alt_value = float(values[1])
                di[era][model][scenario]["magt"] = magt_value
                di[era][model][scenario]["alt"] = alt_value
                i += 1
    # This block drops all the invalid dimensional combinations that are a result of jamming historical and projected data into the same data cube. These are no data values (-9999) that should be culled.
    models.remove("cruts31")
    for model in models:
        di["1995"].pop(model, None)
    di["1995"]["cruts31"].pop("rcp45", None)
    di["1995"]["cruts31"].pop("rcp85", None)
    for k in ["2025", "2050", "2075", "2095"]:
        di[k].pop("cruts31", None)
        for m in di[k].keys():
            di[k][m].pop("historical", None)
    return di


def package_gipl_polygon(gipl_polygon_resp):
    """Package a single data variable (GIPL MAGT or ALT)."""
    di = gipl_polygon_resp
    eras = list(permafrost_encodings["eras"].values())
    models = list(permafrost_encodings["models"].values())
    scenarios = list(permafrost_encodings["scenarios"].values())
    models.remove("cruts31")
    for model in models:
        di["1995"].pop(model, None)
    di["1995"]["cruts31"].pop("rcp45", None)
    di["1995"]["cruts31"].pop("rcp85", None)
    for k in ["2025", "2050", "2075", "2095"]:
        di[k].pop("cruts31", None)
        for m in di[k].keys():
            di[k][m].pop("historical", None)
    return di


def combine_gipl_poly_var_pkgs(magt_di, alt_di):

    combined_gipl_di = {}
    for era in magt_di.keys():
        combined_gipl_di[era] = {}
        for model in magt_di[era].keys():
            combined_gipl_di[era][model] = {}
            for scenario in magt_di[era][model].keys():
                combined_gipl_di[era][model][scenario] = {}
                combined_gipl_di[era][model][scenario]["magt"] = magt_di[era][model][
                    scenario
                ]
                combined_gipl_di[era][model][scenario]["alt"] = alt_di[era][model][
                    scenario
                ]
                combined_gipl_di[era][model][scenario]["statistic"] = "Zonal Mean"
    return combined_gipl_di


@routes.route("/permafrost/")
@routes.route("/permafrost/abstract/")
def pf_about():
    return render_template("permafrost/abstract.html")


@routes.route("/permafrost/point/")
def pf_about_point():
    return render_template("permafrost/point.html")


@routes.route("/permafrost/point/gipl/<lat>/<lon>")
def run_fetch_gipl_1km_point_data(lat, lon):
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
    try:
        gipl_1km_point_data = asyncio.run(fetch_gipl_1km_point_data(x, y))
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    gipl_1km_point_package = package_gipl1km_point_data(gipl_1km_point_data)
    if request.args.get("format") == "csv":
        point_pkg = nullify_and_prune(gipl_1km_point_package, "crrel_gipl")
        if point_pkg in [{}, None, 0]:
            return render_template("404/no_data.html"), 404
        return create_gipl1km_csv(point_pkg, lat=lat, lon=lon)
    return postprocess(gipl_1km_point_package, "crrel_gipl")


@routes.route("/permafrost/point/<lat>/<lon>")
def run_point_fetch_all_permafrost(lat, lon):
    """Run the async request for permafrost data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of permafrost data
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

    gs_results = asyncio.run(
        fetch_data_api(
            GS_BASE_URL, "permafrost_beta", wms_targets, wfs_targets, lat, lon
        )
    )

    x, y = project_latlon(lat, lon, 3338)

    try:
        rasdaman_results = asyncio.run(
            fetch_wcs_point_data(x, y, permafrost_coverage_id)
        )
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500

    data = {
        "gipl": package_gipl(rasdaman_results),
        "obu_magt": package_obu_magt(gs_results[0]),
        "jorg": package_jorgenson(gs_results[1]),
        "obupfx": package_obu_vector(gs_results[2]),
    }

    csv_dicts = []
    if request.args.get("format") == "csv":
        data = nullify_and_prune(data, "permafrost")
        if data in [{}, None, 0]:
            return render_template("404/no_data.html"), 404

        fieldnames = [
            "source",
            "era",
            "model",
            "scenario",
            "variable",
            "value",
        ]

        gipl_data = {"gipl": data["gipl"]}
        csv_dicts += build_csv_dicts(
            gipl_data,
            fieldnames[0:-1],
        )

        # Non-GIPL values have a simpler nesting structure and need to be
        # handled separately.
        non_gipl_fields = [
            "source",
            "variable",
            "value",
        ]
        for source in ["jorg", "obu_magt", "obupfx"]:
            subset = {source: data[source]}
            csv_dicts += build_csv_dicts(
                subset,
                non_gipl_fields,
            )

        place_id = request.args.get("community")
        place_name, place_type = place_name_and_type(place_id)

        metadata = csv_metadata(place_name, place_id, place_type, lat, lon)
        metadata += "# alt is the active layer thickness in meters\n"
        metadata += "# magt is the mean annual ground temperature in degrees Celsius\n"
        metadata += "# ice is the estimated ground ice volume\n"
        metadata += "# pfx is the permafrost extent\n"
        metadata += "# 2025 represents 2011 - 2040\n"
        metadata += "# 2050 represents 2036 - 2065\n"
        metadata += "# 2075 represents 2061 – 2090\n"
        metadata += "# 2095 represents 2086 – 2100\n"

        metadata += "# gipl is the Geophysical Institute's Permafrost Laboratory\n"
        for source in ["gipl", "jorg", "obu_magt", "obupfx"]:
            metadata += "# " + titles[source] + "\n"

        if place_name is not None:
            filename = "Permafrost for " + quote(place_name) + ".csv"
        else:
            filename = "Permafrost for " + lat + ", " + lon + ".csv"

        return write_csv(csv_dicts, fieldnames, filename, metadata)

    return postprocess(data, "permafrost", titles)


async def fetch_gipl_1km_point_data(x, y):
    gipl_request_str = generate_wcs_getcov_str(x, y, "crrel_gipl_outputs")
    gipl_point_data = await fetch_data([generate_wcs_query_url(gipl_request_str)])
    return gipl_point_data
