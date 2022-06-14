import asyncio
import io
import csv
import copy
from urllib.parse import quote
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
)

# local imports
from fetch_data import (
    fetch_data_api,
    fetch_wcs_point_data,
    fetch_bbox_netcdf,
    summarize_within_poly,
    build_csv_dicts,
    write_csv,
    add_titles,
    csv_metadata,
)

from generate_requests import generate_netcdf_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from validate_request import (
    validate_latlon,
    project_latlon,
)
from validate_data import (
    get_poly_3338_bbox,
    nullify_nodata,
    nullify_and_prune,
    postprocess,
    place_name_and_type,
)
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from luts import huc_gdf, permafrost_encodings, akpa_gdf
from . import routes

permafrost_api = Blueprint("permafrost_api", __name__)

# rasdaman targets
permafrost_coverage_id = "iem_gipl_magt_alt_4km"

# geoserver targets
wms_targets = [
    "obu_2018_magt",
]
wfs_targets = {
    "jorgenson_2008_pf_extent_ground_ice_volume": "GROUNDICEV,PERMAFROST",
    "obu_pf_extent": "PFEXTENT",
}

titles = {
    "gipl": "Melvin et al. (2017) GIPL 2.0 Mean Annual Ground Temperature (°C) and Active Layer Thickness (m) Model Output",
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
@routes.route("/groundtemperature/")
@routes.route("/groundtemperature/abstract/")
@routes.route("/activelayer/")
@routes.route("/activelayer/abstract/")
@routes.route("/magtalt/")
@routes.route("/magtalt/abstract/")
@routes.route("/magtalt/")
@routes.route("/magtalt/abstract/")
@routes.route("/alt/")
@routes.route("/alt/abstract/")
@routes.route("/magt/")
@routes.route("/magt/abstract/")
def pf_about():
    return render_template("permafrost/abstract.html")


@routes.route("/permafrost/point/")
@routes.route("/groundtemperature/point/")
@routes.route("/activelayer/point/")
@routes.route("/magtalt/point/")
@routes.route("/alt/point/")
@routes.route("/magt/point/")
def pf_about_point():
    return render_template("permafrost/point.html")


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
