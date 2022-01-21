import asyncio
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)

# local imports
from fetch_data import (
    fetch_data,
    fetch_data_api,
    fetch_wcs_point_data,
    fetch_bbox_netcdf,
    summarize_within_poly,
)
from generate_requests import generate_wcs_getcov_str, generate_netcdf_wcs_getcov_str
from generate_urls import generate_wcs_query_url
from validate_latlon import validate, project_latlon
from validate_data import (
    check_for_nodata,
    get_poly_3338_bbox,
    nodata_message,
)
from config import GS_BASE_URL
from luts import huc8_gdf, permafrost_encodings, akpa_gdf
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

# packaging functions unique to each query
def package_obu_magt(obu_magt_resp):
    """Package Obu MAGT raster data."""
    ds_title = "Obu et al. (2018) Mean Annual Ground Temperature (°C)"
    if obu_magt_resp["features"] == []:
        di = {"title": ds_title, "Data Status": nodata_message}
    else:
        depth = "Top of Permafrost"
        year = "2000-2016"
        title = f"Obu et al. (2018) {year} Mean Annual {depth} Ground Temperature (°C)"
        temp = obu_magt_resp["features"][0]["properties"]["GRAY_INDEX"]
        temp = round(temp, 1)

        di = {"title": title, "year": year, "depth": depth, "temp": temp}
        check_for_nodata(di, "temp", temp, -9999)
    return di


def package_jorgenson(jorgenson_resp):
    """Package Jorgenson vector data."""
    title = "Jorgenson et al. (2008) Permafrost Extent and Ground Ice Volume"
    if jorgenson_resp["features"] == []:
        di = {"title": title, "Data Status": nodata_message}
    else:
        ice = jorgenson_resp["features"][0]["properties"]["GROUNDICEV"]
        pfx = jorgenson_resp["features"][0]["properties"]["PERMAFROST"]
        di = {"title": title, "ice": ice, "pfx": pfx}
    return di


def package_obu_vector(obu_vector_resp):
    """Package Obu permafrost extent vector data."""
    title = "Obu et al. (2018) Permafrost Extent"
    if obu_vector_resp["features"] == []:
        di = {"title": title, "Data Status": nodata_message}
    else:
        pfx = obu_vector_resp["features"][0]["properties"]["PFEXTENT"]
        di = {"title": title, "pfx": pfx}
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
                di[era][model][scenario]["magt"] = float(
                    flattened_resp[i].split(" ")[0]
                )
                di[era][model][scenario]["alt"] = float(flattened_resp[i].split(" ")[1])
                di[era][model][scenario][
                    "title"
                ] = "Melvin et al. (2017) GIPL 2.0 Mean Annual Ground Temperature (°C) and Active Layer Thickness (m) Model Output"
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
                combined_gipl_di[era][model][scenario][
                    "title"
                ] = "Melvin et al. (2017) GIPL 2.0 Mean Annual Ground Temperature (°C) and Active Layer Thickness (m) Model Output"
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


@routes.route("/permafrost/huc/")
@routes.route("/groundtemperature/huc/")
@routes.route("/activelayer/huc/")
@routes.route("/magtalt/huc/")
@routes.route("/alt/huc/")
@routes.route("/magt/huc/")
def pf_about_huc():
    return render_template("permafrost/huc.html")


@routes.route("/permafrost/protectedarea/")
@routes.route("/groundtemperature/protectedarea/")
@routes.route("/activelayer/protectedarea/")
@routes.route("/magtalt/protectedarea/")
@routes.route("/alt/protectedarea/")
@routes.route("/magt/protectedarea/")
def pf_about_protectedarea():
    return render_template("permafrost/protectedarea.html")


@routes.route("/permafrost/point/<lat>/<lon>")
def run_point_fetch_all_permafrost(lat, lon):
    """Run the async request for permafrost data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of permafrost data
    """
    if not validate(lat, lon):
        abort(400)

    gs_results = asyncio.run(
        fetch_data_api(
            GS_BASE_URL, "permafrost_beta", wms_targets, wfs_targets, lat, lon
        )
    )

    x, y = project_latlon(lat, lon, 3338)
    rasdaman_results = asyncio.run(fetch_wcs_point_data(x, y, permafrost_coverage_id))

    gipl = package_gipl(rasdaman_results)
    obu_magt = package_obu_magt(gs_results[0])
    jorg = package_jorgenson(gs_results[1])
    obu_pfx = package_obu_vector(gs_results[2])
    data = {
        "gipl": gipl,
        "obu_magt": obu_magt,
        "obupfx": obu_pfx,
        "jorg": jorg,
    }
    return data


@routes.route("/permafrost/huc/<huc_id>")
def run_huc_fetch_all_permafrost(huc_id):
    """Endpoint to fetch GIPL data within a HUC.

    Args: huc_id (int): 8-digit HUC ID.

    Returns:
        huc_pkg (dict): JSON-like object containing aggregated permafrost data.
    """
    poly = get_poly_3338_bbox(huc8_gdf, huc_id)
    bounds = poly.bounds

    request_str = generate_netcdf_wcs_getcov_str(
        bounds, permafrost_coverage_id, var_coord=None
    )
    url = generate_wcs_query_url(request_str)
    ds = asyncio.run(fetch_bbox_netcdf([url]))

    alt_poly_sum_di = summarize_within_poly(
        ds, poly, permafrost_encodings, varname="magt"
    )
    magt_poly_sum_di = summarize_within_poly(
        ds, poly, permafrost_encodings, varname="alt"
    )
    magt_huc_pkg = package_gipl_polygon(magt_poly_sum_di)
    alt_huc_pkg = package_gipl_polygon(alt_poly_sum_di)
    combined_pkg = combine_gipl_poly_var_pkgs(magt_huc_pkg, alt_huc_pkg)
    return combined_pkg


@routes.route("/permafrost/protectedarea/<akpa_id>")
def run_protectedarea_fetch_all_permafrost(akpa_id):
    """Endpoint to fetch GIPL data within a protected area.

    Args: akpa_id (str): ID of protected area, e.g. "NPS7"

    Returns:
        huc_pkg (dict): JSON-like object containing aggregated permafrost data.
    """
    poly = get_poly_3338_bbox(akpa_gdf, akpa_id)
    bounds = poly.bounds

    request_str = generate_netcdf_wcs_getcov_str(
        bounds, permafrost_coverage_id, var_coord=None
    )
    url = generate_wcs_query_url(request_str)
    ds = asyncio.run(fetch_bbox_netcdf([url]))

    alt_poly_sum_di = summarize_within_poly(
        ds, poly, permafrost_encodings, varname="magt"
    )
    magt_poly_sum_di = summarize_within_poly(
        ds, poly, permafrost_encodings, varname="alt"
    )
    magt_huc_pkg = package_gipl_polygon(magt_poly_sum_di)
    alt_huc_pkg = package_gipl_polygon(alt_poly_sum_di)
    combined_pkg = combine_gipl_poly_var_pkgs(magt_huc_pkg, alt_huc_pkg)
    return combined_pkg
