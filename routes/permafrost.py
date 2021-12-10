import asyncio
import io
import operator
import time
import itertools
from functools import reduce
from urllib.parse import quote
import numpy as np
import geopandas as gpd
import xarray as xr
from rasterstats import zonal_stats
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)

# local imports
from validate_latlon import validate, project_latlon
from config import GS_BASE_URL
from fetch_data import (
    get_wcs_request_str_allvar,
    generate_wcs_query_url,
    check_for_nodata,
    fetch_data,
    fetch_data_api,
)
from . import routes

permafrost_api = Blueprint("permafrost_api", __name__)

# encodings to interpret rasdaman ingest
varnames = ["magt", "alt"]
scenarios = ["historical", "rcp45", "rcp85"]
models = ["cruts31", "gfdlcm3", "gisse2r", "ipslcm5alr", "mricgcm3", "ncarccsm4"]
eras = ["1995", "2025", "2050", "2075", "2095"]
era_starts = ["1986", "2011", "2036", "2061", "2086"]
era_ends = ["2005", "2040", "2065", "2090", "2100"]
units_lu = {"magt": "°C", "alt": "m"}
era_encoding = {"1995": 0, "2025": 1, "2050": 2, "2075": 3, "2095": 4}
model_encoding = {
    "cruts31": 0,
    "gfdlcm3": 1,
    "gisse2r": 2,
    "ipslcm5alr": 3,
    "mricgcm3": 4,
    "ncarccsm4": 5,
}
scenario_encoding = {"historical": 0, "rcp45": 1, "rcp85": 2}

permafrost_coverage_id = "iem_gipl_magt_alt_4km_wms"
wms_targets = [
    "obu_2018_magt",
]
wfs_targets = {
    "jorgenson_2008_pf_extent_ground_ice_volume": "GROUNDICEV,PERMAFROST",
    "obu_pf_extent": "PFEXTENT",
}


async def fetch_wcs_permafrost_point_data(x, y):
    """Create the async request for data at the specified point.

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
    """
    urls = []
    request_str = get_wcs_request_str_allvar(x, y, permafrost_coverage_id)
    url = generate_wcs_query_url(request_str)
    urls.append(url)
    point_data = await fetch_data(urls)
    return point_data


def package_obu_magt(obu_magt_resp):
    """Package Obu MAGT data in dict"""
    ds_title = "Obu et al. (2018) Mean Annual Ground Temperature (deg. C)"
    if obu_magt_resp["features"] == []:
        di = {"title": ds_title, "Data Status": "No data at this location."}
    else:
        depth = "Top of Permafrost"
        year = "2000-2016"
        title = (
            f"Obu et al. (2018) {year} Mean Annual {depth} Ground Temperature (deg. C)"
        )

        temp = round(obu_magt_resp["features"][0]["properties"]["GRAY_INDEX"], 2)
        di = {"title": title, "year": year, "depth": depth, "temp": temp}
        check_for_nodata(di, "temp", temp, -9999)
    return di


def package_jorgenson(jorgenson_resp):
    """Package Jorgenson data"""
    title = "Jorgenson et al. (2008) Permafrost Extent and Ground Ice Volume"
    if jorgenson_resp["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        ice = jorgenson_resp["features"][0]["properties"]["GROUNDICEV"]
        pfx = jorgenson_resp["features"][0]["properties"]["PERMAFROST"]
        di = {"title": title, "ice": ice, "pfx": pfx}
    return di


def package_obu_vector(obu_vector_resp):
    """Package Obu Permafrost Extent Data"""
    title = "Obu et al. (2018) Permafrost Extent"
    if obu_vector_resp["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        pfx = obu_vector_resp["features"][0]["properties"]["PFEXTENT"]
        di = {"title": title, "pfx": pfx}
    return di


def package_gipl(gipl_resp):
    """Package GIPL MAGT and ALT Data"""
    flattened_resp = sum(sum(gipl_resp, []), [])

    # Nested dict output structure
    di = {
        era: {
            m: {sc: {var: "value" for var in varnames} for sc in scenarios}
            for m in models
        }
        for era in eras
    }

    # Mapping the response to the dictionary
    # This doesn't feel quite right yet.
    i = 0
    for era in di.keys():
        for model in di[era].keys():
            for scenario in di[era][model]:
                di[era][model][scenario]["magt"] = float(
                    flattened_resp[i].split(" ")[0]
                )
                di[era][model][scenario]["alt"] = float(flattened_resp[i].split(" ")[1])
                di[era][model][scenario]["title"] = "GIPL 2.0 Model Output"
                i += 1
    return di


@routes.route("/permafrost/")
@routes.route("/permafrost/abstract/")
@routes.route("/groundtemperature/")
@routes.route("/groundtemperature/abstract/")
@routes.route("/activelayer/")
@routes.route("/activelayer/abstract/")
@routes.route("/magtalt/")
@routes.route("/magtalt/abstract/")
def pf_about():
    return render_template("permafrost/abstract.html")


@routes.route("/permafrost/point/")
def pf_about_point():
    return render_template("permafrost/point.html")


@routes.route("/permafrost/point/<lat>/<lon>")
def run_fetch_all_permafrost(lat, lon):
    """Run the async request for permafrost data at a single point.
    Args:
        varname (str): Abbreviation for the variable of interest, either "magt" or "alt"
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
    rasdaman_results = asyncio.run(fetch_wcs_permafrost_point_data(x, y))

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
