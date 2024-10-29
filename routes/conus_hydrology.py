import requests
import io
import xarray as xr
import xml.etree.ElementTree as ET
import json
import xml.etree.ElementTree as ET
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)

from generate_requests import generate_conus_hydrology_wcs_str
from config import RAS_BASE_URL
from . import routes

cov_id = "conus_hydro_segments_crstephenson"
# TODO: change this to 'Rasdaman Encoding' once coverage is updated
encoding_attr = "Encoding"


def fetch_hydrology_data(
    cov_id,
    encoding_attr,
    geom_id,
    lc,
    model,
    scenario,
    era,
    vars,
):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one geometry ID at a time!
    The lc, model, scenario, era and vars parameters are all optional; omitting any of these will not slice the datacube
    in that dimension and will return all data. String parameters (except vars) are encoded to integers before the WCS request.
    Args:
        coverage_id (str): Coverage ID for the hydrology data
        encoding_attr (str): Attribute name that holds dictionary of Rasdaman encodings
        geom_id (int): Geometry ID for the hydrology data
        lc (str): Land cover type (dynamic or static)
        model (str): Model name (e.g. CCSM4)
        scenario (str): Scenario name (e.g. historical)
        era (str): Era name (e.g. 1976_2005)
        vars (list): a list of variable names (e.g. ['dh3', 'dh15'])

    Returns:
        Xarray dataset with hydrological stats for the requested var/lc/model/scenario/era combination
    """
    lc_, model_, scenario_, era_ = encode_parameters(
        cov_id, encoding_attr, lc, model, scenario, era
    )
    # TODO: use RAS_BASE_URL config env variable instead of hardcoded URL
    url = RAS_BASE_URL + generate_conus_hydrology_wcs_str(
        cov_id, geom_id, lc_, model_, scenario_, era_, vars
    )

    print(url)

    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        ds = xr.open_dataset(io.BytesIO(r.content))

    return ds


def encode_parameters(cov_id, encoding_attr, lc, model, scenario, era):
    """
    Function to encode the parameters for the Rasdaman request.
    Searches the XML response from the DescribeCoverage request for the encodings metadata and
    returns the dictionary of encodings. Encodes the input parameters to integers for the WCS request.
    Args:
        cov_id (str): Coverage ID for the hydrology data
        encoding_attr (str): Attribute name that holds dictionary of Rasdaman encodings
        lc (str): Land cover type (dynamic or static)
        model (str): Model name (e.g. CCSM4)
        scenario (str): Scenario name (e.g. historical)
        era (str): Era name (e.g. 1976_2005)
    Returns:
        Tuple of encoded parameters (integers) for Rasdaman request"""

    url = f"https://zeus.snap.uaf.edu/rasdaman/ows?&SERVICE=WCS&VERSION=2.1.0&REQUEST=DescribeCoverage&COVERAGEID={cov_id}&outputType=GeneralGridCoverage"
    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        tree = ET.ElementTree(ET.fromstring(r.content))

    xml_search_string = str(".//{http://www.rasdaman.org}" + encoding_attr)
    encoding_dict_str = tree.findall(xml_search_string)[0].text
    encoding_dict = eval(encoding_dict_str)

    if lc is not None:
        lc_ = encoding_dict["lc"][lc]
    else:
        lc_ = None

    if model is not None:
        model_ = encoding_dict["model"][model]
    else:
        model_ = None

    if scenario is not None:
        scenario_ = encoding_dict["scenario"][scenario]
    else:
        scenario_ = None

    if era is not None:
        era_ = encoding_dict["era"][era]
    else:
        era_ = None

    return lc_, model_, scenario_, era_


@routes.route("/conus_hydrology/")
def conus_hydrology_about():
    return render_template("/documentation/conus_hydrology.html")


@routes.route("/conus_hydrology/<geom_id>")
def run_get_conus_hydrology_point_data(
    geom_id, lc=None, model=None, scenario=None, era=None, vars=None
):
    """
    Function to fetch hydrology data from Rasdaman for a single geometry ID.
    Additional reguest arguments can be made for land cover type, model, scenario, era, and variables.
    For example: /conus_hydrology/12345?lc=dynamic&model=CCSM4&scenario=historical&era=1976_2005&vars=dh3,dh15
    Args:
        geom_id (str): Geometry ID for the hydrology data
    Returns:
        JSON response with hydrological stats for the requested geom ID.
    """

    # sort through request arguments and assign to variables, if they exist
    if request.args.get("lc"):
        lc = request.args.get("lc")

    if request.args.get("model"):
        model = request.args.get("model")

    if request.args.get("scenario"):
        scenario = request.args.get("scenario")

    if request.args.get("era"):
        era = request.args.get("era")

    if request.args.get("vars"):
        if len(request.args.get("vars").split(",")) > 1:
            vars = request.args.get("vars").split(",")
        else:
            vars = [request.args.get("vars")]

    ds = fetch_hydrology_data(
        cov_id, encoding_attr, geom_id, lc, model, scenario, era, vars
    )

    # save nc to test size of return
    ds.to_netcdf("/home/jdpaul3/stats_from_geom_id.nc", engine="h5netcdf")

    # # populate the stats in the data dictionary with the hydrology statistics
    # huc6_data_dict = populate_stats(huc6_data_dict, stats_ds, lc, model, scenario, era)

    # # return Flask JSON Response
    # json_results = json.dumps(huc6_data_dict, indent=4)

    # # save json to test size of return
    # with open("/home/jdpaul3/result.json", "w", encoding="utf-8") as f:
    #     json.dump(json_results, f, ensure_ascii=False, indent=4)

    return "done!"
