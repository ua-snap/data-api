import requests
import io
import xarray as xr
import json
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


def fetch_hydrology_data(cov_id, geom_id):
    """
    Function to fetch hydrology data from Rasdaman. Data is fetched for one geometry ID at a time!
    Args:
        coverage_id (str): Coverage ID for the hydrology data
        geom_id (str): Geometry ID for the hydrology data

    Returns:
        Xarray dataset with hydrological stats for the all var/lc/model/scenario/era combinations for the requested geom ID.
    """

    url = RAS_BASE_URL + generate_conus_hydrology_wcs_str(cov_id, geom_id)
    print(url)

    with requests.get(url, verify=False) as r:
        if r.status_code != 200:
            return render_template("500/server_error.html"), 500
        ds = xr.open_dataset(io.BytesIO(r.content))

    return ds


# might use the code below if we want to request only subsets of the datacube; right now we are requesting the entire contents for the geom ID
# def encode_parameters(cov_id, encoding_attr, lc, model, scenario, era):
#     """
#     Function to encode the parameters for the Rasdaman request.
#     Searches the XML response from the DescribeCoverage request for the encodings metadata and
#     returns the dictionary of encodings. Encodes the input parameters to integers for the WCS request.
#     Args:
#         cov_id (str): Coverage ID for the hydrology data
#         encoding_attr (str): Attribute name that holds dictionary of Rasdaman encodings
#         lc (str): Land cover type (dynamic or static)
#         model (str): Model name (e.g. CCSM4)
#         scenario (str): Scenario name (e.g. historical)
#         era (str): Era name (e.g. 1976_2005)
#     Returns:
#         Tuple of encoded parameters (integers) for Rasdaman request"""

#     url = RAS_BASE_URL + f"ows?&SERVICE=WCS&VERSION=2.1.0&REQUEST=DescribeCoverage&COVERAGEID={cov_id}&outputType=GeneralGridCoverage"
#     with requests.get(url, verify=False) as r:
#         if r.status_code != 200:
#             return render_template("500/server_error.html"), 500
#         tree = ET.ElementTree(ET.fromstring(r.content))

#     xml_search_string = str(".//{http://www.rasdaman.org}" + encoding_attr)
#     encoding_dict_str = tree.findall(xml_search_string)[0].text
#     encoding_dict = eval(encoding_dict_str)

#     if lc is not None:
#         lc_ = encoding_dict["lc"][lc]
#     else:
#         lc_ = None

#     if model is not None:
#         model_ = encoding_dict["model"][model]
#     else:
#         model_ = None

#     if scenario is not None:
#         scenario_ = encoding_dict["scenario"][scenario]
#     else:
#         scenario_ = None

#     if era is not None:
#         era_ = encoding_dict["era"][era]
#     else:
#         era_ = None

#     return lc_, model_, scenario_, era_


def build_dict_and_populate_stats(geom_id, ds):
    """
    Function to populate the stats in the data dictionary with the hydrology statistics.
    The levels of the stats data dictionary are as follows: landcover, model, scenario, era, variable.
    Args:
        geom_id (str): Geometry ID for the hydrology data
        ds (xarray dataset): Dataset with hydrological stats for the geom ID
        data_dict (dict): Data dictionary to populate with the hydrology stats
    Returns:
        Data dictionary with the hydrology stats populated.
    """

    data_dict = {
        geom_id: {"name": None, "centroid_lat": None, "centroid_lon": None, "stats": {}}
    }

    # get the stats from the dataset for each landcover, model, scenario, era, and variable.
    vars = list(ds.data_vars)
    for lc in ds.lc.values:
        data_dict[geom_id]["stats"][lc] = {}
        for model in ds.model.values:
            data_dict[geom_id]["stats"][lc][model] = {}
            for scenario in ds.scenario.values:
                data_dict[geom_id]["stats"][lc][model][scenario] = {}
                for era in ds.era.values:
                    data_dict[geom_id]["stats"][lc][model][scenario][era] = {}
                    stats_dict = {}
                    for var in vars:
                        stat_value = float(
                            ds[var].sel(lc=lc, model=model, scenario=scenario, era=era)
                        )
                        stats_dict[var] = stat_value
                        data_dict[geom_id]["stats"][lc][model][scenario][
                            era
                        ] = stats_dict

    data_dict = encode_data_dict(data_dict, ds)

    return data_dict


# def populate_attributes(geom_id, data_dict):
#    for idx, row in huc_segments.iterrows():

#         geojson = (
#             huc_segments[["seg_id_nat", "GNIS_NAME", "geometry"]]
#             .loc[idx]
#             .to_json(default_handler=str)
#         )

#         segment_dict = dict(
#             {
#                 "name": row.GNIS_NAME,
#                 "data_by_model": dict({}),
#                 "geojson": geojson,
#             }
#         )

#         data_dict["segments"][row.seg_id_nat] = segment_dict

#     return data_dict


@routes.route("/conus_hydrology/")
def conus_hydrology_about():
    return render_template("/documentation/conus_hydrology.html")


@routes.route("/conus_hydrology/<geom_id>")
def run_get_conus_hydrology_point_data(geom_id):
    """
    Function to fetch hydrology data from Rasdaman for a single geometry ID.
    Additional reguest arguments can be made for land cover type, model, scenario, era, and variables.
    For example: /conus_hydrology/12345?lc=dynamic&model=CCSM4&scenario=historical&era=1976_2005&vars=dh3,dh15
    Args:
        geom_id (str): Geometry ID for the hydrology data
    Returns:
        JSON response with hydrological stats for the requested geom ID.
    """
    # might incorporate the code below if we want to field GET requests for further subsetting of the datacube
    # sort through request arguments and assign to variables, if they exist
    # if request.args.get("lc"):
    #     lc = request.args.get("lc")
    # if request.args.get("model"):
    #     model = request.args.get("model")
    # if request.args.get("scenario"):
    #     scenario = request.args.get("scenario")
    # if request.args.get("era"):
    #     era = request.args.get("era")
    # if request.args.get("vars"):
    #     if len(request.args.get("vars").split(",")) > 1:
    #         vars = request.args.get("vars").split(",")
    #     else:
    #         vars = [request.args.get("vars")]

    ds = fetch_hydrology_data(cov_id, geom_id)
    # save nc to test size of return
    ds.to_netcdf("/home/jdpaul3/stats_from_geom_id.nc", engine="h5netcdf")

    # build the data dictionary and populate with the hydrology statistics
    data_dict = build_dict_and_populate_stats(geom_id, ds)

    # populate attributes from vector data
    # data_dict = populate_attributes(geom_id, data_dict)

    # return Flask JSON Response
    json_results = json.dumps(data_dict, indent=4)

    # save json to test size of return
    with open("/home/jdpaul3/result.json", "w", encoding="utf-8") as f:
        json.dump(json_results, f, ensure_ascii=False, indent=4)

    return Response(json_results, mimetype="application/json")
