import asyncio
import io
import itertools
import numpy as np
import geopandas as gpd
import xarray as xr
from aiohttp import ClientSession
from flask import abort, Blueprint, render_template
from rasterstats import zonal_stats
from validate_latlon import validate, validate_bbox, project_latlon
from . import routes
from config import RAS_BASE_URL

iem_api = Blueprint("iem_api", __name__)

huc_gdf = gpd.read_file("data/shapefiles/hydrologic_units\wbdhu8_a_ak.shp").set_index(
    "huc8"
)

# encodings hardcoded for now
dim_encodings = {
    "period": {0: "2040_2070", 1: "2070_2100",},
    "season": {0: "DJF", 1: "MAM", 2: "JJA", 3: "SON",},
    "model": {0: "CCSM4", 1: "MRI-CGCM3",},
    "scenario": {0: "rcp45", 2: "rcp85",},
}


async def fetch_layer_data(url, session):
    """Make an awaitable GET request to URL, return json"""
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    json = await resp.json()
    return json


async def fetch_iem_data(x1, y1, x2=None, y2=None):
    """IEM API - gather all async requests for IEM data

    Note - Currently specific to the preprocessed decadal seasonal
        summary data (tas, pr)
    """
    if x2 is None:
        x, y = x1, y1
    else:
        x, y = f"{x1},{x2}", f"{y1},{y2}"

    # using list in case further endpoints are added
    urls = []
    urls.append(
        RAS_BASE_URL
        + f"ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID=iem_temp_precip_wms&SUBSET=X({x})&SUBSET=Y({y})&FORMAT=application/json"
    )

    print(urls)
async def fetch_bbox_netcdf(x1, y1, x2, y2):
    """Make the async request for the data within the specified bbox

    Args:
        x1 (float): lower x-coordinate bound
        y1 (float): lower y-coordinate bound

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    # only see this ever being a single request
    url = f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID=iem_temp_precip_wms&SUBSET=X({x1},{x2}))&SUBSET=Y({y1},{y2})&FORMAT=application/netcdf"

    async with ClientSession() as session:
        netcdf_bytes = await asyncio.create_task(make_request(url, session))

    # create xarray.DataSet from bytestring
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes))

    return ds

def package_iem(iem_resp):
    """Package IEM tas and pr data in dict

    Since we are relying on some hardcoded mappings between
    integers and the dataset dimensions, we should consider
    having that mapping tracked somewhere such that it is
    imported to help prevent breakage.
    """
    # encodings hardcoded for now
    dim_encodings = {
        "period": {0: "2040_2070", 1: "2070_2100"},
        "season": {0: "DJF", 1: "MAM", 2: "JJA", 3: "SON"},
        "model": {0: "CCSM4", 1: "MRI-CGCM3"},
        "scenario": {0: "rcp45", 1: "rcp85"},
    }

    iem_pkg = {}
    variables = ["tas", "pr"]

    # period, season, model, scenario
    for pi, s_li in enumerate(iem_resp):  # (season_list)
        period = dim_encodings["period"][pi]
        iem_pkg[period] = {}
        for si, m_li in enumerate(s_li):  # (model list)
            season = dim_encodings["season"][si]
            iem_pkg[period][season] = {}
            for mi, sc_li in enumerate(m_li):  # (scenario list)
                model = dim_encodings["model"][mi]
                iem_pkg[period][season][model] = {}
                for sci, values in enumerate(sc_li):
                    scenario = dim_encodings["scenario"][sci]
                    iem_pkg[period][season][model][scenario] = {}

                    if isinstance(values, str):
                        # if values is a string, it was a point query
                        for variable, value in zip(variables, values.split(" ")):
                            iem_pkg[period][season][model][scenario][variable] = value
                    elif isinstance(values, list):
                        # otherwise, bounding box query, create arrays from json
                        query_arr = np.char.split(np.array(values))
                        query_shape = query_arr.shape
                        for variable, i in zip(variables, range(2)):
                            arr = (
                                np.array([data[i] for row in query_arr for data in row])
                                .reshape(query_shape)
                                .astype(np.float32)
                            )
                            iem_pkg[period][season][model][scenario][
                                variable
                            ] = arr.tolist()

    return iem_pkg


def aggregate_dataarray(da, variables, poly, transform):
    """Perform a spatial agrgegation of a data array within a polygon.
    Only supports mean aggregation for now.

    Args:
        da (xarray.DataArray): datacube for individual variable
        variables (list): string names of variables in DataArray
        poly (shapely.Polygon): polygon from shapefile
        transform (affine.Affine): affine transform raster subset
    
    Returns:
        results of aggregation as a JSON-like dict
    """
    dim_combos = itertools.product(*[da[variable].values for variable in variables])
    # use nested for loop to construct results dict like json output for single point
    aggr_results = {}
    # build aggregate results dict for JSON output
    # hardcoded assuming same 4 dimensions,
    #   consider revising with more robust approach
    for period_value in np.int32(da[variables[0]].values):
        period = dim_encodings["period"][period_value]
        aggr_results[period] = {}
        for season_value in np.int32(da[variables[1]].values):
            season = dim_encodings["season"][season_value]
            aggr_results[period][season] = {}
            for model_value in np.int32(da[variables[2]].values):
                model = dim_encodings["model"][model_value]
                aggr_results[period][season][model] = {}
                for scenario_value in np.int32(da[variables[3]].values):
                    scenario = dim_encodings["scenario"][scenario_value]
                    # select subset and compute aggregate
                    arr = da.sel(
                        period=period_value,
                        season=season_value,
                        model=model_value,
                        scenario=scenario_value,
                    ).values
                    aggr_result = zonal_stats(
                        poly, arr, affine=transform, nodata=np.nan, stats=["mean"],
                    )[0]
                    aggr_results[period][season][model][scenario] = aggr_result

    return aggr_results


@routes.route("/iem/")
@routes.route("/iem/abstract/")
def about():
    return render_template("iem/abstract.html")


@routes.route("/iem/point/")
def about_point():
    return render_template("iem/point.html")


@routes.route("/iem/huc/")
def about_huc():
    return render_template("iem/huc.html")


@routes.route("/iem/point/<lat>/<lon>")
def run_fetch_iem_point_data(lat, lon):
    """Run the ansync IEM data requesting for a single point
    and return data as json

    example request: http://localhost:5000/iem/point/65.0628/-146.1627
    """
    if not validate(lat, lon):
        abort(400)

    x, y = project_latlon(lat, lon, 3338)

    results = asyncio.run(fetch_iem_data(x, y))
    iem = package_iem(results[0])

    return iem


@routes.route("/iem/huc/<huc_id>")
def run_aggregate_huc(huc_id):
    """Get data within a huc and aggregate according by mean

    Args:
        huc_id (int): 8-digit HUD ID

    Returns:
        Mean summaries of rasters within HUC

    Notes:
        Rasters refers to the individual isntances of the 
          singular dimension combinations
    """
    # could add check here to make sure HUC is in the geodataframe

    # get the HUC as a single row single column dataframe with
    #   geometry column for zonal_stats
    # reproject is needed for zonal_stats and for initial bbox
    #   bounds for query
    poly_gdf = huc_gdf.loc[[huc_id]][["geometry"]].to_crs(3338)

    poly = poly_gdf.iloc[0]["geometry"]

    # meterological variables as dataset
    met_ds = asyncio.run(fetch_bbox_netcdf(*poly.bounds))

    # aggregate the data and return packaged results
    # compute transform with rioxarray
    aggr_results = {}
    met_ds.rio.set_spatial_dims("X", "Y")
    transform = met_ds.rio.transform()
    variables = ["period", "season", "model", "scenario"]
    aggr_results["tas"] = aggregate_dataarray(met_ds["tas"], variables, poly, transform)
    aggr_results["pr"] = aggregate_dataarray(met_ds["pr"], variables, poly, transform)

    return aggr_results
