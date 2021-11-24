import asyncio
import io
import csv
import time
import itertools
import numpy as np
import geopandas as gpd
import xarray as xr
from aiohttp import ClientSession
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)
from rasterstats import zonal_stats
from validate_latlon import validate, validate_bbox, project_latlon
from . import routes
from config import RAS_BASE_URL
from fetch_data import fetch_layer_data

iem_api = Blueprint("iem_api", __name__)

huc_gdf = gpd.read_file("data/shapefiles/hydrologic_units\wbdhu8_a_ak.shp").set_index(
    "huc8"
)

# encodings hardcoded for now
# fmt: off
# lookup tables derived from the IEM rasdaman ingest luts.py
# pay attention to any changes with ingest and change here as needed
dim_encodings = {
    "varnames": {
        0: "pr",
        1: "tas",
    },
    "decades": {
        0: "2010_2019",
        1: "2020_2029",
        2: "2030_2039",
        3: "2040_2049",
        4: "2050_2059",
        5: "2060_2069",
        6: "2070_2079",
        7: "2080_2089",
        8: "2090_2099",
    },
    "months": {
        0: "Jan",
        1: "Feb",
        2: "Mar",
        3: "Apr",
        4: "May",
        5: "Jun",
        6: "Jul",
        7: "Aug",
        8: "Sep",
        9: "Oct",
        10: "Nov",
        11: "Dec",
    },
    "models": {
        0: "5modelAvg",
        1: "CCSM4",
        2: "MRI-CGCM3",
    },
    "scenarios": {
        0: "rcp45",
        1: "rcp60",
        2: "rcp85",
    },
    "seasons": {
        0: "DJF",
        1: "JJA",
        2: "MAM",
        3: "SON",
    },
    "statnames": {
        0: "hi_std",
        1: "lo_std",
        2: "max",
        3: "mean",
        4: "min",
    },
}

cru_decades = {
    0: "1910_1919",
    1: "1920_1929",
    2: "1930_1939",
    3: "1940_1949",
    4: "1950_1959",
    5: "1960_1969",
    6: "1970_1979",
    7: "1980_1989",
    8: "1990_1999",
    9: "2000_2009",
}
# fmt: on

# store global list of invalid dim value combinations, such as
# model == CRU TS31 and period == 2040-2070, etc.
invalid_dim_values = list(itertools.product(range(2), range(4), [2], range(4)))
invalid_dim_values.extend(itertools.product([2], range(4), range(2), range(4)))
invalid_dim_values.extend(itertools.product(range(3), range(4), range(2), [3]))
invalid_dim_values.extend(itertools.product(range(3), range(4), [2], range(3)))

# do the same as above for only invalid model / period combinations
invalid_model_periods = list(itertools.product(range(2), [2]))
invalid_model_periods.extend(itertools.product([2], range(2)))


async def fetch_point_data(x, y, cov_id):
    """Make the async request for the data at the specified point

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_id (str): Rasdaman coverage id

    Returns:
        nested list containing results of WCS point query
    """
    url = f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID={cov_id}&SUBSET=X({x})&SUBSET=Y({y})&FORMAT=application/json"
    print(url)
    async with ClientSession() as session:
        point_data = await asyncio.create_task(fetch_layer_data(url, session))

    return point_data


def package_point_data(point_data, temporal_key):
    """Add dim names to JSON response from point query

    Args:
        point_data (list): nested list containing JSON
            results of AR5 or CRU point query
        temporal_key (str): the type of summary of source 
            of point_data, either "months" or "seasons"

    Returns:
        Dict with dimension name
    """
    point_data_pkg = {}

    # AR5 data has 9 decades, CRU has 10
    if len(point_data) == 9:
        # AR5 data:
        # varname, decade, month, model, scenario
        #   Since we are relying on some hardcoded mappings between
        # integers and the dataset dimensions, we should consider
        # having that mapping tracked somewhere such that it is
        # imported to help prevent breakage.

        for di, m_li in enumerate(point_data):  # (nested list with month at dim 0)
            decade = dim_encodings["decades"][di]
            point_data_pkg[decade] = {}
            for ai, mod_li in enumerate(m_li):  # (nested list with model at dim 0)
                aggr_period = dim_encodings[temporal_key][ai]
                point_data_pkg[decade][aggr_period] = {}
                for mod_i, s_li in enumerate(
                    mod_li
                ):  # (nested list with scenario at dim 0)
                    model = dim_encodings["models"][mod_i]
                    point_data_pkg[decade][aggr_period][model] = {}
                    for si, v_li in enumerate(
                        s_li
                    ):  # (nested list with varname at dim 0)
                        scenario = dim_encodings["scenarios"][si]
                        point_data_pkg[decade][aggr_period][model][scenario] = {}
                        for vi, value in enumerate(v_li):  # (data values)
                            varname = dim_encodings["varnames"][vi]
                            point_data_pkg[decade][aggr_period][model][scenario][
                                varname
                            ] = value

    elif len(point_data) == 4:
        # hard-code summary period for CRU
        period = "1950-2009"
        point_data_pkg[period] = {}
        for si, v_li in enumerate(point_data):  # (nested list with varname at dim 0)
            season = dim_encodings["seasons"][si]
            model = "CRU-TS31"
            scenario = "CRU_historical"
            point_data_pkg[period][season] = {model: {scenario: {}}}
            for vi, s_li in enumerate(v_li):  # (nested list with statistic at dim 0)
                varname = dim_encodings["varnames"][vi]
                point_data_pkg[period][season][model][scenario][varname] = {}
                for si, value in enumerate(s_li):  # (data values)
                    statname = dim_encodings["statnames"][si]
                    point_data_pkg[period][season][model][scenario][varname][
                        statname
                    ] = value

    return point_data_pkg


def package_ar5_point_summary(point_data):
    """Add dim names to JSON response from point query
    for the AR5 coverages

    Args:
        point_data (list): nested list containing JSON
            results of AR5 or CRU point query

    Returns:
        Dict of query results
    """
    point_data_pkg = {}
    for si, mod_li in enumerate(point_data):  # (nested list with model at dim 0)
        season = dim_encodings["seasons"][si]
        point_data_pkg[season] = {}
        for mod_i, s_li in enumerate(mod_li):  # (nested list with scenario at dim 0)
            model = dim_encodings["models"][mod_i]
            point_data_pkg[season][model] = {}
            for si, v_li in enumerate(s_li):  # (nested list with varname at dim 0)
                scenario = dim_encodings["scenarios"][si]
                point_data_pkg[season][model][scenario] = {}
                for vi, value in enumerate(v_li):  # (data values)
                    varname = dim_encodings["varnames"][vi]
                    point_data_pkg[season][model][scenario][varname] = round(
                        value, rounding[varname]
                    )

    return point_data_pkg


async def fetch_bbox_netcdf(x1, y1, x2, y2, cov_id):
    """Make the async request for the data within the specified bbox

    Args:
        x1 (float): lower x-coordinate bound
        y1 (float): lower y-coordinate bound
        x2 (float): upper x-coordinate bound
        y2 (float): upper y-coordinate bound
        cov_id (str): Coverage id

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    # only see this ever being a single request
    url = f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID={cov_id}&SUBSET=X({x1},{x2}))&SUBSET=Y({y1},{y2})&FORMAT=application/netcdf"

    start_time = time.time()
    async with ClientSession() as session:
        netcdf_bytes = await asyncio.create_task(make_netcdf_request(url, session))

    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )

    # create xarray.DataSet from bytestring
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes))

    return ds


def aggregate_dataarray(ds, poly, transform, temporal_key):
    """Perform a spatial agrgegation of a data array within a polygon.
    Only supports mean aggregation for now.

    Args:
        ds (xarray.DataSet): datacube for all variables
        dimensions (list): string names of variables in DataSet
        poly (shapely.Polygon): polygon from shapefile
        transform (affine.Affine): affine transform raster subset
        temporal_key (str): Type of queried data, either "seasons" or "months"

    Returns:
        results of aggregation as a JSON-like dict
    """

    def run_zonal_stats(ds, ds_sel_di, poly, transform):
        """Helper to run zonal stats on 
        selected subset of DataSet"""
        # default rasdaman band name is "Gray"
        arr = ds["Gray"].sel(ds_sel_di).values
        aggr_result = zonal_stats(
            poly, arr, affine=transform, nodata=np.nan, stats=["mean"],
        )[0]

        return aggr_result

    # use nested for loop to construct results dict like json output for single point
    aggr_results = {}
    # build aggregate results dict for JSON output
    # hardcoded assuming same 4 dimensions,
    #   consider revising with more robust approach

    # handle differences in dimensions present between CRU and AR5
    dimensions = ds["Gray"].dims[:-2]
    if len(dimensions) == 3:
        # CRU has 3 dimensions
        for di in np.int32(ds[dimensions[0]].values):
            decade = cru_decades[di]
            aggr_results[decade] = {}
            for ti in np.int32(ds[dimensions[1]].values):
                # derived period is the season or month the underlying
                # "derived" data product was aggregated over
                derived_period = dim_encodings[temporal_key][ti]
                aggr_results[decade][derived_period] = {}
                model = "CRU-TS31"
                aggr_results[decade][derived_period][model] = {}
                scenario = "CRU_historical"
                aggr_results[decade][derived_period][model][scenario] = {}
                for vi in np.int32(ds[dimensions[2]].values):
                    varname = dim_encodings["varnames"][vi]
                    # construct dict for ds.sel based on whether CRU or AR5
                    ds_sel_di = {
                        "decade": di,
                        temporal_key[:-1]: ti,
                        "varname": vi,
                    }
                    aggr_result = run_zonal_stats(ds, ds_sel_di, poly, transform)
                    aggr_results[decade][derived_period][model][scenario][
                        varname
                    ] = round(aggr_result["mean"], 1)

    elif len(dimensions) == 5:
        # AR5 has 5 dimensions
        for di in np.int32(ds[dimensions[0]].values):
            decade = dim_encodings["decades"][di]
            aggr_results[decade] = {}
            for ti in np.int32(ds[dimensions[1]].values):
                # derived period is the season or month the underlying
                # "derived" data product was aggregated over
                derived_period = dim_encodings[temporal_key][ti]
                aggr_results[decade][derived_period] = {}
                for mi in np.int32(ds[dimensions[2]].values):
                    model = dim_encodings["models"][mi]
                    aggr_results[decade][derived_period][model] = {}
                    for sci in np.int32(ds[dimensions[3]].values):
                        scenario = dim_encodings["scenarios"][sci]
                        # select subset and compute aggregate
                        aggr_results[decade][derived_period][model][scenario] = {}
                        for vi in np.int32(ds[dimensions[4]].values):
                            varname = dim_encodings["varnames"][vi]
                            # construct dict for ds.sel based on whether CRU or AR5
                            ds_sel_di = {
                                "decade": di,
                                temporal_key[:-1]: ti,
                                "model": mi,
                                "scenario": sci,
                                "varname": vi,
                            }
                            aggr_result = run_zonal_stats(
                                ds, ds_sel_di, poly, transform
                            )
                            aggr_results[decade][derived_period][model][scenario][
                                varname
                            ] = round(aggr_result["mean"], 1)

    return aggr_results


def create_csv(packaged_data, temporal_key):
    """
    Returns a CSV version of the fetched data, as a string.

    Args:
        packaged_data (json): output from the package_point_data function here

    Returns:
        string of CSV data
    """
    output = io.StringIO()

    fieldnames = [
        "variable",
        "date_range",
        temporal_key[:-1],
        "model",
        "scenario",
        "value",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()

    for decade in dim_encodings["decades"].values():
        # naming is a bit of an issue with the various levels of aggregation going on.
        # derived_period is the season or month the underlying
        # "derived" data product was aggregated over
        for derived_period in dim_encodings[temporal_key].values():
            if decade in list(cru_decades.values()):
                for varname in ["pr", "tas"]:
                    writer.writerow(
                        {
                            "variable": varname,
                            "date_range": decade,
                            # temporal_key is either "seasons" or "months"
                            temporal_key[:-1]: derived_period,
                            "model": "CRU-TS31",
                            "scenario": "Historical",
                            "value": packaged_data[decade][derived_period]["CRU-TS31"][
                                "CRU_historical"
                            ][varname],
                        }
                    )
            else:
                for model in dim_encodings["models"].values():
                    for scenario in dim_encodings["scenarios"].values():
                        for varname in ["pr", "tas"]:
                            writer.writerow(
                                {
                                    "variable": varname,
                                    "date_range": decade,
                                    temporal_key[:-1]: derived_period,
                                    "model": model,
                                    "scenario": scenario,
                                    "value": packaged_data[decade][derived_period][
                                        model
                                    ][scenario][varname],
                                }
                            )

    return output.getvalue()


def get_temporal_type(args):
    """helper to set some variables based on whether 
    query is for monthly or seasonal data

    Args:
        args (flask.Request.args): dict of contents of query string

    Returns: 
        temporal type and key - type is used for building rasdaman 
        URL, key is used for packaging/manuipulations 

    Notes:
        currently two options available with these data, 
        seasonal or monthly, and default is seasonal
    """
    # if not monthly, defaults to seasonal
    if args.get("summary") == "monthly":
        temporal_type = "monthly"
        temporal_key = "months"
    else:
        temporal_type = "seasonal"
        temporal_key = "seasons"

    return temporal_type, temporal_key


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
def run_fetch_point_data(lat, lon):
    """Run the async IEM data requesting for a single point
    and return data as json

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON of data at provided latitude and longitude

    Notes:
        example request: http://localhost:5000/iem/point/65.0628/-146.1627
    """
    if not validate(lat, lon):
        abort(400)

    x, y = project_latlon(lat, lon, 3338)

    # currently two options available with these data, seasonal or monthly,
    # and default is seasonal
    temporal_type, temporal_key = get_temporal_type(request.args)

    ar5_point_data = asyncio.run(
        fetch_point_data(x, y, f"iem_ar5_2km_taspr_{temporal_type}")
    )
    ar5_point_pkg = package_point_data(ar5_point_data, temporal_key)
    cru_point_data = asyncio.run(
        fetch_point_data(x, y, "iem_cru_2km_taspr_seasonal_baseline_stats")
    )
    # use CRU as basis for combined point package for chronolical consistency
    point_pkg = package_point_data(cru_point_data, temporal_key)
    # combine the CRU and AR5 packages
    for decade, summaries in ar5_point_pkg.items():
        point_pkg[decade] = summaries

    if request.args.get("format") == "csv":
        csv_data = create_csv(point_pkg, temporal_key)
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Type": 'text/csv; name="climate.csv"',
                "Content-Disposition": 'attachment; filename="climate.csv"',
            },
        )

    return point_pkg


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
    # TODO What if the huc_id is invalid?
    poly_gdf = huc_gdf.loc[[huc_id]][["geometry"]].to_crs(3338)

    poly = poly_gdf.iloc[0]["geometry"]

    # currently two options available with these data, seasonal or monthly,
    # and default is seasonal
    temporal_type, temporal_key = get_temporal_type(request.args)

    # meterological variables as dataset
    ar5_met_ds = asyncio.run(
        fetch_bbox_netcdf(*poly.bounds, f"iem_ar5_2km_taspr_{temporal_type}")
    )
    cru_met_ds = asyncio.run(
        fetch_bbox_netcdf(*poly.bounds, f"iem_cru_2km_taspr_{temporal_type}")
    )

    # aggregate the data and return packaged results
    # compute transform with rioxarray
    ar5_met_ds.rio.set_spatial_dims("X", "Y")
    transform = ar5_met_ds.rio.transform()
    # dimensions = ["decade", temporal_key[:-1], "model", "scenario"]
    ar5_aggr_results = aggregate_dataarray(ar5_met_ds, poly, transform, temporal_key)
    # use CRU as basis for combined point package for chronolical consistency
    aggr_results = aggregate_dataarray(cru_met_ds, poly, transform, temporal_key)
    # combine the CRU and AR5 packages
    for decade, summaries in ar5_aggr_results.items():
        aggr_results[decade] = summaries

    if request.args.get("format") == "csv":
        csv_data = create_csv(aggr_results, temporal_key)
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Type": 'text/csv; name="climate.csv"',
                "Content-Disposition": 'attachment; filename="climate.csv"',
            },
        )

    return aggr_results
