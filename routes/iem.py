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
from urllib.parse import quote

# local imports
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

# lookup for rounding values
rounding = {
    "tas": 1,
    "pr": 0,
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


def make_wcs_url(
    x1, y1, cov_id, encoding="json", summary_decades=None, x2=None, y2=None
):
    """Make a WCS query for one of the IEM rasdaman coverages

    Args:
        x1 (float): x-coordiante for point query, lower x-coordinate bound if not
        y1 (float): y-coordinate for point query, lower y-coordinate bound if not
        cov_id (str): Rasdaman coverage id
        summary_decades (tuple): 2-tuple of integers mapped to
            desired range of decades to summarise over,
            e.g. (6, 8) for 2070-2099. This option constructs
            a WCS query with a WCPS query inside.
        x2 (float): upper bound of x axis to query. Assumes point query if not set.
        y2 (float): upper bound of y axis to query. Requires x be specified or will error.

    Returns:
        WCS query URL.
    """
    # set up x/y for query type
    if x2 is None:
        x = x1
        y = y1
    else:
        x = f"{x1},{x2}"
        y = f"{y1},{y2}"

    # make encoding proper
    encoding = f"application/{encoding}"

    base_url = f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST={{}}"
    if not summary_decades:
        request_str = f"GetCoverage&COVERAGEID={cov_id}&SUBSET=X({x})&SUBSET=Y({y})&FORMAT={encoding}"
    else:
        d1, d2 = summary_decades
        # not sure if this is the proper way
        # to compute average.
        n = len(np.arange(d1, d2 + 1))

        # x and y == strings ==> need colon for correct syntax
        try:
            y = y.replace(",", ":")
            x = x.replace(",", ":")
        except AttributeError:
            pass

        request_str = quote(
            (
                f"ProcessCoverages&query=for $c in ({cov_id}) "
                f"let $a := (condense + over $t decade({d1}:{d2}) "
                f"using $c[decade($t),X({x}),Y({y})] ) / {n} "
                f'return encode( $a , "{encoding}")'
            )
        )

    return base_url.format(request_str)


async def fetch_point_data(x, y, cov_id, summary_decades=None):
    """Make the async request for the data at the specified point

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_id (str): Rasdaman coverage id
        summary_decades (tuple): 2-tuple of integers mapped to 
            desired range of decades to summarise over, 
            e.g. (6, 8) for 2070-2099

    Returns:
        nested list containing results of WCS point query
    """
    url = make_wcs_url(x, y, cov_id, summary_decades=summary_decades)

    async with ClientSession() as session:
        point_data = await asyncio.create_task(fetch_layer_data(url, session))

    return point_data


def package_cru_point_data(point_data):
    """Add dim names to JSON response from point query
    for the CRU TS historical basline coverage

    Args:
        point_data (list): nested list containing JSON
            results of CRU point query

    Returns:
        Dict of query results
    """
    point_data_pkg = {}
    # hard-code summary period for CRU
    period = "1950_2009"
    point_data_pkg[period] = {}
    for si, v_li in enumerate(point_data):  # (nested list with varname at dim 0)
        season = dim_encodings["seasons"][si]
        model = "CRU-TS40"
        scenario = "CRU_historical"
        point_data_pkg[period][season] = {model: {scenario: {}}}
        for vi, s_li in enumerate(v_li):  # (nested list with statistic at dim 0)
            varname = dim_encodings["varnames"][vi]
            point_data_pkg[period][season][model][scenario][varname] = {}
            for si, value in enumerate(s_li):  # (data values)
                statname = dim_encodings["statnames"][si]
                point_data_pkg[period][season][model][scenario][varname][
                    statname
                ] = round(value, rounding[varname])

    return point_data_pkg


def package_ar5_point_data(point_data):
    """Add dim names to JSON response from AR5 point query

    Args:
        point_data (list): nested list containing JSON
            results of AR5 or CRU point query

    Returns:
        Dict with dimension name
    """
    point_data_pkg = {}
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
            season = dim_encodings["seasons"][ai]
            point_data_pkg[decade][season] = {}
            for mod_i, s_li in enumerate(
                mod_li
            ):  # (nested list with scenario at dim 0)
                model = dim_encodings["models"][mod_i]
                point_data_pkg[decade][season][model] = {}
                for si, v_li in enumerate(s_li):  # (nested list with varname at dim 0)
                    scenario = dim_encodings["scenarios"][si]
                    point_data_pkg[decade][season][model][scenario] = {}
                    for vi, value in enumerate(v_li):  # (data values)
                        varname = dim_encodings["varnames"][vi]
                        point_data_pkg[decade][season][model][scenario][varname] = value

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


async def fetch_bbox_netcdf(
    x1, y1, x2, y2, cov_id, summary_decades=None,
):
    """Make the async request for the data within the specified bbox

    Args:
        x1 (float): lower x-coordinate bound
        y1 (float): lower y-coordinate bound
        x2 (float): upper x-coordinate bound
        y2 (float): upper y-coordinate bound
        cov_id (str): Coverage id
        summary_decades (tuple): 2-tuple of integers mapped to 
            desired range of decades to summarise over, 
            e.g. (6, 8) for 2070-2099

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    url = make_wcs_url(
        x1,
        y1,
        cov_id,
        encoding="netcdf",
        summary_decades=summary_decades,
        x2=x2,
        y2=y2,
    )

    start_time = time.time()
    async with ClientSession() as session:
        netcdf_bytes = await asyncio.create_task(
            fetch_layer_data(url, session, encoding="netcdf")
        )

    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )

    # create xarray.DataSet from bytestring
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes))

    return ds


def run_zonal_stats(arr, poly, transform):
    """Helper to run zonal stats on 
        selected subset of DataSet"""
    # default rasdaman band name is "Gray"
    aggr_result = zonal_stats(
        poly, arr, affine=transform, nodata=np.nan, stats=["mean"],
    )[0]

    return aggr_result


def summarize_ar5_within_poly(ds, poly, transform):
    """Perform a spatial agrgegation (mean) of a data array within a polygon.
    Hardcoded for AR5 seasonal coverage.

    Args:
        ds (xarray.DataSet): datacube for all variables
        poly (shapely.Polygon): polygon from shapefile
        transform (affine.Affine): affine transform raster subset

    Returns:
        results of aggregation as a JSON-like dict
    """
    # use nested for loop to construct results dict like json output for single point
    aggr_results = {}
    # build aggregate results dict for JSON output
    # hardcoded assuming same 4 dimensions,
    #   consider revising with more robust approach
    for di in np.int32(ds["decade"].values):
        decade = dim_encodings["decades"][di]
        aggr_results[decade] = {}
        for si in np.int32(ds["season"].values):
            # derived period is the season or month the underlying
            # "derived" data product was aggregated over
            season = dim_encodings["seasons"][si]
            aggr_results[decade][season] = {}
            for mi in np.int32(ds["model"].values):
                model = dim_encodings["models"][mi]
                aggr_results[decade][season][model] = {}
                for sci in np.int32(ds["scenario"].values):
                    scenario = dim_encodings["scenarios"][sci]
                    # select subset and compute aggregate
                    aggr_results[decade][season][model][scenario] = {}
                    for vi in np.int32(ds["varname"].values):
                        varname = dim_encodings["varnames"][vi]
                        arr = (
                            ds["Gray"]
                            .sel(
                                {
                                    "decade": di,
                                    "season": si,
                                    "model": mi,
                                    "scenario": sci,
                                    "varname": vi,
                                }
                            )
                            .values
                        )
                        aggr_result = run_zonal_stats(arr, poly, transform)
                        aggr_results[decade][season][model][scenario][varname] = round(
                            aggr_result["mean"], rounding[varname]
                        )

    return aggr_results


def summarize_ar5_clim_within_poly(ds, poly, transform):
    """Perform a spatial agrgegation (mean) of a data array within a polygon.
    Hardcoded for AR5 seasonal coverage.

    Args:
        ds (xarray.DataSet): datacube for all variables
        poly (shapely.Polygon): polygon from shapefile
        transform (affine.Affine): affine transform raster subset

    Returns:
        results of aggregation as a JSON-like dict
    """
    # use nested for loop to construct results dict like json output for single point
    aggr_results = {}
    for si in np.int32(ds["season"].values):
        # derived period is the season or month the underlying
        # "derived" data product was aggregated over
        season = dim_encodings["seasons"][si]
        aggr_results[season] = {}
        for mi in np.int32(ds["model"].values):
            model = dim_encodings["models"][mi]
            aggr_results[season][model] = {}
            for sci in np.int32(ds["scenario"].values):
                scenario = dim_encodings["scenarios"][sci]
                # select subset and compute aggregate
                aggr_results[season][model][scenario] = {}
                for vi in np.int32(ds["varname"].values):
                    varname = dim_encodings["varnames"][vi]
                    arr = (
                        ds["Gray"]
                        .sel(
                            {"season": si, "model": mi, "scenario": sci, "varname": vi,}
                        )
                        .values
                    )
                    aggr_result = run_zonal_stats(arr, poly, transform)
                    aggr_results[season][model][scenario][varname] = round(
                        aggr_result["mean"], rounding[varname]
                    )

    return aggr_results


def summarize_cru_within_poly(ds, poly, transform):
    """Perform a spatial agrgegation of a data array within a polygon.
    Hardcoded for CRU TS seasonal baseline stats coverage.

    Args:
        ds (xarray.DataSet): datacube for all variables
        poly (shapely.Polygon): polygon from shapefile
        transform (affine.Affine): affine transform raster subset

    Returns:
        results of aggregation as a JSON-like dict
    """
    # use nested for loop to construct results dict like json output for single point
    aggr_results = {}
    for si in np.int32(ds["season"].values):
        season = dim_encodings["seasons"][si]
        model = "CRU-TS40"
        scenario = "CRU_historical"
        aggr_results[season] = {model: {scenario: {}}}
        for vi in np.int32(ds["varname"].values):
            varname = dim_encodings["varnames"][vi]
            aggr_results[season][model][scenario][varname] = {}
            for sti in np.int32(ds["stat"].values):
                statname = dim_encodings["statnames"][sti]
                arr = ds["Gray"].sel({"season": si, "varname": vi, "stat": sti,}).values
                aggr_result = run_zonal_stats(arr, poly, transform)
                aggr_results[season][model][scenario][varname][statname] = round(
                    aggr_result["mean"], rounding[varname]
                )

    return aggr_results


def create_csv(packaged_data):
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

    # get and combine the CRU and AR5 packages
    # use CRU as basis for combined point package for chronolical consistency
    # order of listing: CRU (1950-2009), AR5 2040-2069 summary,
    #     AR5 2070-2099 summary, AR5 seasonal data
    # query CRU baseline summary
    cru_point_data = asyncio.run(
        fetch_point_data(x, y, "iem_cru_2km_taspr_seasonal_baseline_stats")
    )
    point_pkg = package_cru_point_data(cru_point_data)
    # query summarized AR5 data for 2040-2070 and 2070-2090
    for period, decades in zip(["2040_2069", "2070_2099"], [(3, 5), (6, 8)]):
        summary_data = asyncio.run(
            fetch_point_data(x, y, "iem_ar5_2km_taspr_seasonal", decades)
        )
        point_pkg[period] = package_ar5_point_summary(summary_data)
    # query AR5 unsummarized data
    ar5_point_data = asyncio.run(fetch_point_data(x, y, "iem_ar5_2km_taspr_seasonal"))
    ar5_point_pkg = package_ar5_point_data(ar5_point_data)
    # include ar5 point data in point_pkg dict
    for decade, summaries in ar5_point_pkg.items():
        point_pkg[decade] = summaries

    if request.args.get("format") == "csv":
        csv_data = create_csv(point_pkg)
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

    cru_ds = asyncio.run(
        fetch_bbox_netcdf(*poly.bounds, f"iem_cru_2km_taspr_seasonal_baseline_stats")
    )
    # compute transform with rioxarray, used
    # for zonal_stats
    cru_ds.rio.set_spatial_dims("X", "Y")
    transform = cru_ds.rio.transform()
    # use CRU to begin storing results combined point package
    aggr_results = {"1950_2009": summarize_cru_within_poly(cru_ds, poly, transform)}

    for ar5_period, decades in zip(["2040_2069", "2070_2099"], [(3, 5), (6, 8)]):

        # need to make two separate WCPS queries, one for each future climatology
        ar5_clim_ds = asyncio.run(
            fetch_bbox_netcdf(*poly.bounds, f"iem_ar5_2km_taspr_seasonal", decades)
        )
        aggr_results[ar5_period] = summarize_ar5_clim_within_poly(
            ar5_clim_ds, poly, transform
        )

    ar5_ds = asyncio.run(fetch_bbox_netcdf(*poly.bounds, f"iem_ar5_2km_taspr_seasonal"))
    ar5_results = summarize_ar5_within_poly(ar5_ds, poly, transform)

    for decade, summaries in ar5_results.items():
        aggr_results[decade] = summaries

    if request.args.get("format") == "csv":
        csv_data = create_csv(aggr_results)
        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Type": 'text/csv; name="climate.csv"',
                "Content-Disposition": 'attachment; filename="climate.csv"',
            },
        )

    return aggr_results
