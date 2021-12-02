import asyncio
import io
import csv
import time
import itertools
from urllib.parse import quote
import numpy as np
import geopandas as gpd
import xarray as xr
from flask import (
    abort,
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
)
from rasterstats import zonal_stats

# local imports
from validate_latlon import validate, project_latlon
from fetch_data import get_wcs_request_str, generate_wcs_query_url, fetch_data
from . import routes

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
# fmt: on


def get_wcps_request_str(x, y, cov_id, summary_decades, encoding="json"):
    """Generates a WCPS query specific to the 
    coverages used in the endpoints herein. The only
    axis we are currently averaging over is "decade", so
    this function creates a WCPS query from integer 
    values corresponding to decades to summarize over.

    Args:
        x (float or str): x-coordinate for point query, or string
            composed as "x1:x2" for bbox query, where x1 and x2 are
            lower and upper bounds of bbox
        y (float or str): y-coordinate for point query, or string
            composed as "y1:y2" for bbox query, where y1 and y2 are
            lower and upper bounds of bbox
        cov_id (str): Rasdaman coverage ID
        summary_decades (tuple): 2-tuple of integers mapped to 
            desired range of decades to summarise over, 
            e.g. (6, 8) for 2070-2099
        encoding (str): currently supports either "json" or "netcdf"
            for point or bbox queries, respectively

    Returns:
        WCPS query to be included in generate_wcs_url()
    """
    d1, d2 = summary_decades
    n = len(np.arange(d1, d2 + 1))
    wcps_request_str = quote(
        (
            f"ProcessCoverages&query=for $c in ({cov_id}) "
            f"let $a := (condense + over $t decade({d1}:{d2}) "
            f"using $c[decade($t),X({x}),Y({y})] ) / {n} "
            f'return encode( $a , "application/{encoding}")'
        )
    )

    return wcps_request_str


def get_from_dict(data_dict, map_list):
    return reduce(operator.getitem, map_list, data_dict)


async def fetch_point_data(x, y, cov_ids, summary_decades):
    """Make the async request for the data at the specified point

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        cov_ids (list): Rasdaman coverage ids
        summary_decades (tuple): 2-tuple of integers mapped to 
            desired range of decades to summarise over, 
            e.g. (6, 8) for 2070-2099

    Returns:
        list of data results from each cov_id/summary_decades 
        pairing
    """
    urls = []
    for cov_id, decade_tpl in zip(cov_ids, summary_decades):
        if decade_tpl:
            # if summary decades are given, create a WCPS request string
            request_str = get_wcps_request_str(x, y, cov_id, decade_tpl)
        else:
            # otheriwse use generic WCS request str
            request_str = get_wcs_request_str(x, y, cov_id)
        urls.append(generate_wcs_query_url(request_str))

    point_data_list = await fetch_data(urls)

    return point_data_list


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


async def fetch_bbox_netcdf(x1, y1, x2, y2, cov_ids, summary_decades):
    """Make the async request for the data within the specified bbox

    Args:
        x1 (float): lower x-coordinate bound
        y1 (float): lower y-coordinate bound
        x2 (float): upper x-coordinate bound
        y2 (float): upper y-coordinate bound
        cov_ids (str): list of Coverage ids to fetch the same bbox over
        summary_decades (list): list of either None or 2-tuples of integers 
            mapped to desired range of decades to summarise over, 
            e.g. (6, 8) for 2070-2099. List items need to 
            correspond to items in cov_ids.

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    encoding = "netcdf"

    urls = []
    for cov_id, decade_tpl in zip(cov_ids, summary_decades):
        if decade_tpl:
            # if summary decades are given, create a WCPS request string
            x = f"{x1}:{x2}"
            y = f"{y1}:{y2}"
            request_str = get_wcps_request_str(x, y, cov_id, decade_tpl, encoding)
        else:
            # otheriwse use generic WCS request str
            x = f"{x1},{x2}"
            y = f"{y1},{y2}"
            request_str = get_wcs_request_str(x, y, cov_id, encoding)
        urls.append(generate_wcs_query_url(request_str))

    start_time = time.time()
    data_list = await fetch_data(urls)
    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {round(time.time() - start_time)}s"
    )

    # create xarray.DataSet from bytestring
    ds_list = [xr.open_dataset(io.BytesIO(netcdf_bytes)) for netcdf_bytes in data_list]

    return ds_list


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
        "season",
        "model",
        "scenario",
        "value",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()

    # add CRU data
    cru_period = "1950_2009"
    for season in dim_encodings["seasons"].values():
        for varname in ["pr", "tas"]:
            for statname in dim_encodings["statnames"].values():
                writer.writerow(
                    {
                        "variable": varname,
                        "date_range": cru_period,
                        "season": season,
                        "model": "CRU-TS40",
                        "scenario": "Historical",
                        "value": packaged_data[cru_period][season]["CRU-TS40"][
                            "CRU_historical"
                        ][varname][statname],
                    }
                )

    # AR5 periods
    for ar5_period in ["2040_2069", "2070_2099"]:
        for season in dim_encodings["seasons"].values():
            for model in dim_encodings["models"].values():
                for scenario in dim_encodings["scenarios"].values():
                    for varname in ["pr", "tas"]:
                        writer.writerow(
                            {
                                "variable": varname,
                                "date_range": ar5_period,
                                "season": season,
                                "model": model,
                                "scenario": scenario,
                                "value": packaged_data[ar5_period][season][model][
                                    scenario
                                ][varname],
                            }
                        )

    for decade in dim_encodings["decades"].values():
        for season in dim_encodings["seasons"].values():
            # naming is a bit of an issue with the various levels of aggregation going on.
            # derived_period is the season or month the underlying
            # "derived" data product was aggregated over

            for model in dim_encodings["models"].values():
                for scenario in dim_encodings["scenarios"].values():
                    for varname in ["pr", "tas"]:
                        writer.writerow(
                            {
                                "variable": varname,
                                "date_range": season,
                                "season": season,
                                "model": model,
                                "scenario": scenario,
                                "value": packaged_data[decade][season][model][scenario][
                                    varname
                                ],
                            }
                        )

    return output.getvalue()


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


def make_fetch_args():
    """Fixed helper function for ensuring 
    consistency between point and HUC queries
    """
    cov_ids = [
        "iem_cru_2km_taspr_seasonal_baseline_stats",
        "iem_ar5_2km_taspr_seasonal",
        "iem_ar5_2km_taspr_seasonal",
        "iem_ar5_2km_taspr_seasonal",
    ]
    summary_decades = [None, (3, 5), (6, 8), None]

    return cov_ids, summary_decades


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
    cov_ids, summary_decades = make_fetch_args()
    point_data_list = asyncio.run(fetch_point_data(x, y, cov_ids, summary_decades))

    # package point data with decoded coord values (names)
    # these functions are hard-coded  with coord values for now
    point_pkg = {}
    point_pkg["1950_2009"] = package_cru_point_data(point_data_list[0])
    point_pkg["2040_2069"] = package_ar5_point_summary(point_data_list[1])
    point_pkg["2070_2099"] = package_ar5_point_summary(point_data_list[2])
    # package AR5 decadal data with decades and fold into data pakage
    ar5_point_pkg = package_ar5_point_data(point_data_list[3])
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
    # fetch bbox data
    cov_ids, summary_decades, summary_periods = make_fetch_args()
    ds_list = asyncio.run(fetch_bbox_netcdf(*poly.bounds, cov_ids, summary_decades))

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
