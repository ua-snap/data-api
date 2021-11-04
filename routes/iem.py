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
dim_encodings = {
    "period": {
        0: "2040_2070",
        1: "2070_2100",
        2: "1910-2009",
    },
    "season": {
        0: "DJF",
        1: "MAM",
        2: "JJA",
        3: "SON",
    },
    "model": {
        0: "CCSM4",
        1: "MRI-CGCM3",
        2: "CRU-TS31",
    },
    "scenario": {
        0: "rcp45",
        1: "rcp60",
        2: "rcp85",
        3: "CRU_historical",
    },
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


async def make_netcdf_request(url, session):
    """Make an awaitable GET request to WCS URL with
    netCDF encoding

    Args:
        url (str): WCS query with ncetCDF encoding
        session (aiohttp.ClientSession): the client session instance

    Returns:
        Query result, deocded differently depending on encoding.
    """
    resp = await session.get(url)
    resp.raise_for_status()
    query_result = await resp.read()

    return query_result


async def fetch_point_data(x, y):
    """Make the async request for the data at the specified point

    Args:
        x (float): lower x-coordinate bound
        y (float): lower y-coordinate bound
        x2 (float): upper x-coordinate bound
        y2 (float): upper y-coordinate bound

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    url = f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID=iem_ar5_cruts31_temp_precip_wms&SUBSET=X({x})&SUBSET=Y({y})&FORMAT=application/json"

    async with ClientSession() as session:
        point_data = await asyncio.create_task(fetch_layer_data(url, session))

    return point_data


async def fetch_bbox_netcdf(x1, y1, x2, y2):
    """Make the async request for the data within the specified bbox

    Args:
        x1 (float): lower x-coordinate bound
        y1 (float): lower y-coordinate bound

    Returns:
        xarray.DataSet containing results of WCS netCDF query
    """
    # only see this ever being a single request
    url = f"{RAS_BASE_URL}/ows?&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage&COVERAGEID=iem_ar5_cruts31_temp_precip_wms&SUBSET=X({x1},{x2}))&SUBSET=Y({y1},{y2})&FORMAT=application/netcdf"

    start_time = time.time()
    async with ClientSession() as session:
        netcdf_bytes = await asyncio.create_task(make_netcdf_request(url, session))

    app.logger.info(
        f"Fetched BBOX data from Rasdaman, elapsed time {(time.time() - start_time)}s"
    )

    # create xarray.DataSet from bytestring
    ds = xr.open_dataset(io.BytesIO(netcdf_bytes))

    # TODO: This is a temporary workaround to retitle the bands coming from
    # Rasdaman. It appears to be a bug with the returned NetCDF output.
    ds = ds.rename_vars({"tas": "pr", "pr": "tas"})

    return ds


def package_point_data(point_data):
    """Add dim names to JSON response from point query

    Args:
        point_data (json)

    Returns:
        Dict with dimension name
    """
    point_data_pkg = {}
    variables = ["tas", "pr"]

    # period, season, model, scenario
    # Since we are relying on some hardcoded mappings between
    # integers and the dataset dimensions, we should consider
    # having that mapping tracked somewhere such that it is
    # imported to help prevent breakage.
    for pi, s_li in enumerate(point_data):  # (season_list)
        period = dim_encodings["period"][pi]
        point_data_pkg[period] = {}
        for si, m_li in enumerate(s_li):  # (model list)
            season = dim_encodings["season"][si]
            point_data_pkg[period][season] = {}
            for mi, sc_li in enumerate(m_li):  # (scenario list)
                # verify that model and period combinations are valid before
                #   creating placeholder
                if (pi, mi) not in invalid_model_periods:
                    model = dim_encodings["model"][mi]
                    point_data_pkg[period][season][model] = {}
                    for sci, values in enumerate(sc_li):
                        scenario = dim_encodings["scenario"][sci]
                        # verify that all dim value combinations are valid before
                        #   creating placeholder
                        if (pi, si, mi, sci) not in invalid_dim_values:
                            point_data_pkg[period][season][model][scenario] = {}

                            for variable, value in zip(variables, values.split(" ")):
                                point_data_pkg[period][season][model][scenario][
                                    variable
                                ] = round(float(value), 1)

    return point_data_pkg


def create_csv(packaged_data):
    """
    Returns a CSV version of the fetched data, as a string.

    Args:
        packaged_data (json): output from the package_point_data function here

    Returns:
        string of CSV data
    """
    output = io.StringIO()

    fieldnames = ["variable", "date_range", "season", "model", "scenario", "value"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)

    writer.writeheader()

    for period_key, period in dim_encodings["period"].items():
        for season_key, season in dim_encodings["season"].items():

            if period == "1910-2009":
                writer.writerow(
                    {
                        "variable": "pr",
                        "date_range": period,
                        "season": season,
                        "model": model,
                        "scenario": "Historical",
                        "value": packaged_data[period][season][model]["CRU_historical"][
                            "pr"
                        ],
                    }
                )
                writer.writerow(
                    {
                        "variable": "tas",
                        "date_range": period,
                        "season": season,
                        "model": "CRU-TS31",
                        "scenario": "Historical",
                        "value": packaged_data[period][season][model]["CRU_historical"][
                            "tas"
                        ],
                    }
                )
            else:
                for model_key, model in dim_encodings["model"].items():
                    for scenario_keys, scenario in dim_encodings["scenario"].items():
                        if (scenario == "CRU_historical") or (model == "CRU-TS31"):
                            continue

                        writer.writerow(
                            {
                                "variable": "pr",
                                "date_range": period,
                                "season": season,
                                "model": model,
                                "scenario": scenario,
                                "value": packaged_data[period][season][model][scenario][
                                    "pr"
                                ],
                            }
                        )
                        writer.writerow(
                            {
                                "variable": "tas",
                                "date_range": period,
                                "season": season,
                                "model": model,
                                "scenario": scenario,
                                "value": packaged_data[period][season][model][scenario][
                                    "tas"
                                ],
                            }
                        )

    return output.getvalue()


def aggregate_dataarray(ds, dimensions, poly, transform):
    """Perform a spatial agrgegation of a data array within a polygon.
    Only supports mean aggregation for now.

    Args:
        ds (xarray.DataSet): datacube for all variables
        dimensions (list): string names of variables in DataSet
        poly (shapely.Polygon): polygon from shapefile
        transform (affine.Affine): affine transform raster subset

    Returns:
        results of aggregation as a JSON-like dict
    """

    dim_combos = itertools.product(*[ds[dimension].values for dimension in dimensions])
    # use nested for loop to construct results dict like json output for single point
    aggr_results = {}
    # build aggregate results dict for JSON output
    # hardcoded assuming same 4 dimensions,
    #   consider revising with more robust approach

    for pi in np.int32(ds[dimensions[0]].values):
        period = dim_encodings["period"][pi]
        aggr_results[period] = {}
        for si in np.int32(ds[dimensions[1]].values):
            season = dim_encodings["season"][si]
            aggr_results[period][season] = {}
            for mi in np.int32(ds[dimensions[2]].values):
                # verify that model and period combinations are valid before
                #   creating placeholder
                if (pi, mi) not in invalid_model_periods:
                    model = dim_encodings["model"][mi]
                    aggr_results[period][season][model] = {}
                    for sci in np.int32(ds[dimensions[3]].values):
                        scenario = dim_encodings["scenario"][sci]
                        # select subset and compute aggregate
                        # verify that dim value combination is valid before
                        #   creating placeholder
                        if (pi, si, mi, sci) not in invalid_dim_values:
                            aggr_results[period][season][model][scenario] = {}
                            for val in ["tas", "pr"]:
                                # fmt: off
                                arr = (
                                    ds[val]
                                    .sel(
                                        period=pi,
                                        season=si,
                                        model=mi,
                                        scenario=sci,
                                    )
                                    .values
                                )
                                aggr_result = zonal_stats(
                                    poly,
                                    arr,
                                    affine=transform,
                                    nodata=np.nan,
                                    stats=["mean"],
                                )[0]
                                # fmt: on
                                aggr_results[period][season][model][scenario][
                                    val
                                ] = round(aggr_result["mean"], 1)

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

    point_data = asyncio.run(fetch_point_data(x, y))
    point_pkg = package_point_data(point_data)

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

    else:
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

    # meterological variables as dataset
    met_ds = asyncio.run(fetch_bbox_netcdf(*poly.bounds))

    # aggregate the data and return packaged results
    # compute transform with rioxarray
    met_ds.rio.set_spatial_dims("X", "Y")
    transform = met_ds.rio.transform()
    dimensions = ["period", "season", "model", "scenario"]
    aggr_results = aggregate_dataarray(met_ds, dimensions, poly, transform)
    return aggr_results
