import asyncio
import numpy as np
import geopandas as gpd
from aiohttp import ClientSession
from flask import abort, Blueprint, render_template
from validate_latlon import validate, validate_bbox, project_latlon
from . import routes
from config import RAS_BASE_URL
from fiona.errors import DriverError

try:
    huc_gdf = gpd.read_file("data/shapefiles/hydrologic_units\wbdhu8_a_ak.shp")
    iem_api = Blueprint("iem_api", __name__)
except DriverError:
    print("Blueprint object 'iem_api' was not created.")


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

    async with ClientSession() as session:
        tasks = [fetch_layer_data(url, session) for url in urls]
        results = await asyncio.gather(*tasks)

    return results


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

                    # elif isinstance(values, list):
                    #     iem_pkg[period][season][model][scenario] = values

    return iem_pkg


@routes.route("/iem/")
@routes.route("/iem/about/")
def about():
    return render_template("iem.html")


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


@routes.route("/iem/bbox/<lat1>/<lon1>/<lat2>/<lon2>")
def run_fetch_iem_bbox_data(lat1, lon1, lat2, lon2):
    """Run the ansync IEM data requesting for a bounding box
    and return data as json

    example request: http://localhost:5000/iem/bbox/65/-145.5/65.5/-145
    """
    if not validate_bbox(lat1, lon1, lat2, lon2):
        abort(400)

    x1, y1, x2, y2 = project_latlon(lat1, lon1, 3338, lat2, lon2)

    results = asyncio.run(fetch_iem_data(x1, y1, x2, y2))

    iem = package_iem(results[0])

    return iem


@routes.route("/iem/huc/<huc_id>/<stats>")
def run_fetch_iem_aggregate_huc(huc_id, stats):
    """Get data within a huc and aggregate according to
    stat methods in <stats>
    """
    pass
