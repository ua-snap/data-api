import asyncio
import logging
import time
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from fetch_data import fetch_data, fetch_geoserver_data
from generate_urls import generate_wfs_search_url
from validate_request import validate_latlon
from postprocessing import nullify_nodata, postprocess
from config import GS_BASE_URL, WEST_BBOX, EAST_BBOX
from luts import landcover_names, smokey_bear_names, smokey_bear_styles, snow_status
from . import routes

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

fire_api = Blueprint("fire_api", __name__)

wms_targets = [
    "alaska_landcover_2015",
    "spruceadj_3338",
    "snow_cover_3338",
    "alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099",
    "aqi_forecast_6_hrs",
    "aqi_forecast_12_hrs",
    "aqi_forecast_24_hrs",
    "aqi_forecast_48_hrs",
]
wfs_targets = {"historical_fire_perimeters": "NAME,FIREYEAR"}


def package_aqi_forecast(aqi_forecast_resp):
    """Package AQI forecast data in dict"""
    if aqi_forecast_resp["features"] == []:
        return None
    pm25_conc = aqi_forecast_resp["features"][0]["properties"]["PM2.5_Concentration"]
    aqi = aqi_forecast_resp["features"][0]["properties"]["AQI"]
    if pm25_conc is None or aqi is None:
        return None
    di = {"pm25_conc": round(pm25_conc), "aqi": round(aqi)}
    return di


def package_fire_history(fihist_resp):
    """Package fire history data in dict"""
    if fihist_resp["features"] == []:
        return None
    di = {}
    for i in fihist_resp["features"]:
        fi_name = list(i.values())[-1]["NAME"]
        fi_year = list(i.values())[-1]["FIREYEAR"]
        di.update({fi_name: fi_year})
    return di


def package_flammability(flammability_resp):
    """Package flammability data in dict"""
    title = "Projected relative flammability"
    if flammability_resp["features"] == []:
        return None
    flamm = round(flammability_resp["features"][0]["properties"]["GRAY_INDEX"], 4)
    flamm = nullify_nodata(flamm, "fire")
    if flamm is None:
        return None
    di = {"title": title, "flamm": flamm}
    return di


def package_snow(snow_resp):
    """Package snow cover data"""
    title = "Today's Snow Cover"
    if snow_resp["features"] == []:
        return None
    snow_index = snow_resp["features"][0]["properties"]["GRAY_INDEX"]
    if snow_index == 0:
        return None
    snow = snow_status[snow_index]
    di = {"title": title, "is_snow": snow}
    return di


def package_fire_danger(fire_danger_resp):
    """Package fire danger data in dict"""
    title = "Today's Fire Danger"
    if fire_danger_resp["features"] == []:
        return None
    code = fire_danger_resp["features"][0]["properties"]["GRAY_INDEX"]
    if code == 6:
        return None
    fitype = smokey_bear_names[code]
    color = smokey_bear_styles[code]
    di = {"title": title, "code": code, "type": fitype, "color": color}
    return di


def package_landcover(landcover_resp):
    """Package landcover data in dict"""
    title = "Land cover types"
    if landcover_resp["features"] == []:
        return None
    code = landcover_resp["features"][0]["properties"]["PALETTE_INDEX"]
    lctype = landcover_names[code]["type"]
    if code == 0:
        return None
    color = landcover_names[code]["color"]
    di = {"title": title, "code": code, "type": lctype, "color": color}
    return di


@routes.route("/fire/")
@routes.route("/fire/abstract/")
@routes.route("/fire/point/")
def fire_about():
    start_time = time.time()
    logger.info(f"Fire about endpoint accessed: {request.path}")
    response = render_template("documentation/fire.html")
    elapsed = time.time() - start_time
    logger.info(f"Fire about endpoint response in {elapsed:.3f} seconds")
    return response


@routes.route("/fire/point/<lat>/<lon>")
def run_fetch_fire(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
    """
    start_time = time.time()
    logger.info(f"Fire point endpoint accessed: lat={lat}, lon={lon}")
    validation = validate_latlon(lat, lon)
    if validation == 400:
        elapsed = time.time() - start_time
        logger.warning(
            f"Bad request for fire point: lat={lat}, lon={lon} (in {elapsed:.3f} seconds)"
        )
        return render_template("400/bad_request.html"), 400
    if validation == 422:
        elapsed = time.time() - start_time
        logger.warning(
            f"Invalid lat/lon for fire point: lat={lat}, lon={lon} (in {elapsed:.3f} seconds)"
        )
        return (
            render_template(
                "422/invalid_latlon.html", west_bbox=WEST_BBOX, east_bbox=EAST_BBOX
            ),
            422,
        )
    try:
        results = asyncio.run(
            fetch_geoserver_data(
                GS_BASE_URL, "alaska_wildfires", wms_targets, wfs_targets, lat, lon
            )
        )
        fire_points = asyncio.run(
            fetch_data(
                [
                    generate_wfs_search_url(
                        "alaska_wildfires:fire_points", lat, lon, nearby_fires=True
                    )
                ]
            )
        )
        fire_polygons = asyncio.run(
            fetch_data(
                [
                    generate_wfs_search_url(
                        "alaska_wildfires:fire_polygons",
                        lat,
                        lon,
                        nearby_fires=True,
                    )
                ]
            )
        )
    except Exception as exc:
        elapsed = time.time() - start_time
        if hasattr(exc, "status") and exc.status == 404:
            logger.warning(
                f"No data for fire point: lat={lat}, lon={lon} (in {elapsed:.3f} seconds)"
            )
            return render_template("404/no_data.html"), 404
        logger.error(
            f"Error in fire point fetch: lat={lat}, lon={lon}, error={exc} (in {elapsed:.3f} seconds)"
        )
        return render_template("500/server_error.html"), 500
    landcover = package_landcover(results[0])
    firedanger = package_fire_danger(results[1])
    snow = package_snow(results[2])
    relflammability = package_flammability(results[3])
    aqi_forecast_6_hrs = package_aqi_forecast(results[4])
    aqi_forecast_12_hrs = package_aqi_forecast(results[5])
    aqi_forecast_24_hrs = package_aqi_forecast(results[6])
    aqi_forecast_48_hrs = package_aqi_forecast(results[7])
    firehist = package_fire_history(results[8])
    data = {
        "lc": landcover,
        "is_snow": snow,
        "cfd": firedanger,
        "hist_fire": firehist,
        "aqi_6": aqi_forecast_6_hrs,
        "aqi_12": aqi_forecast_12_hrs,
        "aqi_24": aqi_forecast_24_hrs,
        "aqi_48": aqi_forecast_48_hrs,
        "prf": relflammability,
        "fire_points": fire_points["features"],
        "fire_polygons": fire_polygons["features"],
    }
    elapsed = time.time() - start_time
    logger.info(
        f"Fire point fetch returned JSON: lat={lat}, lon={lon} (in {elapsed:.3f} seconds)"
    )
    return postprocess(data, "fire")
