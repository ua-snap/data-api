import asyncio
from flask import abort, Blueprint, render_template
from . import routes
from validate_latlon import validate
from fetch_data import (
    fetch_layer_data,
    generate_query_urls,
    generate_base_wms_url,
    generate_base_wfs_url,
    fetch_data_api,
    check_for_nodata,
)
from luts import landcover_names, smokey_bear_names, smokey_bear_styles, snow_status
from config import GS_BASE_URL

fire_api = Blueprint("fire_api", __name__)

wms_targets = [
    "alaska_landcover_2015",
    "spruceadj_3338",
    "snow_cover_3338",
    "alfresco_relative_flammability_NCAR-CCSM4_rcp85_2000_2099",
]
wfs_targets = {"historical_fire_perimiters": "NAME,FIREYEAR"}


def package_fire_history(fihist_resp):
    """Package fire history data in dict"""
    title = "Historical fires"
    if fihist_resp["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
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
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        flamm = round(flammability_resp["features"][0]["properties"]["GRAY_INDEX"], 4)
        di = {"title": title, "flamm": flamm}
        check_for_nodata(di, "flamm", flamm, -9999)
    return di


def package_snow(snow_resp):
    """Package snow cover data"""
    title = "Today's Snow Cover"
    if snow_resp["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        snow = snow_status[snow_resp["features"][0]["properties"]["GRAY_INDEX"]]
        di = {"title": title, "is_snow": snow}
    return di


def package_fire_danger(fire_danger_resp):
    """Package fire danger data in dict"""
    title = "Today's Fire Danger"
    if fire_danger_resp["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        code = fire_danger_resp["features"][0]["properties"]["GRAY_INDEX"]
        fitype = smokey_bear_names[code]
        color = smokey_bear_styles[code]
        di = {"title": title, "code": code, "type": fitype, "color": color}
    return di


def package_landcover(landcover_resp):
    """Package landcover data in dict"""
    title = "Land cover types"
    if landcover_resp["features"] == []:
        di = {"title": title, "Data Status": "No data at this location."}
    else:
        code = landcover_resp["features"][0]["properties"]["PALETTE_INDEX"]
        lctype = landcover_names[code]["type"]
        color = landcover_names[code]["color"]
        di = {"title": title, "code": code, "type": lctype, "color": color}
    return di


@routes.route("/fire/")
@routes.route("/fire/abstract/")
def fire_about():
    return render_template("fire/abstract.html")


@routes.route("/fire/point/")
def fire_about_point():
    return render_template("fire/point.html")


@routes.route("/fire/point/<lat>/<lon>")
def run_fetch_fire(lat, lon):
    """Run the async requesting and return data
    example request: http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
    """
    if not validate(lat, lon):
        abort(400)
    # verify that lat/lon are present
    results = asyncio.run(
        fetch_data_api(
            GS_BASE_URL, "alaska_wildfires", wms_targets, wfs_targets, lat, lon
        )
    )
    landcover = package_landcover(results[0])
    firedanger = package_fire_danger(results[1])
    snow = package_snow(results[2])
    relflammability = package_flammability(results[3])
    firehist = package_fire_history(results[4])
    data = {
        "lc": landcover,
        "is_snow": snow,
        "cfd": firedanger,
        "hist_fire": firehist,
        "prf": relflammability,
    }
    return data
