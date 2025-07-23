"""
Configuration common to multiple routes.
"""

import os

GS_BASE_URL = os.getenv("API_GS_BASE_URL") or "https://gs.earthmaps.io/geoserver/"
RAS_BASE_URL = os.getenv("API_RAS_BASE_URL") or "https://zeus.snap.uaf.edu/rasdaman/"
WEST_BBOX = [-180, 51.3492, -122.8098, 71.3694]
EAST_BBOX = [172.4201, 51.3492, 180, 71.3694]
SEAICE_BBOX = [-180, 30.98, 180, 90]
INDICATORS_BBOX = [0, 49.94, 359.37, 90]
WEB_APP_URL = os.getenv("WEB_APP_URL") or "https://northernclimatereports.org/"

if os.getenv("SITE_OFFLINE"):
    SITE_OFFLINE = os.getenv("SITE_OFFLINE").lower() == "true"
else:
    SITE_OFFLINE = False

geojson_names = ["alaska", "blockyAlaska", "elevation", "mizukami", "slie"]