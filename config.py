"""
Configuration common to multiple routes.
"""
import os

GS_BASE_URL = os.getenv("API_GS_BASE_URL") or "https://gs.mapventure.org/geoserver/"
RAS_BASE_URL = (
    os.getenv("API_RAS_BASE_URL") or "http://zeus.snap.uaf.edu:8080/rasdaman/"
)
WEST_BBOX = [-180, 51.3492, -122.8098, 71.3694]
EAST_BBOX = [179.7607, 51.3492, 180, 71.3694]