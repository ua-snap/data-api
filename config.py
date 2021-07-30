"""
Configuration common to multiple routes.
"""
import os

GS_BASE_URL = os.getenv("API_GS_BASE_URL") or "https://gs.mapventure.org/geoserver/"
