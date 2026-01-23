import asyncio
import io
import numpy as np
import xarray as xr
import ast
import pandas as pd
from datetime import datetime
import geopandas as gpd
import copy
from aiohttp import ClientSession
from flask import (
    Blueprint,
    Response,
    render_template,
    request,
    current_app as app,
    jsonify,
)

from generate_requests import generate_conus_hydrology_wcs_str
from generate_urls import (
    generate_wfs_conus_hydrology_url,
    generate_usgs_gauge_daily_streamflow_data_url,
    generate_usgs_gauge_metadata_url,
)
from fetch_data import fetch_data, fetch_layer_data, describe_via_wcps
from validate_request import get_axis_encodings
from postprocessing import prune_nulls_with_max_intensity
from csv_functions import create_csv
from config import RAS_BASE_URL
from . import routes


@routes.route("/arctic_hydrology/")
def arctic_hydrology_about():
    return render_template("/documentation/arctic_hydrology.html")
