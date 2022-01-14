"""Module for look-up-table like objects"""
import pickle
import geopandas as gpd

bbox_offset = 0.000000001

landcover_names = {
    0: {"type": "No Data at this location.", "color": "#ffffff"},
    1: {"type": "Temperate or sub-polar needleleaf forest", "color": "#003d00"},
    2: {"type": "Sub-polar taiga or needleleaf forest", "color": "#949c70"},
    5: {
        "type": "Temperate or sub-polar broadleaf deciduous forest",
        "color": "#148c3d",
    },
    6: {"type": "Mixed forest", "color": "#5c752b"},
    8: {"type": "Temperate or sub-polar shrubland", "color": "#b38a33"},
    10: {"type": "Temperate or sub-polar grassland", "color": "#e1cf8a"},
    11: {"type": "Sub-polar or polar shrubland-lichen-moss", "color": "#9c7554"},
    12: {"type": "Sub-polar or polar grassland-lichen-moss", "color": "#bad48f"},
    13: {"type": "Sub-polar or polar barren-lichen-moss", "color": "#408a70"},
    14: {"type": "Wetland", "color": "#6ba38a"},
    15: {"type": "Cropland", "color": "#e6ae66"},
    16: {"type": "Barren land", "color": "#a8abae"},
    17: {"type": "Urban and built-up", "color": "#DD40D6"},
    18: {"type": "Water", "color": "#4c70a3"},
    19: {"type": "Snow and ice", "color": "#eee9ee"},
}
smokey_bear_names = {
    1: "Low",
    2: "Medium",
    3: "High",
    4: "Very High",
    5: "Extreme",
    6: "No data at this location.",
}
smokey_bear_styles = {
    1: "#2b83ba",
    2: "#abdda4",
    3: "#ffffbf",
    4: "#fdae61",
    5: "#d7191c",
    6: "#ffffff",
}
snow_status = {
    1: "Sea",
    2: False,
    3: "Sea ice",
    4: True,
    0: "No data at this location.",
}

permafrost_encodings = {
    "eras": {0: "1995", 1: "2025", 2: "2050", 3: "2075", 4: "2095"},
    "models": {
        0: "cruts31",
        1: "gfdlcm3",
        2: "gisse2r",
        3: "ipslcm5alr",
        4: "mricgcm3",
        5: "ncarccsm4",
    },
    "scenarios": {0: "historical", 1: "rcp45", 2: "rcp85"},
    "rounding": {"magt": 1, "alt": 1},
    "gipl_varnames": ["magt", "alt"],
    "gipl_era_starts": ["1986", "2011", "2036", "2061", "2086"],
    "gipl_era_ends": ["2005", "2040", "2065", "2090", "2100"],
    "gipl_units_lu": {"magt": "°C", "alt": "m"},
}

# For the forest endpoint.  This file is just a generated pickle
# from the `dbf` file that will be downloaded with the .zip that
# is linked in the documentation page for the point query, including
# only the columns we need for this lookup.
with open("data/luts_pickles/akvegwetlandcomposite.pkl", "rb") as fp:
    ak_veg_di = pickle.load(fp)

# HUC-8 Polygons can be imported by various endpoints
huc_src = "data/shapefiles/hydrologic_units\wbdhu8_a_ak.shp"
huc8_gdf = gpd.read_file(huc_src).set_index("huc8")

# AK Protected Area Polygons can be imported by various endpoints
akpa_src = "data/shapefiles/ak_protected_areas.shp"
akpa_gdf = gpd.read_file(akpa_src).set_index("id")
