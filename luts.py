"""Module for look-up-table like objects"""
import os

host = os.environ.get("API_HOSTNAME") or "https://earthmaps.io"

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
    2: "Moderate",
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

place_type_labels = {
    "huc": "HUC",
    "protected_area": "Protected Area",
    "borough": "Borough",
    "census_area": "Census Area",
    "fire_zone": "Fire Management Unit",
    "corporation": "Corporation",
    "climate_division": "Climate Division",
    "ethnolinguistic_region": "Ethnolinguistic Region",
    "first_nation": "Canadian First Nation",
    "game_management_unit": "Game Management Unit",
}

cached_urls = [
    "/eds/all/",
    "/alfresco/flammability/local/",
    "/alfresco/veg_type/local/",
    "/beetles/point/",
    "/elevation/point/",
    "/taspr/point/",
    "/indicators/base/point/",
    "/ncr/permafrost/point/",
    "/eds/hydrology/point/",
    "/alfresco/flammability/area/",
    "/alfresco/veg_type/area/",
    "/beetles/area/",
    "/elevation/area/",
    "/taspr/area/",
    "/indicators/base/area/",
]

# Used for generating output for vector_data for /places/all
all_jsons = [
    "communities",
    "hucs",
    "protected_areas",
    "corporations",
    "climate_divisions",
    "ethnolinguistic_regions",
    "fire_zones",
    "game_management_units",
    "first_nations",
    "boroughs",
    "census_areas",
]

# Look-up table for expected value for the NCR application.
# TODO: Change these in NCR so we don't need this LUT.
areas_near = {
    "borough": "ak_boros_near",
    "census_area": "ak_censusarea_near",
    "climate_division": "climate_divisions_near",
    "corporation": "corporations_near",
    "ethnolinguistic_region": "ethnolinguistic_regions_near",
    "fire_zone": "fire_management_units_near",
    "game_management_unit": "game_management_units_near",
    "first_nation": "ca_first_nations_near",
    "huc": "hucs_near",
    "protected_area": "protected_areas_near",
}
