"""Module for look-up-table like objects"""
import os
import pickle
import fiona
import geopandas as gpd
import pandas as pd

host = os.environ.get("API_HOSTNAME") or "https://earthmaps.io"

bbox_offset = 0.000000001

# constants used in vectordata.py to search for named locations
proximity_search_radius_m = (10**5) / 2
community_search_radius_m = 50000
total_bounds_buffer = 0.05

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

json_types = {
    "communities": "data/jsons/ak_communities.json",
    "boroughs": "data/jsons/ak_boroughs.json",
    "census_areas": "data/jsons/ak_census_areas.json",
    "hucs": "data/jsons/ak_hucs.json",
    "huc8s": "data/jsons/ak_huc8.json",
    "huc12s": "data/jsons/ak_huc12.json",
    "protected_areas": "data/jsons/ak_protected_areas.json",
    "fire_zones": "data/jsons/ak_fire_mgmt_zones.json",
    "corporations": "data/jsons/ak_native_corporations.json",
    "climate_divisions": "data/jsons/ak_climate_divisions.json",
    "ethnolinguistic_regions": "data/jsons/ethnolinguistic_regions.json",
    "first_nations": "data/jsons/canada_first_nations.json",
    "game_management_units": "data/jsons/game_management_units.json",
}

place_type_labels = {
    "huc8s": "HUC",
    "protected_areas": "Protected Area",
    "boroughs": "Borough",
    "census_areas": "Census Area",
    "fire_zones": "Fire Management Unit",
    "corporations": "Corporation",
    "climate_divisions": "Climate Division",
    "ethnolinguistic_regions": "Ethnolinguistic Region",
    "first_nations": "Canadian First Nation",
    "game_management_units": "Game Management Unit",
}

# Unused variable for now. Can be used by re-caching function to pre-cache
# all HUC types listed below.
huc_jsons = {json_types["huc8s"], json_types["huc12s"]}

cached_urls = [
    "/alfresco/flammability/area/",
    "/alfresco/flammability/point/",
    "/alfresco/veg_type/area/",
    "/alfresco/veg_type/point/",
    "/beetles/area/",
    "/beetles/point/",
    "/eds/all/",
    "/elevation/area/",
    "/elevation/point/",
    "/taspr/area/",
    "/taspr/point/",
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

# For the forest endpoint.  This file is just a generated pickle
# from the `dbf` file that will be downloaded with the .zip that
# is linked in the documentation page for the point query, including
# only the columns we need for this lookup.
with open("data/luts_pickles/akvegwetlandcomposite.pkl", "rb") as fp:
    ak_veg_di = pickle.load(fp)

try:
    # Below Polygons can be imported by various endpoints
    # HUC-8 # note these are native WGS84 in the #geo-vector repo
    hucs_src = "data/jsons/ak_hucs.json"
    hucs_gdf = gpd.read_file(hucs_src).set_index("id").to_crs(3338)

    print("HUCS")
    # HUC-12
    huc12_src = "data/shapefiles/ak_huc12s.shp"
    huc12_gdf = gpd.read_file(huc12_src).set_index("id").to_crs(3338)
    print("HUCS12")
    # AK Protected Areas
    akpa_src = "data/jsons/ak_protected_areas.json"
    with fiona.open(akpa_src) as src:
        akpa_gdf = gpd.GeoDataFrame.from_features(src).set_index("id").to_crs(3338)
    print("Protected Area")
    # AK Fire Management Zones
    akfire_src = "data/jsons/ak_fire_mgmt_zones.json"
    akfire_gdf = gpd.read_file(akfire_src).set_index("id").to_crs(3338)
    print("Fire")
    # AK Corporations
    akco_src = "data/jsons/ak_native_corporations.json"
    akco_gdf = gpd.read_file(akco_src).set_index("id").to_crs(3338)
    print("Corp")
    # AK Climate Divisions
    akclim_src = "data/jsons/ak_climate_divisions.json"
    akclim_gdf = gpd.read_file(akclim_src).set_index("id").to_crs(3338)
    print("Climate")
    # Ethnolinguistic Regions
    aketh_src = "data/jsons/ethnolinguistic_regions.json"
    aketh_gdf = gpd.read_file(aketh_src).set_index("id").to_crs(3338)
    print("Eth")
    # AK Game Management Units
    akgmu_src = "data/jsons/game_management_units.json"
    akgmu_gdf = gpd.read_file(akgmu_src).set_index("id").to_crs(3338)
    print("GMU")
    # Canadian First Nations
    cafn_src = "data/jsons/canada_first_nations.json"
    cafn_gdf = gpd.read_file(cafn_src).set_index("id").to_crs(3338)
    print("CAFN")
    # Alaska Boroughs
    boro_src = "data/jsons/ak_boroughs.json"
    boro_gdf = gpd.read_file(boro_src).set_index("id").to_crs(3338)
    print("Boro")
    # Unorganized Borough Census Areas
    akcensus_src = "data/jsons/ak_census_areas.json"
    akcensus_gdf = gpd.read_file(akcensus_src).set_index("id").to_crs(3338)
    print("Census")
    # join HUCs into same GeoDataFrame for easier lookup
    huc_gdf = pd.concat(
        [hucs_gdf.reset_index(), huc12_gdf.reset_index()], ignore_index=True
    ).set_index("id")
    valid_huc_ids = huc_gdf.index.values


    type_di = dict()
    type_di["huc"] = hucs_gdf
    type_di["huc12"] = huc12_gdf
    type_di["protected_area"] = akpa_gdf
    type_di["corporation"] = akco_gdf
    type_di["climate_division"] = akclim_gdf
    type_di["ethnolinguistic_region"] = aketh_gdf
    type_di["fire_zone"] = akfire_gdf
    type_di["game_management_unit"] = akgmu_gdf
    type_di["first_nation"] = cafn_gdf
    type_di["borough"] = boro_gdf
    type_di["census_area"] = akcensus_gdf

    update_needed = False
except fiona.errors.DriverError:
    # if this fails, give placeholders until all data can
    # be updated from vectordata.py
    print("It needs to update?")
    update_needed = True
    (
        huc8_gdf,
        huc12_gdf,
        # akpa_gdf,
        akfire_gdf,
        akco_gdf,
        akclim_gdf,
        aketh_gdf,
        akgmu_gdf,
        cafn_gdf,
        huc_gdf,
        boro_gdf,
        akcensus_gdf,
        valid_huc_ids,
    ) = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    type_di = dict()

# look-up for updating place names and data via geo-vector GitHub repo
shp_di = {}
shp_di["akhucs"] = {
    "src_dir": "alaska_hucs",
    "prefix": "ak_hucs",
    "poly_type": "huc",
}
# shp_di["akhuc12s"] = {
#     "src_dir": "alaska_hucs",
#     "prefix": "ak_huc12s",
#     "poly_type": "huc12",
#     "retain": [],
# }
shp_di["ak_pa"] = {
    "src_dir": "protected_areas/ak_protected_areas",
    "prefix": "ak_protected_areas",
    "poly_type": "protected_area",
    "retain": "area_type",
}
shp_di["akfire"] = {
    "src_dir": "fire",
    "prefix": "ak_fire_management",
    "poly_type": "fire_zone",
}
shp_di["akcorps"] = {
    "src_dir": "corporation",
    "prefix": "ak_native_corporations",
    "poly_type": "corporation",
}
shp_di["akethno"] = {
    "src_dir": "ethnolinguistic",
    "prefix": "ethnolinguistic_regions",
    "poly_type": "ethnolinguistic_region",
    "retain": "alt_name",
}
shp_di["akclimdivs"] = {
    "src_dir": "climate_divisions",
    "prefix": "ak_climate_divisions",
    "poly_type": "climate_division",
}
shp_di["akgmus"] = {
    "src_dir": "game_management_units",
    "prefix": "ak_gmu",
    "poly_type": "game_management_unit",
}
shp_di["cnfns"] = {
    "src_dir": "first_nations",
    "prefix": "first_nation_traditional_territories",
    "poly_type": "first_nation",
}
shp_di["akboros"] = {
    "src_dir": "boroughs",
    "prefix": "ak_boroughs",
    "poly_type": "borough",
}
shp_di["akcensusareas"] = {
    "src_dir": "census_areas",
    "prefix": "ak_census_areas",
    "poly_type": "census_area",
}
