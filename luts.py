"""Module for look-up-table like objects"""

import os

host = os.environ.get("API_HOSTNAME") or "https://earthmaps.io"

bbox_offset = 0.000000001

# Coverages with non-EPSG:3338 projections
geotiff_projections = {
    "hsia_arctic_production": "EPSG:3572",
}

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
    "yt_watershed": "Yukon Watershed",
    "protected_area": "Protected Area",
    "borough": "Borough",
    "census_area": "Census Area",
    "fire_zone": "Fire Management Unit",
    "yt_fire_district": "Yukon Fire District",
    "corporation": "Corporation",
    "climate_division": "Climate Division",
    "ethnolinguistic_region": "Ethnolinguistic Region",
    "first_nation": "Canadian First Nation",
    "game_management_unit": "Game Management Unit",
    "yt_game_management_subzone": "Yukon Game Management Subzone",
    "ecoregion": "Ecoregion",
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
    "yt_watersheds",
    "protected_areas",
    "corporations",
    "climate_divisions",
    "ethnolinguistic_regions",
    "fire_zones",
    "yt_fire_districts",
    "game_management_units",
    "yt_game_management_subzones",
    "first_nations",
    "boroughs",
    "census_areas",
    "ecoregions",
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
    "yt_fire_district": "yt_fire_districts_near",
    "game_management_unit": "game_management_units_near",
    "yt_game_management_subzone": "yt_game_management_subzones_near",
    "first_nation": "ca_first_nations_near",
    "huc": "hucs_near",
    "yt_watershed": "yt_watersheds_near",
    "protected_area": "protected_areas_near",
    "ecoregion": "ecoregions_near",
}

# table to decode field names for demographic data from GeoServer
# NOTE: fields that were not truncated do not appear here!
# see data dictionary in the repo for more info: https://github.com/ua-snap/epa-justice/blob/main/README.md
demographics_fields = {
    "pct_65_plu": "pct_65_plus",
    "pct_africa": "pct_african_american",
    "pct_amer_i": "pct_amer_indian_ak_native",
    "pct_below_": "pct_below_150pov",
    "pct_diabet": "pct_diabetes",
    "pct_hawaii": "pct_hawaiian_pacislander",
    "pct_hispan": "pct_hispanic_latino",
    "pct_insure": "pct_insured",
    "pct_minori": "pct_minority",
    "pct_no_bba": "pct_no_bband",
    "pct_no_hsd": "pct_no_hsdiploma",
    "pct_under_": "pct_under_18",
    "pct_unde_1": "pct_under_5",
    "pct_uninsu": "pct_uninsured",
    "pct_w_disa": "pct_w_disability",
    "pct_foodst": "pct_foodstamps",
    "pct_crowdi": "pct_crowding",
    "pct_single": "pct_single_parent",
    "pct_unempl": "pct_unemployed",
    "total_popu": "total_population",
    # the fields below are low and high confidence intervals (90% CI values)
    "pct_asth_1": "pct_asthma_low",
    "pct_asth_2": "pct_asthma_high",
    "pct_copd_l": "pct_copd_low",
    "pct_copd_h": "pct_copd_high",
    "pct_diab_1": "pct_diabetes_low",
    "pct_diab_2": "pct_diabetes_high",
    "pct_emos_1": "pct_emospt_low",
    "pct_emos_2": "pct_emospt_high",
    "pct_food_1": "pct_foodstamps_low",
    "pct_food_2": "pct_foodstamps_high",
    "pct_mh_hig": "pct_mh_high",
    "pct_hd_hig": "pct_hd_high",
    "pct_stro_1": "pct_stroke_low",
    "pct_stro_2": "pct_stroke_high",
    "pct_w_di_1": "pct_w_disability_high",
    "pct_w_di_2": "pct_w_disability_low",
    "pct_insu_1": "pct_insured_high",
    "pct_insu_2": "pct_insured_low",
    "pct_unin_1": "pct_uninsured_high",
    "pct_unin_2": "pct_uninsured_low",
    "pct_no_b_1": "pct_no_bband_high",
    "pct_no_b_2": "pct_no_bband_low",
    "pct_crow_1": "pct_crowding_high",
    "pct_crow_2": "pct_crowding_low",
    "pct_hcost_": "pct_hcost_high",
    "pct_hcos_1": "pct_hcost_low",
    "pct_no_h_1": "pct_no_hsdiploma_high",
    "pct_no_h_2": "pct_no_hsdiploma_low",
    "pct_belo_1": "pct_below_150pov_high",
    "pct_belo_2": "pct_below_150pov_low",
    "pct_mino_1": "pct_minority_high",
    "pct_mino_2": "pct_minority_low",
    "pct_sing_1": "pct_single_parent_high",
    "pct_sing_2": "pct_single_parent_low",
    "pct_unem_1": "pct_unemployed_high",
    "pct_unem_2": "pct_unemployed_low",
}


demographics_descriptions = {
    # population, age, and race
    "name": {
        "description": "",
        "source": "",
    },
    "comment": {
        "description": "",
        "source": "",
    },
    "total_population": {
        "description": "total_population is the total population of the community",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_under_18": {
        "description": "pct_under_18 is the percentage of the population under age 18; this value was calculated by summing the population count of multiple sex by age categories and expressing that sum as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_under_5": {
        "description": "pct_under_5 is the percentage of the population under age 5; this value was calculated by summing the population count of multiple sex by age categories and expressing that sum as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_65_plus": {
        "description": "pct_65_plus is the percentage of the population age 65 and older; this value was calculated by summing the population count of multiple sex by age categories and expressing that sum as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_african_american": {
        "description": "pct_african_american is the percentage of the population that is African American; this value was calculated by taking the population count of African Americans and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_amer_indian_ak_native": {
        "description": "pct_amer_indian_ak_native is the percentage of the population that is American Indian or Alaska Native; this value was calculated by taking the population count of American Indians or Alaska Natives and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_asian": {
        "description": "pct_asian is the percentage of the population that is Asian; this value was calculated by taking the population count of Asians and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_hawaiian_pacislander": {
        "description": "pct_hawaiian_pacislander is the percentage of the population that is Native Hawaiian and Pacific Islander; this value was calculated by taking the population count of Native Hawaiians and Pacific Islanders and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_hispanic_latino": {
        "description": "pct_hispanic_latino is the percentage of the population that is Hispanic or Latino; this value was calculated by taking the population count of Hispanics or Latinos and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_white": {
        "description": "pct_white is the percentage of the population that is White; this value was calculated by taking the population count of Whites and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_multi": {
        "description": "pct_multi is the percentage of the population that is two or more races; this value was calculated by taking the population count of two or more races and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    "pct_other": {
        "description": "pct_other is the percentage of the population that is other race; this value was calculated by taking the population count of other races and expressing that count as a percentage of the total population",
        "source": "U.S. Census Demographic and Housing Characteristics Survey for 2020",
    },
    # health conditions
    "pct_asthma": {
        "description": "pct_asthma is the percentage of adults aged >=18 years who report being diagnosed with and currently having asthma; this value is a crude prevalence rate",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_asthma_low": {
        "description": "pct_asthma_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with and currently having asthma",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_asthma_high": {
        "description": "pct_asthma_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with and currently having asthma",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_copd": {
        "description": "pct_copd is the percentage of adults aged >=18 years who report being diagnosed with chronic obstructive pulmonary disease (COPD), emphysema, or chronic bronchitis",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_copd_low": {
        "description": "pct_copd_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with chronic obstructive pulmonary disease (COPD), emphysema, or chronic bronchitis",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_copd_high": {
        "description": "pct_copd_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with chronic obstructive pulmonary disease (COPD), emphysema, or chronic bronchitis",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_diabetes": {
        "description": "pct_diabetes is the percentage of adults aged >=18 years who report being diagnosed with diabetes (excluding diabetes during pregnancy/gestational diabetes); this value is a crude prevalence rate",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_diabetes_low": {
        "description": "pct_diabetes_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with diabetes (excluding diabetes during pregnancy/gestational diabetes)",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_diabetes_high": {
        "description": "pct_diabetes_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with diabetes (excluding diabetes during pregnancy/gestational diabetes)",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_hd": {
        "description": "pct_hd is the percentage of adults aged >=18 years who report being diagnosed with coronary heart disease; this value is a crude prevalence rate",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_hd_low": {
        "description": "pct_hd_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with coronary heart disease",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_hd_high": {
        "description": "pct_hd_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report being diagnosed with coronary heart disease",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_mh": {
        "description": "pct_mh is the percentage of adults aged >=18 years who report having 'frequent mental distress' (mental health including stress, depression, and problems with emotions, was not good for 14 or more days during the past 30 days); this value is a crude prevalence rate",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_mh_low": {
        "description": "pct_mh_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report having 'frequent mental distress' (mental health including stress, depression, and problems with emotions, was not good for 14 or more days during the past 30 days)",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_mh_high": {
        "description": "pct_mh_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report having 'frequent mental distress' (mental health including stress, depression, and problems with emotions, was not good for 14 or more days during the past 30 days)",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_stroke": {
        "description": "pct_stroke is the percentage of adults aged >=18 years who report having ever been told by a doctor, nurse, or other health professional that they have had a stroke; this value is a crude prevalence rate",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_stroke_low": {
        "description": "pct_stroke_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report having ever been told by a doctor, nurse, or other health professional that they have had a stroke",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_stroke_high": {
        "description": "pct_stroke_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report having ever been told by a doctor, nurse, or other health professional that they have had a stroke",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_emospt": {
        "description": "pct_emospt is the percentage of adults aged >=18 years who report 'lack of social and emotional support' (self-report sometimes, rarely, or never getting the social and emotional support needed); this value is a crude prevalence rate",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_emospt_low": {
        "description": "pct_emospt_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years who report 'lack of social and emotional support' (self-report sometimes, rarely, or never getting the social and emotional support needed)",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_emospt_high": {
        "description": "pct_emospt_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years who report 'lack of social and emotional support' (self-report sometimes, rarely, or never getting the social and emotional support needed)",
        "source": "CDC PLACES dataset for 2024",
    },
    # social determinants of health
    "pct_minority": {
        "description": "pct_minority is the percentage of the population of racial or ethnic minority status (including individuals who identified as any of the following: Hispanic or Latino (any race); Black and African American, non-Hispanic; American Indian and Alaska Native, non-Hispanic; Asian, non-Hispanic; Native Hawaiian and Other Pacific Islander, non-Hispanic; Two or More Races, non-Hispanic; Other Races, non-Hispanic)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_minority_low": {
        "description": "pct_minority_low is the lower bound of the 90% confidence interval for percentage of the population of racial or ethnic minority status (including individuals who identified as any of the following: Hispanic or Latino (any race); Black and African American, non-Hispanic; American Indian and Alaska Native, non-Hispanic; Asian, non-Hispanic; Native Hawaiian and Other Pacific Islander, non-Hispanic; Two or More Races, non-Hispanic; Other Races, non-Hispanic)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_minority_high": {
        "description": "pct_minority_high is the upper bound of the 90% confidence interval for percentage of the population of racial or ethnic minority status (including individuals who identified as any of the following: Hispanic or Latino (any race); Black and African American, non-Hispanic; American Indian and Alaska Native, non-Hispanic; Asian, non-Hispanic; Native Hawaiian and Other Pacific Islander, non-Hispanic; Two or More Races, non-Hispanic; Other Races, non-Hispanic)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_foodstamps": {
        "description": "pct_foodstamps is the percentage of adults aged >=18 years that received food stamps in the past 12 months",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_foodstamps_low": {
        "description": "pct_foodstamps_low is the lower bound of the 90% confidence interval for percentage of adults aged >=18 years that received food stamps in the past 12 months",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_foodstamps_high": {
        "description": "pct_foodstamps_high is the upper bound of the 90% confidence interval for percentage of adults aged >=18 years that received food stamps in the past 12 months",
        "source": "CDC PLACES dataset for 2024",
    },
    "pct_w_disability": {
        "description": "pct_w_disability is the percentage of the population with a reported disability (presence of six types of disability related to serious difficulty including: hearing, vision, concentrating, remembering or making decisions (i.e. cognition), walking or climbing stairs (i.e. mobility), dressing or bathing (i.e., self-care), and doing errands alone (i.e., independent living))",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_w_disability_low": {
        "description": "pct_w_disability_low is the lower bound of the 90% confidence interval for percentage of the population with a reported disability (presence of six types of disability related to serious difficulty including: hearing, vision, concentrating, remembering or making decisions (i.e. cognition), walking or climbing stairs (i.e. mobility), dressing or bathing (i.e., self-care), and doing errands alone (i.e., independent living))",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_w_disability_high": {
        "description": "pct_w_disability_high is the upper bound of the 90% confidence interval for percentage of the population with a reported disability (presence of six types of disability related to serious difficulty including: hearing, vision, concentrating, remembering or making decisions (i.e. cognition), walking or climbing stairs (i.e. mobility), dressing or bathing (i.e., self-care), and doing errands alone (i.e., independent living))",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_insured": {
        "description": "pct_insured is the percentage of the population with health insurance",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_insured_low": {
        "description": "pct_insured_low is the lower bound of the 90% confidence interval for percentage of the population with health insurance",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_insured_high": {
        "description": "pct_insured_high is the upper bound of the 90% confidence interval for percentage of the population with health insurance",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_uninsured": {
        "description": "pct_uninsured is the percentage of the population without health insurance",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_uninsured_low": {
        "description": "pct_uninsured_low is the lower bound of the 90% confidence interval for percentage of the population without health insurance",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_uninsured_high": {
        "description": "pct_uninsured_high is the upper bound of the 90% confidence interval for percentage of the population without health insurance",
        "source": "U.S. Census American Community Survey 5-year estimates for years 2019-2023",
    },
    "pct_no_bband": {
        "description": "pct_no_bband is the percentage of households with no broadband internet subscription",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_no_bband_low": {
        "description": "pct_no_bband_low is the lower bound of the 90% confidence interval for percentage of households with no broadband internet subscription",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_no_bband_high": {
        "description": "pct_no_bband_high is the upper bound of the 90% confidence interval for percentage of households with no broadband internet subscription",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_no_hsdiploma": {
        "description": "pct_no_hsdiploma is the percentage of adults aged >=25 years with no high school diploma",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_no_hsdiploma_low": {
        "description": "pct_no_hsdiploma_low is the lower bound of the 90% confidence interval for percentage of adults aged >=25 years with no high school diploma",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_no_hsdiploma_high": {
        "description": "pct_no_hsdiploma_high is the upper bound of the 90% confidence interval for percentage of adults aged >=25 years with no high school diploma",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_below_150pov": {
        "description": "pct_below_150pov is the percentage of population living below 150% of the federal poverty threshold",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_below_150pov_low": {
        "description": "pct_below_150pov_low is the lower bound of the 90% confidence interval for percentage of population living below 150% of the federal poverty threshold",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_below_150pov_high": {
        "description": "pct_below_150pov_high is the upper bound of the 90% confidence interval for percentage of population living below 150% of the federal poverty threshold",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_crowding": {
        "description": "pct_crowding is the percentage of households with 'crowding' (occupied housing units with 1.01 to 1.50 and 1.51 or more occupants per room)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_crowding_low": {
        "description": "pct_crowding_low is the lower bound of the 90% confidence interval for percentage of households with 'crowding' (occupied housing units with 1.01 to 1.50 and 1.51 or more occupants per room)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_crowding_high": {
        "description": "pct_crowding_high is the upper bound of the 90% confidence interval for percentage of households with 'crowding' (occupied housing units with 1.01 to 1.50 and 1.51 or more occupants per room)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_hcost": {
        "description": "pct_hcost is the percentage of households with 'housing cost burden' (households with annual income less than $75,000 that spend 30% or more of their household income on housing)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_hcost_low": {
        "description": "pct_hcost_low is the lower bound of the 90% confidence interval for percentage of households with 'housing cost burden' (households with annual income less than $75,000 that spend 30% or more of their household income on housing)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_hcost_high": {
        "description": "pct_hcost_high is the upper bound of the 90% confidence interval for percentage of households with 'housing cost burden' (households with annual income less than $75,000 that spend 30% or more of their household income on housing)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_unemployed": {
        "description": "pct_unemployed is the percentage of the population >= 16 years in the civilian labor force who are unemployed (jobless but are available to work and have actively looked for work in the past 4 weeks)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_unemployed_low": {
        "description": "pct_unemployed_low is the lower bound of the 90% confidence interval for percentage of the population >= 16 years in the civilian labor force who are unemployed (jobless but are available to work and have actively looked for work in the past 4 weeks)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_unemployed_high": {
        "description": "pct_unemployed_high is the upper bound of the 90% confidence interval for percentage of the population >= 16 years in the civilian labor force who are unemployed (jobless but are available to work and have actively looked for work in the past 4 weeks)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_single_parent": {
        "description": "pct_single_parent is the percentage of single parent households (households with a male or female householder with no spouse or partner present with children of the householder)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_single_parent_low": {
        "description": "pct_single_parent_low is the lower bound of the 90% confidence interval for percentage of single parent households (households with a male or female householder with no spouse or partner present with children of the householder)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
    "pct_single_parent_high": {
        "description": "pct_single_parent_high is the upper bound of the 90% confidence interval for percentage of single parent households (households with a male or female householder with no spouse or partner present with children of the householder)",
        "source": "CDC PLACES Social Determinants of Health dataset for 2024 (originally derived from ACS estimates 2017-2021)",
    },
}


# order of fields for demographics CSV (should match presentation of fields in NCR)
demographics_order = [
    # etc
    "comment",
    # population
    "total_population",
    # age by category
    "pct_under_5",
    "pct_under_18",
    "pct_65_plus",
    # race/ethnicity
    "pct_hispanic_latino",
    "pct_white",
    "pct_african_american",
    "pct_amer_indian_ak_native",
    "pct_asian",
    "pct_hawaiian_pacislander",
    "pct_other",
    "pct_multi",
    # health conditions
    "pct_asthma",
    "pct_asthma_low",
    "pct_asthma_high",
    "pct_copd",
    "pct_copd_low",
    "pct_copd_high",
    "pct_hd",
    "pct_hd_low",
    "pct_hd_high",
    "pct_diabetes",
    "pct_diabetes_low",
    "pct_diabetes_high",
    "pct_stroke",
    "pct_stroke_low",
    "pct_stroke_high",
    "pct_mh",
    "pct_mh_low",
    "pct_mh_high",
    # social determinants of health
    "pct_minority",
    "pct_minority_low",
    "pct_minority_high",
    "pct_no_hsdiploma",
    "pct_no_hsdiploma_low",
    "pct_no_hsdiploma_high",
    "pct_below_150pov",
    "pct_below_150pov_low",
    "pct_below_150pov_high",
    "pct_unemployed",
    "pct_unemployed_low",
    "pct_unemployed_high",
    "pct_foodstamps",
    "pct_foodstamps_low",
    "pct_foodstamps_high",
    "pct_single_parent",
    "pct_single_parent_low",
    "pct_single_parent_high",
    "pct_no_bband",
    "pct_no_bband_low",
    "pct_no_bband_high",
    "pct_crowding",
    "pct_crowding_low",
    "pct_crowding_high",
    "pct_hcost",
    "pct_hcost_low",
    "pct_hcost_high",
    "pct_emospt",
    "pct_emospt_low",
    "pct_emospt_high",
    "pct_w_disability",
    "pct_w_disability_low",
    "pct_w_disability_high",
    "pct_uninsured",
    "pct_uninsured_low",
    "pct_uninsured_high",
]
