"""Module for look-up-table like objects"""

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
