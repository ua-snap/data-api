from flask import request, Response, render_template
import copy
import csv
import io
from urllib.parse import quote
from postprocessing import nullify_and_prune
from fetch_data import extract_nested_dict_keys, get_from_dict
from luts import place_type_labels, demographics_order
from validate_data import place_name_and_type
from datetime import datetime


def create_csv(
    data,
    endpoint,
    place_id=None,
    lat=None,
    lon=None,
    source_metadata=None,
    filename_prefix=None,
    vars=None,
    start_year=None,
    end_year=None,
):
    """Create a CSV for any supported data set
    Args:
        data (dict): dict with same structure as corresponding JSON endpoint
        endpoint (str): string used to determine CSV processing approach
        place_id (str): place identifier (e.g., AK124)
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons
        source_metadata: optional metadata to credit data sources
        filename_prefix: optional filename prefix (a month, for example)
        vars: optional list of variables to include in CSV
        start_year: optional start year for CSV
        end_year: optional end year for CSV
    Returns:
        CSV Response
    """
    if not place_id:
        place_id = request.args.get("community")
    if endpoint in ["conus_hydrology", "arctic_hydrology"]:
        place_name = "Stream ID " + place_id
        place_id = None
        place_type = "stream"
    else:
        place_name, place_type = place_name_and_type(place_id)

    if not endpoint.startswith("places_"):
        metadata = csv_metadata(
            place_name, place_id, place_type, lat, lon, start_year, end_year
        )
    else:
        metadata = ""

    properties = {}

    data = nullify_and_prune(data, endpoint)
    if data in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    if endpoint == "beetles":
        properties = beetles_csv(data)
    elif endpoint == "cmip6_downscaled":
        properties = cmip6_downscaled_csv(data, vars)
    elif endpoint == "cmip6_indicators":
        properties = cmip6_indicators_csv(data)
    elif endpoint == "cmip6_monthly":
        properties = cmip6_monthly_csv(data, vars)
    elif endpoint in [
        "heating_degree_days_Fdays",
        "degree_days_below_zero_Fdays",
        "air_thawing_index_Fdays",
        "air_freezing_index_Fdays",
        "heating_degree_days_Fdays_all",
        "degree_days_below_zero_Fdays_all",
        "air_thawing_index_Fdays_all",
        "air_freezing_index_Fdays_all",
    ]:
        properties = degree_days_csv(data, endpoint)
    elif endpoint == "flammability":
        properties = flammability_csv(data)
    elif endpoint in ["gipl", "gipl_summary"]:
        properties = gipl_csv(data, endpoint)
    elif endpoint in ["ncar12km_indicators"]:
        properties = ncar12km_indicators_csv(data)
    elif endpoint == "landfast_sea_ice":
        properties = landfast_sea_ice_csv(data)
    elif endpoint == "permafrost":
        properties = permafrost_csv(data, source_metadata)
    elif endpoint.startswith("places_"):
        properties = places_csv(data, endpoint)
    elif endpoint == "seaice":
        properties = seaice_csv(data)
    elif endpoint == "snow":
        properties = snow_csv(data)
    elif endpoint in [
        "temperature",
        "precipitation",
        "taspr",
        "temperature_mmm",
        "temperature_all",
        "precipitation_all",
        "proj_precip",
        "tas2km",
    ]:
        properties = taspr_csv(data, endpoint)
    elif endpoint == "temperature_anomalies":
        properties = temperature_anomalies_csv(data)
    elif endpoint == "veg_type":
        properties = veg_type_csv(data)
    elif endpoint in ["wet_days_per_year", "wet_days_per_year_all"]:
        properties = wet_days_per_year_csv(data, endpoint)
    elif endpoint in ["hydrology", "hydrology_mmm"]:
        properties = hydrology_csv(data, endpoint)
    elif endpoint == "demographics":
        properties = demographics_csv(data)
    elif endpoint == "era5wrf_4km":
        properties = era5wrf_csv(data)
    elif endpoint == "fire_weather":
        properties = fire_weather_csv(data, filename_prefix, vars)
    elif endpoint == "conus_hydrology":
        properties = conus_hydrology_csv(data, filename_prefix, source_metadata)
    elif endpoint == "arctic_hydrology":
        properties = arctic_hydrology_csv(data, filename_prefix)

    else:
        return render_template("500/server_error.html"), 500

    # Append data-specific metadata to location metadata.
    properties["metadata"] = metadata + properties["metadata"]

    filename = ""
    if filename_prefix is not None:
        filename += filename_prefix + " "
    filename += properties["filename_data_name"]
    if start_year is not None and end_year is not None:
        filename += f" ({start_year} - {end_year})"
    if not endpoint.startswith("places_"):
        filename += " for "
        if place_name is not None:
            filename += place_name
        elif endpoint == "demographics":
            filename += "All communities in Alaska"
        else:
            filename += lat + " " + lon
    filename += ".csv"
    properties["filename"] = quote(filename)

    return write_csv(properties)


def csv_metadata(
    place_name=None,
    place_id=None,
    place_type=None,
    lat=None,
    lon=None,
    start_year=None,
    end_year=None,
):
    """
    Creates metadata string to add to beginning of CSV file.

    Args:
        place_name (str): Name of the place, None if just lat/lon
        place_id (str): place identifier (e.g., AK124)
        place_type (str): point or area
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons
        start_year: optional start year for CSV
        end_year: optional end year for CSV

    Returns:
        Multiline metadata string
    """
    metadata = "# Location: "
    if place_name is None and lat is not None and lon is not None:
        metadata += lat + " " + lon + "\n"
        # if lat and lon and type huc12, then it's a local / point-to-huc query
        if place_type == "huc12":
            metadata += "# Corresponding HUC12 code: " + place_id + "\n"
    elif place_name is None and lat is None and lon is None:
        metadata += (
            "All communities Alaska\n"  # this covers the demographic request for "all"
        )
    elif place_type == "community":
        metadata += place_name + "\n"
    elif place_type == "stream":
        metadata += place_name + ": " + lat + " " + lon + "\n"
    else:
        metadata += place_name + " (" + place_type_labels[place_type] + ")\n"

    if start_year is not None and end_year is not None:
        metadata += f"# Time range: ({start_year} - {end_year})\n"

    metadata += (
        "# View a report for this location at https://earthmaps.io"
        + request.path
        + "\n"
    )

    return metadata


def build_csv_dicts(packaged_data, package_coords, fill_di=None, values=None):
    """
    Returns a list of dicts to be written out later as a CSV.
    Args:
        packaged_data (json): JSONlike data package output
            from the run_fetch_* and run_aggregate_* functions
        package_coords (list): list of string values corresponding to
            levels of the packaged_data dict. Should be a subset of fieldnames arg.
        fill_di (dict): dict to fill in columns with fixed values.
            Keys should specify the field name and value should be the
            value to fill

    Returns:
        list of dicts with keys/values corresponding to fieldnames
    """
    # extract the coordinate values stored in keys. assumes uniform structure
    # across entire data package (i.e. n levels deep where n == len(fieldnames))
    data_package_coord_combos = extract_nested_dict_keys(packaged_data)
    rows = []
    previous_coord_breadcrumb = None
    for coords in data_package_coord_combos:
        # If there is no data, don't add to CSV line
        if len(coords) <= 1:
            continue
        row_di = {}
        # need more general way of handling fields to be inserted before or after
        # what are actually available in packaged dicts
        for field, coord in zip(package_coords, coords):
            row_di[field] = coord
        # fill in columns with fixed values if specified
        if fill_di:
            for fieldname, value in fill_di.items():
                row_di[fieldname] = value
        # write the actual value
        coords.pop()
        coord_breadcrumb = coords
        if coord_breadcrumb == previous_coord_breadcrumb:
            continue
        else:
            previous_coord_breadcrumb = coord_breadcrumb
        for value in values:
            coords.append(value)
            try:
                row_di[value] = get_from_dict(packaged_data, coords)
            except KeyError:
                row_di[value] = None
            coords.pop()
        rows.append(row_di)

    return rows


def write_csv(properties):
    """
    Creates and returns a downloadable CSV file from list of CSV dicts.

    Args:
        properties (dict): metadata, fieldnames, CSV dicts, and filename

    Returns:
        CSV Response
    """
    output = io.StringIO()
    output.write(properties["metadata"])
    writer = csv.DictWriter(output, fieldnames=properties["fieldnames"])
    writer.writeheader()
    writer.writerows(properties["csv_dicts"])

    response = Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": "attachment; filename="
            + properties["filename"]
            + "; filename*=utf-8''"
            + properties["filename"],
        },
    )
    return response


def era5wrf_csv(data):

    csv_dicts = []
    for date in sorted(data.keys()):
        row = {"date": date}
        row.update(data[date])
        csv_dicts.append(row)

    if not csv_dicts:
        fieldnames = ["date"]
    else:
        fieldnames = ["date"] + [key for key in csv_dicts[0].keys() if key != "date"]

    filename = "Dynamically Downscaled ERA5 4km Data"
    metadata_variables = {
        "t2_mean": "t2_mean is the mean daily temperature in degrees Celsius",
        "t2_max": "t2_max is the maximum daily temperature in degrees Celsius",
        "t2_min": "t2_min is the minimum daily temperature in degrees Celsius",
        "rh2_mean": "rh2_mean is the mean daily relative humidity in percent",
        "rh2_max": "rh2_max is the maximum daily relative humidity in percent",
        "rh2_min": "rh2_min is the minimum daily relative humidity in percent",
        "wspd10_max": "wspd10_max is the maximum daily wind speed in meters per second",
        "wspd10_mean": "wspd10_mean is the mean daily wind speed in meters per second",
        "wdir10_mean": "wdir10_mean is the mean daily wind direction in degrees",
        "rainnc_sum": "rainnc_sum is the total daily accumulated precipitation in millimeters",
        "seaice_max": "seaice_max is the maximum daily sea ice concentration, range 0-1",
    }

    metadata = "# Dynamically Downscaled ERA5 4km Data\n"
    metadata += "# Variables and Units:\n"
    for var, description in metadata_variables.items():
        if var in fieldnames:
            metadata += f"# {var}: {description}\n"
    metadata += "#\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename,
    }


def beetles_csv(data):
    # If this is an area, we include percentages in the CSV fields.
    if (
        "percent-high-protection"
        in data["1988-2017"]["Daymet"]["Historical"]["low"].keys()
    ):
        coords = ["era", "model", "scenario", "snowpack"]
        values = [
            "climate-protection",
            "percent-high-protection",
            "percent-minimal-protection",
            "percent-no-protection",
        ]
    else:
        coords = ["era", "model", "scenario", "snowpack"]
        values = ["climate-protection"]

    fieldnames = coords + values
    csv_dicts = build_csv_dicts(
        data,
        fieldnames,
        values=values,
    )

    fieldnames_to_unhyphenate = [
        "climate-protection",
        "percent-high-protection",
        "percent-minimal-protection",
        "percent-no-protection",
    ]

    # Unhyphenate column headers.
    for index in range(len(fieldnames)):
        if fieldnames[index] in fieldnames_to_unhyphenate:
            fieldnames[index] = fieldnames[index].replace("-", " ")

    # Unhyphenate column values.
    renamed_csv_dicts = []
    for csv_dict in csv_dicts:
        renamed_dict = {}
        for key, value in csv_dict.items():
            if key in fieldnames_to_unhyphenate:
                renamed_dict[key.replace("-", " ")] = value
            else:
                renamed_dict[key] = value
        renamed_csv_dicts.append(renamed_dict)
    csv_dicts = renamed_csv_dicts

    filename_data_name = "Climate Protection from Spruce Beetles"
    metadata = "# Values shown are for climate-related protection level from spruce beetle spread in the area.\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def cmip6_downscaled_csv(data, vars=None):
    metadata_variables = {
        "pr": "# pr is the daily precipitation in mm.\n",
        "tasmax": "# tasmax is the maximum daily temperature in deg C.\n",
        "tasmin": "# tasmin is the minimum daily temperature in deg C.\n",
    }

    coords = ["model", "scenario", "date"]

    if vars is not None:
        values = vars
    else:
        values = list(metadata_variables.keys())

    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)

    metadata = ""
    for variable in values:
        metadata += metadata_variables[variable]

    # This dictionary contains the variable pairs that would append to the file name if selected.
    # This is most likely to happen when the user is downloading the CSV from ARDAC.
    cmip6_variable_groups = {
        "Temperature": {"tasmin", "tasmax"},
        "Precipitation": {"pr"},
    }

    cmip6_variable_name = None

    # This checks if the variables going into the CSV are a part of the CMIP6 variable groups.
    # The set of variables must match the required variables exactly or else the default name is used.
    for name, required_vars in cmip6_variable_groups.items():
        if required_vars == set(vars):
            cmip6_variable_name = name
            break

    # File name is "Downscaled CMIP6 Daily" by default.
    filename_data_name = (
        f"Downscaled CMIP6 Daily {cmip6_variable_name}"
        if cmip6_variable_name
        else "Downscaled CMIP6 Daily"
    )

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def cmip6_indicators_csv(data):

    if "summarize" in request.args and request.args.get("summarize") == "mmm":
        coords = ["model", "scenario", "year", "variable"]
        values = ["max", "mean", "min"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)
        metadata = "# dw are Deep Winter Days. This is the number of days with minimum temperature below -30 (deg C).\n"
        metadata += "# ftc are Freeze-Thaw Days. This is defined as a day where maximum daily temperature is above 0°C and minimum daily temperature is at or below 0°C.\n"
        metadata += "# rx1day is the Maximum 1-day Precipitation. This is the maximum precipitation total for a single calendar day in mm.\n"
        metadata += "# su are Summer Days. This is the number of days with maximum temperature above 25 (deg C).\n"
        filename_data_name = "CMIP6 Indicators Era Summaries"
    else:
        coords = ["model", "scenario", "year"]
        values = ["dw", "ftc", "rx1day", "su"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)
        metadata = "# dw are Deep Winter Days. This is the number of days with minimum temperature below -30 (deg C).\n"
        metadata += "# ftc are Freeze-Thaw Days. This is defined as a day where maximum daily temperature is above 0°C and minimum daily temperature is at or below 0°C.\n"
        metadata += "# rx1day is the Maximum 1-day Precipitation. This is the maximum precipitation total for a single calendar day in mm.\n"
        metadata += "# su are Summer Days. This is the number of days with maximum temperature above 25 (deg C).\n"
        filename_data_name = "CMIP6 Indicators"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def cmip6_monthly_csv(data, vars=None):
    metadata_variables = {
        "clt": "# clt is the mean monthly cloud area fraction as a percentage.\n",
        "evspsbl": "# evspsbl is the total monthly evaporation (including sublimation and transpiration) in kg/m²/s.\n",
        "hfls": "# hfls is the mean monthly surface upward latent heat flux in W/m².\n",
        "hfss": "# hfss is the mean monthly surface upward sensible heat flux in W/m².\n",
        "pr": "# pr is the total monthly precipitation in mm.\n",
        "prsn": "# prsn is the precipitation as snow in kg m-2 s-1; includes precipitation of all forms of water in the solid phase.\n",
        "psl": "# psl is the mean monthly sea level pressure in Pa.\n",
        "rlds": "# rlds is the mean monthly surface downwelling longwave flux in the air in W/m².\n",
        "rsds": "# rsds is the mean monthly surface downwelling shortwave flux in the air in W/m².\n",
        "sfcWind": "# sfcWind is the mean near surface wind speed in m/s.\n",
        "siconc": "# siconc is the sea ice concentration or the percentage of grid cell covered by sea ice.\n",
        "snw": "# snw is the snow water equivalent in mm.\n",
        "tas": "# tas is the mean monthly temperature in deg C.\n",
        "tasmax": "# tasmax is the maximum monthly temperature in deg C.\n",
        "tasmin": "# tasmin is the minimum monthly temperature in deg C.\n",
        "ts": "# ts is the mean monthly surface temperature in deg C.\n",
        "uas": "# uas is the mean monthly near surface eastward wind in m/s.\n",
        "vas": "# vas is the mean monthly near surface northward wind in m/s.\n",
    }

    coords = ["model", "scenario", "month"]

    if vars is not None:
        values = vars
    else:
        values = list(metadata_variables.keys())

    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)

    metadata = ""
    for variable in values:
        metadata += metadata_variables[variable]

    # This dictionary contains the variable pairs that would append to the file name if selected.
    # This is most likely to happen when the user is downloading the CSV from ARDAC.
    cmip6_variable_groups = {
        "Temperature": {"tas", "tasmin", "tasmax"},
        "Precipitation": {"pr"},
        "Wind": {"sfcWind", "uas", "vas"},
        "Oceanography": {"psl", "ts"},
        "Evaporation": {"evspsbl"},
        "Solar Radiation & Cloud Cover": {"rsds", "rlds", "hfss", "hfls", "clt"},
        "Snow": {"snw", "prsn"},
        "Sea Ice": {"siconc"},
    }

    cmip6_variable_name = None

    # This checks if the variables going into the CSV are a part of the CMIP6 variable groups.
    # The set of variables must match the required variables exactly or else the default name is used.
    for name, required_vars in cmip6_variable_groups.items():
        if required_vars == set(vars):
            cmip6_variable_name = name
            break

    # File name is "CMIP6 Monthly" by default.
    filename_data_name = (
        f"CMIP6 Monthly {cmip6_variable_name}"
        if cmip6_variable_name
        else "CMIP6 Monthly"
    )

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def degree_days_csv(data, endpoint):
    if endpoint in [
        "heating_degree_days_Fdays",
        "degree_days_below_zero_Fdays",
        "air_thawing_index_Fdays",
        "air_freezing_index_Fdays",
    ]:
        coords = ["model"]
        values = ["ddmin", "ddmean", "ddmax"]
    elif endpoint in [
        "heating_degree_days_Fdays_all",
        "degree_days_below_zero_Fdays_all",
        "air_thawing_index_Fdays_all",
        "air_freezing_index_Fdays_all",
        "dd_preview",
    ]:
        coords = ["model", "scenario", "year"]
        values = ["dd"]

    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)

    if endpoint in ["heating_degree_days_Fdays", "heating_degree_days_Fdays_all"]:
        filename_data_name = "Heating Degree Days"
        metadata = "# dd is the cumulative annual degree days below 65 degrees F for the specified model and scenario\n"
    elif endpoint in [
        "degree_days_below_zero_Fdays",
        "degree_days_below_zero_Fdays_all",
    ]:
        filename_data_name = "Degree Days Below Zero"
        metadata = "# dd is the cumulative annual degree days below 0 degrees F for the specified model and scenario\n"
    elif endpoint in ["air_thawing_index_Fdays", "air_thawing_index_Fdays_all"]:
        filename_data_name = "Air Thawing Index"
        metadata = "# dd is the cumulative annual degree days above freezing for the specified model and scenario\n"
    elif endpoint in ["air_freezing_index_Fdays", "air_freezing_index_Fdays_all"]:
        filename_data_name = "Air Freezing Index"
        metadata = "# dd is the cumulative annual degree days below freezing for the specified model and scenario\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def flammability_csv(data):
    # Reformat data to nesting structure expected by other CSV functions.
    for era in data.keys():
        for model in data[era].keys():
            for scenario, value in data[era][model].items():
                data[era][model][scenario] = {"mean": value}
    coords = ["date_range", "model", "scenario"]
    values = ["mean"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    filename_data_name = "Flammability"
    metadata = "# mean is the mean of of annual means\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def gipl_csv(data, endpoint):
    if endpoint == "gipl_summary":
        coords = ["summary"]
    elif endpoint == "gipl" or endpoint == "gipl_preview":
        coords = ["model", "year", "scenario"]
    values = [
        "magt0.5m",
        "magt1m",
        "magt2m",
        "magt3m",
        "magt4m",
        "magt5m",
        "magtsurface",
        "permafrostbase",
        "permafrosttop",
        "talikthickness",
    ]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    if endpoint == "gipl_preview":
        metadata = ""
    else:
        metadata = "# GIPL model outputs for ten variables including mean annual ground temperature (deg C) at various depths below the surface as well as talik thickness (m) and depths of permafrost base and top (m)\n"
    filename_data_name = "GIPL 1 km Model Outputs"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def ncar12km_indicators_csv(data):
    # Reorder eras so that midcentury rows appear before longterm rows in CSV.
    reordered = {}
    for indicator in data.keys():
        reordered[indicator] = {}
        for era in ["historical", "midcentury", "longterm"]:
            if era in data[indicator].keys():
                reordered[indicator][era] = data[indicator][era]

    coords = ["indicator", "era", "model", "scenario"]
    values = ["min", "mean", "max"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(reordered, fieldnames, values=values)
    metadata = "# cd is the Very Cold Day Threshold (deg C). Only 5 days in a year are colder than this.\n"
    metadata += "# cdd are Consecutive Dry Days. This is the number of consecutive days with less than 1mm precipitation.\n"
    metadata += "# csdi is the Cold Spell Duration Index. This is a cold spell metric: the number of cold days (<10th percentile) occurring in a row following an initial cold spell period of six days.\n"
    metadata += "# cwd are Consecutive Wet Days. This is the number of consecutive days with more than 1mm precipitation.\n"
    metadata += "# dw are Deep Winter Days. This is the number of days with mean temperature below -30 (deg C).\n"
    metadata += "# hd is the Very Hot Day Threshold (deg C). Only 5 days in a year are warmer than this.\n"
    metadata += "# r10mm are Heavy Precipitation Days. This is the number of individual days with 10mm or more precipitation.\n"
    metadata += "# rx1day is the Maximum 1-day Precipitation. This is the maximum precipitation total for a single day in mm.\n"
    metadata += "# rx5day is the Maximum 5-day Precipitation. This is the maximum precipitation total for a 5-day period in mm.\n"
    metadata += "# su are Summer Days. This is the number of days with mean temperature above 25 (deg C).\n"
    metadata += "# wsdi is the Warm Spell Duration Index. This is a heat wave metric: the number of hot days (>90th percentile) occurring in a row following an initial warm spell period of six days.\n"
    filename_data_name = "Temperature & Precipitation Indicators"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def landfast_sea_ice_csv(data):
    # Reformat data to nesting structure expected by other CSV functions.
    for key, value in data.items():
        data[key] = {"value": value}
    coords = ["date"]
    values = ["value"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    metadata = "# Landfast Sea Ice Value Key: 0: Open ocean or non-landfast sea ice; 128: Land; 255: Landfast Sea Ice\n"
    filename_data_name = "Landfast Sea Ice"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def permafrost_csv(data, source_metadata):
    filename_data_name = "Permafrost"
    sources = {
        "gipl_1km": {
            "coords": [
                "source",
                "model",
                "year",
                "scenario",
            ],
            "values": [
                "magt0.5m",
                "magt1m",
                "magt2m",
                "magt3m",
                "magt4m",
                "magt5m",
                "magtsurface",
                "permafrostbase",
                "permafrosttop",
                "talikthickness",
            ],
        },
    }

    metadata = "# magt*m is the mean annual ground temperature at a given depth (* meters) in degrees Celsius\n"
    metadata += "# magtsurface is the mean annual ground temperature at the ground surface in degrees Celsius\n"
    metadata += "# permafrost base is the lower boundary of the permafrost below the surface in meters\n"
    metadata += "# permafrost top is the upper boundary of the permafrost below the surface in meters\n"
    metadata += "# talikthickness is the thickness of the perennially unfrozen ground occurring in permafrost terrain in meters\n"
    metadata += "# gipl is the Geophysical Institute's Permafrost Laboratory\n"

    all_fields = []
    csv_dicts = []
    for source in sources.keys():
        fieldnames = sources[source]["coords"] + sources[source]["values"]
        all_fields += fieldnames
        source_data = {source: data[source]}
        metadata += "# " + source_metadata[source] + "\n"
        csv_dicts += build_csv_dicts(
            source_data, fieldnames, values=sources[source]["values"]
        )
    fieldnames = list(dict.fromkeys(all_fields))

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def places_csv(data, endpoint):
    if endpoint in ["places_all", "places_communities"]:
        values = [
            "name",
            "alt_name",
            "region",
            "country",
            "latitude",
            "longitude",
            "type",
        ]
    else:
        values = [
            "name",
            "type",
        ]

    reformatted_data = {}
    for item in data:
        reformatted_data[item["id"]] = {}
        for key in values:
            if key in item.keys():
                reformatted_data[item["id"]].update({key: item[key]})
            else:
                reformatted_data[item["id"]].update({key: None})

    coords = ["id"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(reformatted_data, fieldnames, values=values)
    metadata = "# Places listed here can be used in queries to the Alaska + Arctic Geospatial Data API\n"

    if endpoint == "places_all":
        filename_data_name = "Places (All)"
    elif endpoint == "places_communities":
        filename_data_name = "Places (Communities)"
    elif endpoint == "places_huc":
        filename_data_name = "Places (HUCs)"
    elif endpoint == "places_corporation":
        filename_data_name = "Places (Corporations)"
    elif endpoint == "places_climate_division":
        filename_data_name = "Places (Climate Divisions)"
    elif endpoint == "places_ethnolinguistic_region":
        filename_data_name = "Places (Ethnolinguistic Regions)"
    elif endpoint == "places_game_management_unit":
        filename_data_name = "Places (Game Management Units)"
    elif endpoint == "places_fire_zone":
        filename_data_name = "Places (Fire Zones)"
    elif endpoint == "places_first_nation":
        filename_data_name = "Places (First Nations)"
    elif endpoint == "places_borough":
        filename_data_name = "Places (Boroughs)"
    elif endpoint == "places_census_area":
        filename_data_name = "Places (Census Areas)"
    elif endpoint == "places_protected_area":
        filename_data_name = "Places (Protected Areas)"
    elif endpoint == "places_yt_fire_district":
        filename_data_name = "Places (Yukon Fire Districts)"
    elif endpoint == "places_yt_game_management_subzone":
        filename_data_name = "Places (Yukon Game Management Subzones)"
    elif endpoint == "places_yt_watershed":
        filename_data_name = "Places (Yukon Watersheds)"
    elif endpoint == "places_ecoregion":
        filename_data_name = "Places (Ecoregions)"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def seaice_csv(data):
    reformatted_data = {}
    for key, value in data.items():
        [year, month] = key.split("-")
        month_name = datetime.strptime(month, "%m").strftime("%B")
        if year not in reformatted_data:
            reformatted_data[year] = {}
        reformatted_data[year][month_name] = {"concentration": value}
    coords = ["year", "month"]
    values = ["concentration"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(reformatted_data, fieldnames, values=values)
    metadata = "# Sea Ice Concentration is the percentage of sea ice coverage at the given latitude and longitude for each year and month.\n"
    filename_data_name = "Sea Ice Concentration"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def snow_csv(data):
    coords = ["model", "scenario", "decade"]
    values = ["SFE"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    metadata = "# SFE is the total annual snowfall equivalent in millimeters for the specified model-scenario-decade\n"
    filename_data_name = "SFE"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def taspr_csv(data, endpoint):
    tas_metadata = "# tas is the mean annual near-surface air temperature in degrees Celsius for the specified model and scenario\n"
    pr_metadata = "# pr is the total annual precipitation in millimeters for the specified model and scenario\n"

    csv_dicts = []
    if endpoint in ["temperature", "precipitation", "taspr"]:
        all_fields = []

        # Any key starting with year less than 2010 is considered historical.
        historical_data = {k: v for (k, v) in data.items() if int(k[0:4]) < 2010}
        coords = ["date_range", "season", "model", "scenario", "variable"]
        values = ["mean", "min", "max", "median", "hi_std", "lo_std", "q1", "q3"]
        fieldnames = coords + values
        all_fields += fieldnames
        csv_dicts += build_csv_dicts(historical_data, fieldnames, values=values)

        # Any key starting with year 2010 or higher is considered projected.
        projected_data = {k: v for (k, v) in data.items() if int(k[0:4]) >= 2010}
        coords = ["date_range", "season", "model", "scenario"]

        metadata = "# mean is the mean of annual means\n"
        metadata += "# median is the median of annual means\n"
        metadata += "# max is the maximum annual mean\n"
        metadata += "# min is the minimum annual mean\n"
        metadata += "# q1 is the first quartile of the annual means\n"
        metadata += "# q3 is the third quartile of the annual means\n"
        metadata += "# hi_std is the mean + standard deviation of annual means\n"
        metadata += "# lo_std is the mean - standard deviation of annual means\n"
        metadata += "# DJF is December - February\n"
        metadata += "# MAM is March - May\n"
        metadata += "# JJA is June - August\n"
        metadata += "# SON is September - November\n"

        if endpoint == "temperature":
            values = ["tas"]
            metadata = tas_metadata + metadata
            filename_data_name = "Temperature"
        elif endpoint == "precipitation":
            values = ["pr"]
            metadata = pr_metadata + metadata
            filename_data_name = "Precipitation"
        elif endpoint == "taspr":
            values = ["tas", "pr"]
            metadata = tas_metadata + pr_metadata + metadata
            filename_data_name = "Temperature & Precipitation"

        fieldnames = coords + values
        all_fields += fieldnames
        csv_dicts += build_csv_dicts(projected_data, fieldnames, values=values)

        # Reformat CSV dicts to have more consistent column structure between
        # historical and projected stats.
        reformatted_csv_dicts = []
        for csv_dict in csv_dicts:
            # Add "tas" to variable column and rename value column to "mean".
            if "tas" in csv_dict:
                tas_dict = copy.deepcopy(csv_dict)
                if "pr" in tas_dict:
                    del tas_dict["pr"]
                tas_dict["variable"] = "tas"
                tas_dict["mean"] = tas_dict.pop("tas")
                reformatted_csv_dicts.append(tas_dict)
            # Add "pr" to variable column and rename value column to "mean".
            if "pr" in csv_dict:
                pr_dict = copy.deepcopy(csv_dict)
                if "tas" in pr_dict:
                    del pr_dict["tas"]
                pr_dict["variable"] = "pr"
                pr_dict["mean"] = pr_dict.pop("pr")
                reformatted_csv_dicts.append(pr_dict)
            # For historical CSV dicts, copy as-is.
            if "tas" not in csv_dict and "pr" not in csv_dict:
                reformatted_csv_dicts.append(csv_dict)

            if "tas" in all_fields:
                all_fields.remove("tas")
            if "pr" in all_fields:
                all_fields.remove("pr")
            all_fields.append("mean")

            csv_dicts = reformatted_csv_dicts
            fieldnames = list(dict.fromkeys(all_fields))

    elif endpoint in ["temperature_mmm", "precipitation_mmm"]:
        tas_metadata = "# tas is the temperature at surface in degrees Celsius\n"
        pr_metadata = "# pr is precipitation in millimeters\n"

        coords = ["model", "scenario", "year"]
        if endpoint == "temperature_mmm":
            values = ["tasmin", "tasmean", "tasmax"]
        elif endpoint == "precipitation_mmm":
            values = ["prmin", "prmean", "prmax"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)

        metadata = (
            "# tasmin is the minimum temperature for the specified model and scenario\n"
        )
        metadata += (
            "# tasmean is the mean temperature for the specified model and scenario\n"
        )
        metadata += (
            "# tasmax is the maximum temperature for the specified model and scenario\n"
        )

        if endpoint == "temperature_mmm":
            metadata = tas_metadata + metadata
            filename_data_name = "Temperature"
        elif endpoint == "precipitation_mmm":
            metadata = pr_metadata + metadata
            filename_data_name = "Precipitation"

    elif endpoint in ["temperature_all", "precipitation_all"]:
        coords = ["model", "scenario", "year"]
        if endpoint == "temperature_all":
            values = ["tas"]
        elif endpoint == "precipitation_all":
            values = ["pr"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)

        if endpoint == "temperature_all":
            metadata = tas_metadata
            filename_data_name = "Temperature"
        elif endpoint == "precipitation_all":
            metadata = pr_metadata
            filename_data_name = "Precipitation"

    elif endpoint == "proj_precip":
        coords = ["exceedance_probability", "duration", "model", "era"]
        values = ["pf", "pf_lower", "pf_upper"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)
        metadata = (
            "# exceedance_probability is the annual exceedance probability in percent\n"
        )
        metadata += "# duration is the amount of time for the predicted amount of precipitation\n"
        metadata += "# model is the model the data is derived from\n"
        metadata += (
            "# era is the time range for this predicted amount of precipitation \n"
        )
        metadata += "# pf is amount of precipitation in mm\n"
        metadata += "# pf_lower is the lower bound of the 95% confidence interval of the variable pf\n"
        metadata += "# pf_upper is the upper bound of the 95% confidence interval of the variable pf\n"
        filename_data_name = "Future Projections of Precipitation"

    elif endpoint == "tas2km":
        all_fields = []

        coords = ["model", "scenario", "month", "year"]
        values = ["tasmin", "tasmean", "tasmax"]
        fieldnames = coords + values
        all_fields += fieldnames
        csv_dicts += build_csv_dicts(data["historical"], fieldnames, values=values)
        csv_dicts += build_csv_dicts(data["projected"], fieldnames, values=values)

        metadata = "# tasmin is the minimum temperature in degrees C\n"
        metadata += "# tasmean is the mean temperature in degrees C\n"
        metadata += "# tasmax is the maximum temperature in degrees C\n"

        metadata = tas_metadata + metadata
        filename_data_name = "Monthly Temperature"

    # Change "CRU_historical" scenario to just "Historical".
    for csv_dict in csv_dicts:
        if (
            endpoint not in ["proj_precip", "tas2km"]
            and csv_dict["scenario"] == "CRU_historical"
        ):
            csv_dict["scenario"] = "Historical"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def temperature_anomalies_csv(data):
    # Separate anomaly and baseline data due to heterogenous data structures.
    filtered_baseline_data = {}
    filtered_anomaly_data = {}
    for model in data.keys():
        filtered_baseline_data[model] = data[model]["temperature_baseline"]
        filtered_anomaly_data[model] = data[model]["temperature_anomalies"]

    baseline_data = {}
    anomaly_data = {}

    for model, value in filtered_baseline_data.items():
        baseline_data[model] = {"1951-1980": {"temperature_baseline": {"value": value}}}

    for model in filtered_anomaly_data.keys():
        anomaly_data[model] = dict()
        for scenario in filtered_anomaly_data[model].keys():
            anomaly_data[model][scenario] = dict()
            for year, value in filtered_anomaly_data[model][scenario].items():
                anomaly_data[model][scenario][year] = {
                    "temperature_anomaly": {"value": value}
                }

    baseline_coords = ["model", "year(s)", "variable"]
    baseline_values = ["value"]
    baseline_fieldnames = baseline_coords + baseline_values
    csv_dicts = build_csv_dicts(
        baseline_data, baseline_fieldnames, values=baseline_values
    )

    anomaly_coords = ["model", "scenario", "year(s)", "variable"]
    anomaly_values = ["value"]
    anomaly_fieldnames = anomaly_coords + anomaly_values
    csv_dicts += build_csv_dicts(
        anomaly_data, anomaly_fieldnames, values=anomaly_values
    )

    csv_dicts = sorted(csv_dicts, key=lambda x: x["model"])

    all_fieldnames = anomaly_fieldnames

    metadata = "# temperature_baseline is the mean of annuals means from 1951-1980 for the corresponding model in degrees C\n"
    metadata += "# temperature_anomaly is calculated by subtracting the 1951-1980 baseline from the annual mean in degrees C\n"

    filename_data_name = "Temperature Anomalies"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": all_fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def veg_type_csv(data):
    # Reformat data to nesting structure expected by other CSV functions.
    for era in data.keys():
        for model in data[era].keys():
            for scenario in data[era][model].keys():
                for veg_type, value in data[era][model][scenario].items():
                    data[era][model][scenario][veg_type] = {"percent": value}
    coords = ["date_range", "model", "scenario", "veg_type"]
    values = ["percent"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    filename_data_name = "Vegetation Type"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": "",
        "filename_data_name": filename_data_name,
    }


def wet_days_per_year_csv(data, endpoint):
    if endpoint == "wet_days_per_year":
        coords = ["era"]
        values = ["wdpymin", "wdpymean", "wdpymax"]
    elif endpoint == "wet_days_per_year_all":
        coords = ["model", "year"]
        values = ["wdpy"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    metadata = "# wdpy is the count of wet days (days where the total precipitation amount is greater than or equal to 1.0 mm) per calendar year\n"
    filename_data_name = "Wet Days Per Year"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def hydrology_csv(data, endpoint):
    if endpoint == "hydrology":
        coords = ["model", "scenario", "month", "era"]
        values = [
            "evap",
            "glacier_melt",
            "iwe",
            "pcp",
            "runoff",
            "sm1",
            "sm2",
            "sm3",
            "snow_melt",
            "swe",
            "tmax",
            "tmin",
        ]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)
        metadata = "# Hydrology model outputs for ten variables; decadal means of monthly values.\n"
        metadata += "# model is the model the data is derived from\n"
        metadata += "# scenario is the emissions scenario\n"
        metadata += "# month is the month of year over which data are summarized\n"
        metadata += "# era is the decade over which data are summarized\n"
        metadata += "# variable is the hydrology variable name\n"
        metadata += "# evap is the decadal mean of the monthly sum of daily evapotranspiration in mm\n"
        metadata += "# glacier_melt is the decadal mean of the monthly sum of daily glacier ice melt in mm\n"
        metadata += "# iwe is the decadal mean of the monthly maximum of daily ice water equivalent in mm\n"
        metadata += "# pcp is the decadal mean of the monthly sum of daily precipitation in mm\n"
        metadata += "# runoff is the decadal mean of the monthly sum of daily surface runoff in mm\n"
        metadata += "# sm1 is the decadal mean of the monthly mean of daily soil moisture in layer 1 in mm\n"
        metadata += "# sm2 is the decadal mean of the monthly mean of daily soil moisture in layer 2 in mm\n"
        metadata += "# sm3 is the decadal mean of the monthly mean of daily soil moisture in layer 3 in mm\n"
        metadata += "# snowmelt is the decadal mean of the monthly sum of daily snowmelt in mm\n"
        metadata += "# swe is the decadal mean of the monthly maximum of daily snow water equivalent in mm\n"
        metadata += "# tmax is the decadal mean of the monthly mean of daily maximum air temperature at 2m in degrees C\n"
        metadata += "# tmin is the decadal mean of the monthly mean of daily minimum air temperature at 2m in degrees C\n"
        filename_data_name = "Hydrology Model Outputs - Decadal Mean Values - "

        return {
            "csv_dicts": csv_dicts,
            "fieldnames": fieldnames,
            "metadata": metadata,
            "filename_data_name": filename_data_name,
        }

    if endpoint == "hydrology_mmm":
        coords = ["model", "scenario", "month", "variable"]
        values = ["min", "mean", "max"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)
        metadata = "# Hydrology model outputs for ten variables; minimum mean and maximum across all decades (1950-2099).\n"
        metadata += "# model is the model the data is derived from\n"
        metadata += "# scenario is the emissions scenario\n"
        metadata += "# month is the month of year over which data are summarized\n"
        metadata += "# variable is the hydrology variable name\n"
        metadata += "# mean is the mean of all decadal mean values\n"
        metadata += "# max is the maximum of all decadal mean values\n"
        metadata += "# min is the minimum of all decadal mean values\n"
        metadata += "# evap is the monthly sum of daily evapotranspiration in mm\n"
        metadata += (
            "# glacier_melt is the monthly sum of daily glacier ice melt in mm\n"
        )
        metadata += "# iwe is the monthly maximum of daily ice water equivalent in mm\n"
        metadata += "# pcp is the monthly sum of daily precipitation in mm\n"
        metadata += "# runoff is the monthly sum of daily surface runoff in mm\n"
        metadata += (
            "# sm1 is the monthly mean of daily soil moisture in layer 1 in mm\n"
        )
        metadata += (
            "# sm2 is the monthly mean of daily soil moisture in layer 2 in mm\n"
        )
        metadata += (
            "# sm3 is the monthly mean of daily soil moisture in layer 3 in mm\n"
        )
        metadata += "# snowmelt is the monthly sum of daily snowmelt in mm\n"
        metadata += (
            "# swe is the monthly maximum of daily snow water equivalent in mm\n"
        )
        metadata += "# tmax is the monthly mean of daily maximum air temperature at 2m in degrees C\n"
        metadata += "# tmin is the monthly mean of daily minimum air temperature at 2m in degrees C\n"

        filename_data_name = "Hydrology Model Outputs - Minimum, Mean, and Maximum Across All Decades 1950-2099 - "

        return {
            "csv_dicts": csv_dicts,
            "fieldnames": fieldnames,
            "metadata": metadata,
            "filename_data_name": filename_data_name,
        }


def demographics_csv(data):
    value_cols = []
    for key in data.keys():
        for subkey in data[key].keys():
            if subkey != "description" and subkey != "source":
                value_cols.append(subkey)
    value_cols = list(set(value_cols)) + ["description", "source"]

    # specify the order of the CSV columns
    fieldnames = ["variable", "Alaska", "United States", "description", "source"]
    community_keys = [key for key in value_cols if key not in fieldnames]
    if not community_keys:
        raise ValueError("No valid community fieldname found in value_cols.")
    community_fieldname = community_keys[0]
    fieldnames.insert(1, community_fieldname)
    csv_dicts = build_csv_dicts(data, fieldnames, values=value_cols)

    # order variables in CSV dicts to match NCR data display order in the luts.py demographics_order list
    ordered_csv_dicts = []
    for key in demographics_order:
        for csv_dict in csv_dicts:
            if csv_dict["variable"] == key:
                ordered_csv_dicts.append(csv_dict)

    metadata = "# Demographic and health data for individual communities plus the state of Alaska and United States.\n"

    filename_data_name = "Demographic and Health Data - "

    return {
        "csv_dicts": ordered_csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def fire_weather_csv(data, filename_prefix, vars):

    # filename_prefix denotes operation ("3dayrollingavg", "summer_fire_danger_rating_days", etc.) to aid in packaging CSV

    # data structure:
    # first level of data is string defining the date range ("1990-2000", etc.), next level is variable, then model
    # if n-day avg operations, we then have date (DOY like "04-01"), then dict of min/mean/max stat values for each date
    # if summary of fire danger ratings operations, we then have a dict of rating categories & their values

    csv_dicts = []
    for time_period in sorted(data.keys()):
        row = {"period": time_period}
        for variable in data[time_period].keys():
            row["variable"] = variable
            for model in data[time_period][variable].keys():
                row["model"] = model
                # test if "rolling" is part of the filename_prefix to determine data structure
                if "Rolling" in filename_prefix:
                    for date in data[time_period][variable][model].keys():
                        row["date"] = date
                        row.update(data[time_period][variable][model][date])
                        for stat in ["min", "mean", "max"]:
                            row[stat] = data[time_period][variable][model][date][stat]
                        csv_dicts.append(copy.deepcopy(row))
                else:  # op is "summer_fire_danger_rating_days"
                    row.update(data[time_period][variable][model])
                    csv_dicts.append(copy.deepcopy(row))

    fieldnames = ["period", "variable", "model"]
    if "Rolling" in filename_prefix:
        fieldnames = fieldnames + ["date", "min", "mean", "max"]
    else:
        fieldnames = fieldnames + sorted(data[time_period][variable][model].keys())

    filename = ""
    metadata_variables = {
        "bui": "Build Up Index",
        "dc": "Drought Code",
        "dmc": "Duff Moisture Code",
        "ffmc": "Fine Fuel Moisture Code",
        "fwi": "Fire Weather Index",
        "isi": "Initial Spread Index",
    }

    metadata = "# CMIP6 Fire Weather Indices\n"
    metadata += "# Variables:\n"
    for var, description in metadata_variables.items():
        if var in vars:
            metadata += f"# {var}: {description}\n"
    metadata += "#\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename,
    }


def conus_hydrology_csv(data, filename_prefix, source_metadata):

    # substrings in filename_prefix denotes endpoint ("Statistics", "Modeled") to aid in packaging CSV

    # data structure for all endpoints:
    # first level of data are strings "data", "id", "latitude", "longitude", "metadata", "name"

    # for stats:
    # the "data" key contains the hydrology data, with levels "landcover", "model", "scenario", "era", then a dict of variable ids and their values
    # the "metadata" key contains sub-levels "source"["citation"] and "variables"[<variable_id>]"description" + "units"
    # all other keys contain single values

    # for modeled daily climatology:
    # the "data" key contains the hydrology data, with levels "landcover", "model", "scenario", "era", then a list of dicts - one for each DOY, with a "doy" key/value, a "water_year_index" key/value, and additional keys/values for each variable id
    # the "metadata" key is identical to the stats endpoint

    # for observed daily climatology:
    # same structure as the modeled daily climatology endpoint, but there is only one landcover/model/scenario combination ("actual"/"usgs"/"observed") and the "metadata" key contains an additional "percent_complete" key/value pair

    # use source_metadata parameter to pass the percent complete info from the observed climatology endpoint metadata into the CSV metadata

    csv_dicts = []
    for key in data.keys():
        if key != "data":
            continue
        for landcover in data["data"].keys():
            for model in data["data"][landcover].keys():
                for scenario in data["data"][landcover][model].keys():
                    for era in data["data"][landcover][model][scenario].keys():
                        row = {
                            "landcover": landcover,
                            "model": model,
                            "scenario": scenario,
                            "era": era,
                        }
                        if "Statistics" in filename_prefix:
                            row.update(data["data"][landcover][model][scenario][era])
                            csv_dicts.append(copy.deepcopy(row))
                        else:  # modeled or observed daily climatology
                            for doy_dict in data["data"][landcover][model][scenario][
                                era
                            ]:
                                row["doy"] = doy_dict["doy"]
                                row["water_year_index"] = doy_dict["water_year_index"]
                                for var_key in doy_dict.keys():
                                    if (
                                        var_key != "doy"
                                        and var_key != "water_year_index"
                                    ):
                                        row[var_key] = doy_dict[var_key]
                                csv_dicts.append(copy.deepcopy(row))
    fieldnames = ["landcover", "model", "scenario", "era"]
    if "Statistics" in filename_prefix:
        # get variable keys from metadata to use as fieldnames
        for var_id in data["metadata"]["variables"].keys():
            fieldnames.append(var_id)
    else:
        fieldnames.append("doy")
        # get variable keys from first doy dict to use as fieldnames
        sample_doy_dict = data["data"][landcover][model][scenario][era][0]
        for var_key in sample_doy_dict.keys():
            if var_key != "doy":
                fieldnames.append(var_key)

    metadata = ""
    if "Statistics" in filename_prefix:
        metadata += "# The following hydrologic statistics are calculated from modeled daily streamflow data:\n"
        metadata += "# dh1: Annual maximum daily flow. Compute the maximum of a 1-day moving average flow for each year. DH1 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dh2: Annual maximum of 3-day moving average flows. Compute the maximum of a 3-day moving average flow for each year. DH2 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dh3: Annual maximum of 7-day moving average flows. Compute the maximum of a 7-day moving average flow for each year. DH3 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dh4: Annual maximum of 30-day moving average flows. Compute the maximum of 30-day moving average flows. Compute the maximum of a 30-day moving average flow for each year. DH4 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dh5: Annual maximum of 90-day moving average flows. Compute the maximum of a 90-day moving average flow for each year. DH5 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dh15: High flow pulse duration. Compute the average duration for flow events with flows above a threshold equal to the 75th percentile value for each year in the flow record. DH15 is the median of the yearly average durations (days/year - temporal).\n"
        metadata += "# dl1: Annual minimum daily flow. Compute the minimum 1-day average flow for each year. DL1 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dl2: Annual minimum of 3-day moving average flow. Compute the minimum of a 3-day moving average flow for each year. DL2 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dl3: Annual minimum of 7-day moving average flow. Compute the minimum of a 7-day moving average flow for each year. DL3 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dl4: Annual minimum of 30-day moving average flow. Compute the minimum of a 30-day moving average flow for each year. DL4 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dl5: Annual minimum of 90-day moving average flow. Compute the minimum of a 90-day moving average flow for each year. DL5 is the mean of these values (cubic feet per second - temporal).\n"
        metadata += "# dl16: Low flow pulse duration. Compute the average pulse duration for each year for flow events below a threshold equal to the 25th percentile value for the entire flow record. DL16 is the median of the yearly average durations (days/year - temporal).\n"
        metadata += "# lf1: Number of days per year below a threshold of 0.1 cubic feet per second per square mile. LF1 is the median annual number of days below the threshold (number of days/year - temporal).\n"
        metadata += "# spr_dur3: Spring (April-June) maximum of 3-day moving average flows. Compute the maximum of a 3-day moving average flow for each year. SPR_DUR3 is the median of these values (cubic feet per second - temporal).\n"
        metadata += "# spr_dur7: Spring (April-June) maximum of 7-day moving average flows. Compute the maximum of a 7-day moving average flow for each year. SPR_DUR7 is the median of these values (cubic feet per second - temporal).\n"
        metadata += "# sum_dur3: Summer (July-September) minimum of 3-day moving average flow. Compute the minimum of a 3-day moving average flow for each year. SUM_DUR3 is the median of these values (cubic feet per second - temporal).\n"
        metadata += "# sum_dur7: Summer (July-September) minimum of 7-day moving average flow. Compute the minimum of a 7-day moving average flow for each year. SUM_DUR7 is the median of these values (cubic feet per second - temporal).\n"
        metadata += "# fh1: High flood pulse count. Compute the average number of flow events with flows above a threshold equal to the 75th percentile value for the entire flow record. FH1 is the mean number of events (number of events/year - temporal).\n"
        metadata += "# fh5: Flood frequency. Compute the average number of flow events with flows above a threshold equal to the median flow value for the entire flow record. FH5 is the mean number of events (number of events/year - temporal).\n"
        metadata += "# fh6: Flood frequency. Compute the average number of flow events with flows above a threshold equal to three times the median flow value for the entire flow record. FH6 is the mean number of events (number of events/year - temporal).\n"
        metadata += "# fh7: Flood frequency. Compute the average number of flow events with flows above a threshold equal to seven times the median flow value for the entire flow record. FH7 is the mean number of events (number of events/year - temporal).\n"
        metadata += "# fl1: Low flood pulse count. Compute the average number of flow events with flows below a threshold equal to the 25th percentile value for the entire flow record. FL1 is the mean number of events (number of events/year - temporal).\n"
        metadata += "# fl3: Frequency of low pulse spells. Compute the average number of flow events with flows below a threshold equal to 5 percent of the mean flow value for the entire flow record. FL3 is the mean number of events (number of events/year - temporal).\n"
        metadata += "# spr_freq: Flood frequency for April-June. Compute the average number of flow events with flows above a threshold equal to the 10th percentile for the entire flow record. SPR_FREQ is the median number of events (number of events/year - temporal).\n"
        metadata += "# sum_freq: Flood frequency for July-September. Compute the average number of flow events with flows below a threshold equal to the 90th percentile for the entire flow record. SUM_FREQ is the median number of events (number of events/year - temporal).\n"
        metadata += "# ma3: Coefficient of variation (standard deviation/mean) for each year. Compute the coefficient of variation for each year of daily flows. Compute the mean of the annual coefficients of variation (percent - temporal).\n"
        metadata += "# ma4: Standard deviation of the percentiles of the logs of the entire flow record divided by the mean of percentiles of the logs. Compute the log10 of the daily flows for the entire record. Compute the 5th; 10th; 15th; 20th; 25th; 30th; 35th; 40th; 45th; 50th; 55th; 60th; 65th; 70th; 75th; 80th; 85th; 90th; and 95th percentiles for the logs of the entire flow record. Percentiles are computed by interpolating between the ordered (ascending) logs of the flow values. Compute the standard deviation and mean for the percentile values. Divide the standard deviation by the mean (percent - spatial).\n"
        metadata += "# ma12: Mean of monthly flow values for January (cubic feet per second - temporal).\n"
        metadata += "# ma13: Mean of monthly flow values for February (cubic feet per second - temporal).\n"
        metadata += "# ma14: Mean of monthly flow values for March (cubic feet per second - temporal).\n"
        metadata += "# ma15: Mean of monthly flow values for April (cubic feet per second - temporal).\n"
        metadata += "# ma16: Mean of monthly flow values for May (cubic feet per second - temporal).\n"
        metadata += "# ma17: Mean of monthly flow values for June (cubic feet per second - temporal).\n"
        metadata += "# ma18: Mean of monthly flow values for July (cubic feet per second - temporal).\n"
        metadata += "# ma19: Mean of monthly flow values for August (cubic feet per second - temporal).\n"
        metadata += "# ma20: Mean of monthly flow values for September (cubic feet per second - temporal).\n"
        metadata += "# ma21: Mean of monthly flow values for October (cubic feet per second - temporal).\n"
        metadata += "# ma22: Mean of monthly flow values for November (cubic feet per second - temporal).\n"
        metadata += "# ma23: Mean of monthly flow values for December (cubic feet per second - temporal).\n"
        metadata += "# ma99: Mean of monthly flow values for the entire year. Compute the mean of the monthly mean flows for each month of the year. MA99 is the mean of these 12 values (cubic feet per second - temporal).\n"
        metadata += "# mh14: Median of annual maximum flows. Compute the annual maximum flows from monthly maximum flows. Compute the ratio of annual maximum flow to median annual flow for each year. MH14 is the median of these ratios (dimensionless - temporal).\n"
        metadata += "# mh20: Specific mean annual maximum flow. MH20 is the mean of the annual maximum flows divided by the drainage area (cubic feet per second/square mile - temporal).\n"
        metadata += "# ml17: Base flow. Compute the mean annual flows. Compute the minimum of a 7-day moving average flow for each year and divide them by the mean annual flow for that year. ML17 is the mean of those ratios (dimensionless - temporal).\n"
        metadata += "# spr_mag: Specific mean spring (April-June) maximum flow. The spr_mag statistic is the median of the annual maximum flows divided by the drainage area (cubic feet per second/square mile - temporal).\n"
        metadata += "# sum_cv: Coefficient of variation (standard deviation/mean) for each year for the summer (July-September). Compute the coefficient of variation for each year of daily flows. Compute the median of the annual coefficients of variation (percent - temporal).\n"
        metadata += "# sum_mag: Minimum of the summer (July-September) flows divided by the drainage area (cubic feet per second/square mile - temporal).\n"
        metadata += "# ra1: Rise rate. Compute the change in flow for days in which the change is positive for the entire flow record. RA1 is the mean of these values (cubic feet per second/day - temporal).\n"
        metadata += "# ra3: Fall rate. Compute the change in flow for days in which the change is negative for the entire flow record. RA3 is the mean of these values (cubic feet per second/day - temporal).\n"
        metadata += "# ra8: Number of reversals. Compute the number of days in each year when the change in flow from one day to the next changes direction. RA8 is the median of the yearly values (days - temporal).\n"
        metadata += "# spr_ord: Julian date of spring (April-June) maximum. Determine the Julian date that the maximum flow occurs for each year. SPR_ORD is the median of these values (Julian day - temporal).\n"
        metadata += "# sum_ord: Julian date of summer (July-September) minimum. Determine the Julian date that the minimum flow occurs for each water year. SUM_ORD is the median of these values (Julian day - temporal).\n"
        metadata += "# th1: Julian date of annual maximum. Determine the Julian date that the maximum flow occurs for each year. TH1 is the median of these values (Julian day - temporal).\n"
        metadata += "# tl1: Julian date of annual minimum. Determine the Julian date that the minimum flow occurs for each water year. TL1 is the median of these values (Julian day - temporal).\n"
    else:
        if "Modeled" in filename_prefix:
            metadata += "# Climatologies are calculated from from modeled daily streamflow data.\n"
            metadata += "# doy is the day of year (1-366) for which the climatology value is reported. \n"
            metadata += "# water_year_index is the water year index (1-366) for which the climatology value is reported. The water year is defined as starting on October 1 (DOY 275 in a 366 day year = water year index 1) and ending September 30 (DOY 274 in a 366 day year = water year index 366).\n"
            metadata += "# doy_min is the minimum streamflow value for the given day of year across all years in the era (cubic feet per second).\n"
            metadata += "# doy_mean is the mean streamflow value for the given day of year across all years in the era (cubic feet per second).\n"
            metadata += "# doy_max is the maximum streamflow value for the given day of year across all years in the era (cubic feet per second).\n"
        else:
            metadata += "# Climatologies are calculated from from observed daily streamflow data.\n"
            metadata += (
                "# The observation record for this time period is "
                + str(source_metadata["percent_complete"])
                + f"% complete.\n"
            )
            metadata += "# doy is the day of year (1-366) for which the climatology value is reported.\n"
            metadata += "# water_year_index is the water year index (1-366) for which the climatology value is reported. The water year is defined as starting on October 1 (DOY 275 in a 366 day year = water year index 1) and ending September 30 (DOY 274 in a 366 day year = water year index 366).\n"
            metadata += "# doy_min is the minimum streamflow value for the given day of year across all available data in the era (cubic feet per second).\n"
            metadata += "# doy_mean is the mean streamflow value for the given day of year across all available data in the era (cubic feet per second).\n"
            metadata += "# doy_max is the maximum streamflow value for the given day of year across all available data in the era (cubic feet per second).\n"

    metadata += "# landcover is the landcover type used to parameterize the precipitation-runoff model.\n"
    metadata += "# model is the global climate model.\n"
    metadata += "# scenario is the emissions scenario.\n"
    metadata += "# era is the time period over which data are summarized.\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": "calculated",
    }


def arctic_hydrology_csv(data, filename_prefix):

    # substrings in filename_prefix denotes endpoint ("Statistics", "Modeled") to aid in packaging CSV

    # data structure for all endpoints:
    # first level of data are strings "data", "id", "latitude", "longitude", "metadata", "name"

    # for stats:
    # the "data" key contains the hydrology data, with levels "model", "era", then a dict of variable ids and their values
    # the "metadata" key contains sub-levels "source"["citation"] and "variables"[<variable_id>]"description" + "units"
    # all other keys contain single values

    # for modeled daily climatology:
    # the "data" key contains the hydrology data, with levels "model", "era", then a list of dicts - one for each DOY, with a "doy" key/value, a "water_year_index" key/value, and additional keys/values for each variable id
    # the "metadata" key is identical to the stats endpoint

    csv_dicts = []
    for key in data.keys():
        if key != "data":
            continue
        for model in data["data"].keys():
            for era in data["data"][model].keys():
                row = {
                    "model": model,
                    "era": era,
                }
                if "Statistics" in filename_prefix:
                    row.update(data["data"][model][era])
                    csv_dicts.append(copy.deepcopy(row))
                else:  # modeled daily climatology
                    for doy_dict in data["data"][model][era]:
                        row["doy"] = doy_dict["doy"]
                        row["water_year_index"] = doy_dict["water_year_index"]
                        for var_key in doy_dict.keys():
                            if var_key != "doy" and var_key != "water_year_index":
                                row[var_key] = doy_dict[var_key]
                        csv_dicts.append(copy.deepcopy(row))
    fieldnames = ["model", "era"]
    if "Statistics" in filename_prefix:
        # get variable keys from metadata to use as fieldnames
        for var_id in data["metadata"]["variables"].keys():
            fieldnames.append(var_id)
    else:
        fieldnames.append("doy")
        # get variable keys from first doy dict to use as fieldnames
        sample_doy_dict = data["data"][model][era][0]
        for var_key in sample_doy_dict.keys():
            if var_key != "doy":
                fieldnames.append(var_key)

    metadata = ""
    if "Statistics" in filename_prefix:
        metadata += "# The following hydrologic statistics are calculated from modeled daily streamflow data:\n"
        metadata += "# ma12: Mean of monthly flow values for January (cubic feet per second - temporal).\n"
        metadata += "# ma13: Mean of monthly flow values for February (cubic feet per second - temporal).\n"
        metadata += "# ma14: Mean of monthly flow values for March (cubic feet per second - temporal).\n"
        metadata += "# ma15: Mean of monthly flow values for April (cubic feet per second - temporal).\n"
        metadata += "# ma16: Mean of monthly flow values for May (cubic feet per second - temporal).\n"
        metadata += "# ma17: Mean of monthly flow values for June (cubic feet per second - temporal).\n"
        metadata += "# ma18: Mean of monthly flow values for July (cubic feet per second - temporal).\n"
        metadata += "# ma19: Mean of monthly flow values for August (cubic feet per second - temporal).\n"
        metadata += "# ma20: Mean of monthly flow values for September (cubic feet per second - temporal).\n"
        metadata += "# ma21: Mean of monthly flow values for October (cubic feet per second - temporal).\n"
        metadata += "# ma22: Mean of monthly flow values for November (cubic feet per second - temporal).\n"
        metadata += "# ma23: Mean of monthly flow values for December (cubic feet per second - temporal).\n"
        metadata += "# ma99: Mean of monthly flow values for the entire year. Compute the mean of the monthly mean flows for each month of the year. MA99 is the mean of these 12 values (cubic feet per second - temporal).\n"
    else:
        metadata += (
            "# Climatologies are calculated from from modeled daily streamflow data.\n"
        )
        metadata += "# doy is the day of year (1-366) for which the climatology value is reported. \n"
        metadata += "# water_year_index is the water year index (1-366) for which the climatology value is reported. The water year is defined as starting on October 1 (DOY 275 in a 366 day year = water year index 1) and ending September 30 (DOY 274 in a 366 day year = water year index 366).\n"
        metadata += "# doy_min is the minimum streamflow value for the given day of year across all years in the era (cubic feet per second).\n"
        metadata += "# doy_mean is the mean streamflow value for the given day of year across all years in the era (cubic feet per second).\n"
        metadata += "# doy_max is the maximum streamflow value for the given day of year across all years in the era (cubic feet per second).\n"

    metadata += "# model is the global climate model.\n"
    metadata += "# era is the time period over which data are summarized.\n"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": "calculated",
    }
