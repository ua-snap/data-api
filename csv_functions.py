from flask import request, Response
import csv
from postprocessing import *
from fetch_data import *
from validate_data import *
from datetime import datetime


def create_csv(
    data,
    endpoint,
    place_id=None,
    lat=None,
    lon=None,
    source_metadata=None,
    filename_prefix=None,
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
    Returns:
        CSV Response
    """
    if not place_id:
        place_id = request.args.get("community")
    place_name, place_type = place_name_and_type(place_id)

    if not endpoint.startswith("places_"):
        metadata = csv_metadata(place_name, place_id, place_type, lat, lon)
    else:
        metadata = ""

    properties = {}

    data = nullify_and_prune(data, endpoint)
    if data in [{}, None, 0]:
        return render_template("404/no_data.html"), 404

    if endpoint == "beetles":
        properties = beetles_csv(data)
    elif endpoint in [
        "heating_degree_days",
        "degree_days_below_zero",
        "thawing_index",
        "freezing_index",
        "heating_degree_days_all",
        "degree_days_below_zero_all",
        "thawing_index_all",
        "freezing_index_all",
    ]:
        properties = degree_days_csv(data, endpoint)
    elif endpoint == "flammability":
        properties = flammability_csv(data)
    elif endpoint in ["gipl", "gipl_summary"]:
        properties = gipl_csv(data, endpoint)
    elif endpoint == "landfastice":
        properties = landfastice_csv(data)
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
    ]:
        properties = taspr_csv(data, endpoint)
    elif endpoint == "veg_type":
        properties = veg_type_csv(data)
    elif endpoint in ["wet_days_per_year", "wet_days_per_year_all"]:
        properties = wet_days_per_year_csv(data, endpoint)
    else:
        return render_template("500/server_error.html"), 500

    # Append data-specific metadata to location metadata.
    properties["metadata"] = metadata + properties["metadata"]

    filename = ""
    if filename_prefix is not None:
        filename += filename_prefix + " "
    filename += properties["filename_data_name"]
    if not endpoint.startswith("places_"):
        filename += " for "
        if place_name is not None:
            filename += quote(place_name)
        else:
            filename += lat + ", " + lon
    filename += ".csv"
    properties["filename"] = filename

    return write_csv(properties)


def csv_metadata(place_name=None, place_id=None, place_type=None, lat=None, lon=None):
    """
    Creates metadata string to add to beginning of CSV file.

    Args:
        place_name (str): Name of the place, None if just lat/lon
        place_id (str): place identifier (e.g., AK124)
        place_type (str): point or area
        lat: latitude for points or None for polygons
        lon: longitude for points or None for polygons

    Returns:
        Multiline metadata string
    """
    metadata = "# Location: "
    if place_name is None:
        metadata += lat + " " + lon + "\n"
        # if lat and lon and type huc12, then it's a local / point-to-huc query
        if place_type == "huc12":
            metadata += "# Corresponding HUC12 code: " + place_id + "\n"
    elif place_type == "community":
        metadata += place_name + "\n"
    else:
        metadata += place_name + " (" + place_type_labels[place_type] + ")\n"

    report_url = WEB_APP_URL + "report/"
    if place_type is None:
        report_url += lat + "/" + lon
    elif place_type == "community":
        report_url += "community/" + place_id
    else:
        report_url += "area/" + place_id
    metadata += "# View a report for this location at " + report_url + "\n"

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
            row_di[value] = get_from_dict(packaged_data, coords)
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
            "Content-Disposition": 'attachment; filename="'
            + properties["filename"]
            + "\"; filename*=utf-8''\""
            + properties["filename"]
            + '"',
        },
    )
    return response


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


def degree_days_csv(data, endpoint):
    if endpoint in [
        "heating_degree_days",
        "degree_days_below_zero",
        "thawing_index",
        "freezing_index",
    ]:
        coords = ["model"]
        values = ["ddmin", "ddmean", "ddmax"]
    elif endpoint in [
        "heating_degree_days_all",
        "degree_days_below_zero_all",
        "thawing_index_all",
        "freezing_index_all",
    ]:
        coords = ["model", "year"]
        values = ["dd"]

    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)

    if endpoint in ["heating_degree_days", "heating_degree_days_all"]:
        filename_data_name = "Heating Degree Days"
        metadata = (
            "# dd is the total annual degree days below 65°F for the specified model\n"
        )
    elif endpoint in ["degree_days_below_zero", "degree_days_below_zero_all"]:
        filename_data_name = "Degree Days Below Zero"
        metadata = (
            "# dd is the total annual degree days below 0°F for the specified model\n"
        )
    elif endpoint in ["thawing_index", "thawing_index_all"]:
        filename_data_name = "Thawing Index"
        metadata = "# dd is the total annual degree days above freezing for the specified model\n"
    elif endpoint in ["freezing_index", "freezing_index_all"]:
        filename_data_name = "Freezing Index"
        metadata = "# dd is the total annual degree days below freezing for the specified model\n"

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
    elif endpoint == "gipl":
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
    metadata = "# GIPL model outputs for ten variables including mean annual ground temperature (deg C) at various depths below the surface as well as talik thickness (m) and depths of permafrost base and top (m)\n"
    filename_data_name = "GIPL 1 km Model Outputs"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
        "metadata": metadata,
        "filename_data_name": filename_data_name,
    }


def landfastice_csv(data):
    # Reformat data to nesting structure expected by other CSV functions.
    for key, value in data.items():
        data[key] = {"status": value}
    coords = ["date"]
    values = ["status"]
    fieldnames = coords + values
    csv_dicts = build_csv_dicts(data, fieldnames, values=values)
    metadata = (
        "# Landfast Ice Status: A 0 value indicates absence and 1 indicates presence.\n"
    )
    filename_data_name = "Landfast Ice Extent"

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
        "jorg": {"coords": ["source"], "values": ["ice", "pfx"]},
        "obu_magt": {"coords": ["source"], "values": ["year", "depth", "temp"]},
        "obupfx": {"coords": ["source"], "values": ["pfx"]},
    }

    metadata = "# magt*m is the mean annual ground temperature at a given depth (* meters) in degrees Celsius\n"
    metadata += "# magtsurface is the mean annual ground temperature at the ground surface in degrees Celsius\n"
    metadata += "# permafrost base is the lower boundary of the permafrost below the surface in meters\n"
    metadata += "# permafrost top is the upper boundary of the permafrost below the surface in meters\n"
    metadata += "# talikthickness is the thickness of the perennially unfrozen ground occurring in permafrost terrain in meters\n"
    metadata += "# ice is the estimated ground ice volume\n"
    metadata += "# pfx is the permafrost extent\n"

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
        coords = ["return_interval", "duration", "model", "era"]
        values = ["pf", "pf_lower", "pf_upper"]
        fieldnames = coords + values
        csv_dicts = build_csv_dicts(data, fieldnames, values=values)
        metadata = "# return_interval is used for the Annual Exceedance Probability (1 / return_interval)\n"
        metadata += "# duration is the amount of time for the predicted amount of precipitation\n"
        metadata += "# model is the model the data is derived from\n"
        metadata += (
            "# era is the time range for this predicted amount of precipitation \n"
        )
        metadata += "# pf is the amount of precipitation predicted\n"
        metadata += "# pf_lower is the lower bound of the 95% confidence interval of the variable pf\n"
        metadata += "# pf_upper is the upper bound of the 95% confidence interval of the variable pf\n"
        filename_data_name = "Future Projections of Precipitation"

    # Change "CRU_historical" scenario to just "Historical".
    for csv_dict in csv_dicts:
        if endpoint != "proj_precip" and csv_dict["scenario"] == "CRU_historical":
            csv_dict["scenario"] = "Historical"

    return {
        "csv_dicts": csv_dicts,
        "fieldnames": fieldnames,
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
