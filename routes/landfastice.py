import asyncio

import pandas as pd
from flask import (
    Blueprint,
    render_template,
    request,
)

# local imports
from . import routes
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
    project_latlon,
    validate_xy_in_coverage_extent,
)
from fetch_data import (
    fetch_wcs_point_data,
    describe_via_wcps,
)
from postprocessing import prune_nulls_with_max_intensity, postprocess
from csv_functions import create_csv

# following are global because we only need to fetch metadata once
# but must do it to determine request validity and what coverage to query
landfastice_api = Blueprint("landfastice_api", __name__)
beaufort_daily_slie_id = "ardac_beaufort_daily_slie"
chukchi_daily_slie_id = "ardac_chukchi_daily_slie"
beaufort_meta = asyncio.run(describe_via_wcps(beaufort_daily_slie_id))
chukchi_meta = asyncio.run(describe_via_wcps(chukchi_daily_slie_id))


def generate_time_index_from_coverage_metadata(meta):
    """Generate a pandas DatetimeIndex from the ansi (i.e. time) axis coordinates in the coverage description metadata.

    CP Note: function is a good candidate to move to a utility module, as it is not necessarily specific to landfast ice data. It could be used to package any OGC coverage with an `ansi` axis where the full temporal range is desired for packaging.

    Args:
        meta (dict): JSON-like dictionary containing coverage metadata

    Returns:
        pd.DatetimeIndex: corresponding to the ansi (i.e. time) axis coordinates
    """
    try:
        # we won't always know the axis positioning / ordering
        ansi_axis = next(
            axis
            for axis in meta["domainSet"]["generalGrid"]["axis"]
            if axis["axisLabel"] == "ansi"
        )
        # this is a list of dates formatted like "1996-11-03T00:00:00.000Z"
        ansi_coordinates = ansi_axis["coordinate"]
        date_index = pd.DatetimeIndex(ansi_coordinates)
        return date_index
    except (KeyError, StopIteration):
        raise ValueError("Unexpected coverage metadata: 'ansi' axis not found")


def package_landfastice_data(landfastice_resp, meta):
    """Package landfast ice extent data into a JSON-like dict.

    Arguments:
        landfastice_resp (list) -- the response from the WCS GetCoverage request

    Returns:
        di (dict) -- a dict where the key is a single date and the value is the landfast ice status (255 = landfast ice is present, 0 = absence)
    """
    time_index = generate_time_index_from_coverage_metadata(meta)
    di = {}
    for dt, ice_value in zip(list(time_index), landfastice_resp):
        di[dt.date().strftime("%Y-%m-%d")] = ice_value
    return di


@routes.route("/landfastice/")
@routes.route("/landfastice/abstract/")
@routes.route("/landfastice/point/")
def about_landfastice():
    return render_template("documentation/landfastice.html")


@routes.route("/landfastice/point/<lat>/<lon>/")
def run_point_fetch_all_landfastice(lat, lon):
    """Run the async request for all landfast ice data at a single point.
    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        JSON-like dict of landfast ice extent data
    """
    # ensure the coordinates are numeric and in +/- 90, +/- 180 range
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    # now construct bboxes to check if the point is within any coverage extent
    beaufort_bbox = construct_latlon_bbox_from_coverage_bounds(beaufort_meta)
    chukchi_bbox = construct_latlon_bbox_from_coverage_bounds(chukchi_meta)
    within_bounds = validate_latlon_in_bboxes(lat, lon, [beaufort_bbox, chukchi_bbox])
    if within_bounds == 422:
        return (
            render_template(
                "422/invalid_latlon_outside_coverage.html",
                bboxes=[beaufort_bbox, chukchi_bbox],
            ),
            422,
        )
    # next, project the lat lon and determine which coverage to query
    x, y = project_latlon(lat, lon, 3338)
    # 10 km buffer query for locations at edges of the 3338 projection
    if validate_xy_in_coverage_extent(
        x, y, beaufort_meta, east_tolerance=10000, north_tolerance=10000
    ):
        target_coverage = beaufort_daily_slie_id
        target_meta = beaufort_meta
    elif validate_xy_in_coverage_extent(
        x, y, chukchi_meta, west_tolerance=10000, north_tolerance=10000
    ):
        target_coverage = chukchi_daily_slie_id
        target_meta = chukchi_meta
    else:
        # this should never happen, because we already establish the point is syntactically valid and within the bounds of at least one bounding box
        return render_template("500/server_error.html"), 500
    try:
        rasdaman_response = asyncio.run(fetch_wcs_point_data(x, y, target_coverage))
        landfastice_time_series = package_landfastice_data(
            rasdaman_response, target_meta
        )
        postprocessed = prune_nulls_with_max_intensity(
            postprocess(landfastice_time_series, "landfast_sea_ice")
        )
        if request.args.get("format") == "csv":
            return create_csv(postprocessed, "landfast_sea_ice", lat=lat, lon=lon)
        return postprocessed
    except Exception as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        return render_template("500/server_error.html"), 500
