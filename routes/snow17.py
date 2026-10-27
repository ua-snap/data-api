import asyncio
import logging
import math
import numpy as np
from aiohttp import ClientSession
from flask import Blueprint, render_template, request

# local imports
from generate_urls import generate_wcs_query_url, generate_describe_coverage_url
from generate_requests import (
    generate_wcs_getcov_str,
    generate_wcps_describe_coverage_str,
)
from fetch_data import make_get_request, fetch_geoserver_data, describe_via_wcps
from validate_request import (
    latlon_is_numeric_and_in_geodetic_range,
    construct_latlon_bbox_from_coverage_bounds,
    validate_latlon_in_bboxes,
    project_latlon,
    generate_time_index_from_coverage_metadata,
)
from snow17_model import snow17, DEFAULT_PARAMS
from config import GS_BASE_URL
from luts import cmip6_downscaled_options, DATA_ERRORS, FETCH_ERRORS
from .elevation import package_astergdem, wms_targets as elevation_wms_targets

from . import routes

logger = logging.getLogger(__name__)

snow17_api = Blueprint("snow17_api", __name__)

# Snow-17 is forced with daily mean air temperature (the mean of tasmax and tasmin)
# and daily precipitation, so only model/scenario combos that have all three are usable.
snow17_forcing_vars = ["pr", "tasmax", "tasmin"]
snow17_options = {
    model: [
        scenario
        for scenario in scenarios
        if all(
            scenario in cmip6_downscaled_options[var].get(model, [])
            for var in snow17_forcing_vars
        )
    ]
    for model, scenarios in cmip6_downscaled_options["tasmax"].items()
}
all_snow17_models = list(snow17_options.keys())
all_snow17_scenarios = sorted(
    set(scenario for scenarios in snow17_options.values() for scenario in scenarios)
)

# Limit concurrent requests to Rasdaman, since a full query is ~200 requests
max_concurrent_requests = 10

nodata_value = -9999

# Design snow load summary options. Eras are inclusive ranges of water years.
design_eras = [(1966, 2014), (2040, 2069), (2070, 2099)]
design_return_period = 50
# Minimum number of snowy years needed to fit a GEV
min_gev_sample_size = 10
gravity = 9.80665  # m/s^2
pa_per_psf = 47.880259


def get_cov_id(var, model, scenario):
    return f"cmip6_downscaled_{var}_{model}_{scenario}_v2_wcs".replace("-", "_")


async def fetch_forcing_combo(session, semaphore, model, scenario, x, y):
    """
    Fetch the time index and the daily pr, tasmax, and tasmin values for one model/scenario combo.

    Args:
        session (aiohttp.ClientSession): the client session instance
        semaphore (asyncio.Semaphore): limits concurrent requests
        model (str): model name
        scenario (str): scenario name
        x (float): x-coordinate (EPSG:3338)
        y (float): y-coordinate (EPSG:3338)

    Returns:
        dict: "time" (pd.DatetimeIndex) plus a numpy array for each forcing variable,
        or None if a variable's values don't line up with the time axis
    """

    async def get(url):
        async with semaphore:
            return await make_get_request(url, session)

    # All variables for a model/scenario share a time axis, so describe one of them.
    describe_url = generate_describe_coverage_url(
        generate_wcps_describe_coverage_str(get_cov_id("pr", model, scenario))
    )
    data_urls = [
        generate_wcs_query_url(
            generate_wcs_getcov_str(x, y, cov_id=get_cov_id(var, model, scenario))
        )
        for var in snow17_forcing_vars
    ]
    metadata, *var_data = await asyncio.gather(
        get(describe_url), *[get(url) for url in data_urls]
    )

    forcing = {"time": generate_time_index_from_coverage_metadata(metadata)}
    for var, values in zip(snow17_forcing_vars, var_data):
        # Some coverages are malformed upstream (e.g., a single time step), and a
        # single-time-step coverage comes back as a scalar rather than a list.
        if not isinstance(values, list) or len(values) != len(forcing["time"]):
            logger.warning(
                f"{var} for {model} {scenario} does not match the time axis of "
                f"{get_cov_id('pr', model, scenario)}, skipping"
            )
            return None
        arr = np.array([np.nan if v is None else v for v in values], dtype=float)
        arr[arr == nodata_value] = np.nan
        forcing[var] = arr
    return forcing


async def fetch_all_forcings(combos, x, y):
    """
    Fetch the forcing data for all model/scenario combos concurrently.

    Args:
        combos (list): list of (model, scenario) tuples
        x (float): x-coordinate (EPSG:3338)
        y (float): y-coordinate (EPSG:3338)

    Returns:
        dict: forcing data keyed by (model, scenario)
    """
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    async with ClientSession() as session:
        results = await asyncio.gather(
            *[
                fetch_forcing_combo(session, semaphore, model, scenario, x, y)
                for model, scenario in combos
            ]
        )
    return dict(zip(combos, results))


def get_elevation(lat, lon):
    """
    Get the mean ASTER GDEM elevation (m) for the 1km cell containing the point, or None if unavailable.
    """
    try:
        results = asyncio.run(
            fetch_geoserver_data(
                GS_BASE_URL, "dem", elevation_wms_targets, {}, lat, lon
            )
        )
        packaged = package_astergdem(results)
        if packaged is None or packaged["mean"] is None:
            return None
        return float(packaged["mean"])
    except FETCH_ERRORS + DATA_ERRORS as exc:
        logger.warning(f"Could not fetch elevation for Snow-17 run: {exc}")
        return None


def parse_snow17_params(args):
    """
    Build the Snow-17 parameter dict from defaults plus any overrides in the HTTP GET parameters.

    Raises:
        ValueError: if a parameter override is not a valid value
    """
    params = dict(DEFAULT_PARAMS)
    for name, default in DEFAULT_PARAMS.items():
        if args.get(name) is None:
            continue
        params[name] = int(args.get(name)) if name == "rvs" else float(args.get(name))

    if params["rvs"] not in (0, 1, 2):
        raise ValueError("rvs must be 0, 1, or 2")
    if not 0 < params["tipm"] < 1:
        raise ValueError("tipm must be between 0 and 1")
    if not 0 <= params["plwhc"] <= 0.4:
        raise ValueError("plwhc must be between 0 and 0.4")
    if params["mbase"] < 0:
        raise ValueError("mbase must be at least 0")
    if params["mfmax"] <= 0 or params["mfmin"] <= 0:
        raise ValueError("mfmax and mfmin must be greater than 0")
    if params["pxtemp1"] >= params["pxtemp2"]:
        raise ValueError("pxtemp1 must be less than pxtemp2")
    return params


def run_snow17_for_forcing(forcing, lat, elevation, params, state=None):
    """Run Snow-17 on one model/scenario's forcing data, returning (swe, outflow, state)."""
    tair = (forcing["tasmax"] + forcing["tasmin"]) / 2
    return snow17(
        forcing["time"].dayofyear.values,
        forcing["pr"],
        tair,
        lat=lat,
        elevation=elevation,
        dt=24,
        state=state,
        **params,
    )


def package_daily(time, swe, outflow):
    """Package daily Snow-17 output as {date: {"swe": value, "outflow": value}}"""
    return {
        t.strftime("%Y-%m-%d"): {"swe": round(s, 1), "outflow": round(o, 1)}
        for t, s, o in zip(time, swe.tolist(), outflow.tolist())
    }


def package_annual(time, swe, outflow):
    """
    Summarize daily Snow-17 output by water year (October 1 - September 30), labeled by
    the calendar year in which the water year ends. Only complete water years are included.
    """
    water_years = time.year + (time.month >= 10)
    di = {}
    for water_year in np.unique(water_years):
        mask = water_years == water_year
        wy_time = time[mask]
        # Skip partial water years at the start/end of the record
        if not (
            wy_time[0].month == 10
            and wy_time[0].day == 1
            and wy_time[-1].month == 9
            and wy_time[-1].day == 30
        ):
            continue
        wy_swe = swe[mask]
        peak_idx = int(np.argmax(wy_swe))
        di[str(water_year)] = {
            "peak_swe": round(float(wy_swe[peak_idx]), 1),
            "peak_swe_date": (
                wy_time[peak_idx].strftime("%Y-%m-%d")
                if wy_swe[peak_idx] > 0
                else None
            ),
            "snow_days": int(np.sum(wy_swe >= 1)),
            "total_outflow": round(float(outflow[mask].sum()), 1),
        }
    return di


def fit_gev_lmoments(values):
    """
    Fit a generalized extreme value (GEV) distribution by the method of L-moments (Hosking, 1990).

    Uses Hosking's parameterization, where shape k > 0 has a bounded upper tail and k < 0 has a
    heavy upper tail. This matches the sign of scipy.stats.genextreme's shape parameter c.

    Args:
        values (array-like): sample of annual maxima

    Returns:
        tuple: (location, scale, shape)
    """
    x = np.sort(np.asarray(values, dtype=float))
    n = len(x)
    i = np.arange(1, n + 1)
    b0 = x.mean()
    b1 = np.sum((i - 1) / (n - 1) * x) / n
    b2 = np.sum((i - 1) * (i - 2) / ((n - 1) * (n - 2)) * x) / n
    l1 = b0
    l2 = 2 * b1 - b0
    l3 = 6 * b2 - 6 * b1 + b0
    if l2 <= 0:
        # Every value is the same
        return l1, 0.0, 0.0

    # Hosking's rational approximation for the shape parameter
    c = 2 / (3 + l3 / l2) - math.log(2) / math.log(3)
    k = 7.8590 * c + 2.9554 * c**2
    if abs(k) < 1e-6:
        # Gumbel limit
        scale = l2 / math.log(2)
        return l1 - 0.5772156649 * scale, scale, 0.0
    scale = l2 * k / ((1 - 2 ** (-k)) * math.gamma(1 + k))
    location = l1 - scale * (1 - math.gamma(1 + k)) / k
    return location, scale, k


def gev_quantile(location, scale, shape, non_exceedance):
    """Quantile of a GEV distribution (Hosking's parameterization) at a non-exceedance probability."""
    y = -math.log(non_exceedance)
    if abs(shape) < 1e-6:
        return location - scale * math.log(y)
    return location + scale / shape * (1 - y**shape)


def design_snow_load(peaks, return_period=design_return_period):
    """
    Estimate the return-period annual peak SWE from a sample of annual peaks.

    Years with no snow are handled as a mixed distribution: a GEV is fit to the nonzero peaks
    and the target non-exceedance probability is adjusted for the fraction of snow-free years.

    Args:
        peaks (array-like): annual peak SWE values (mm)
        return_period (int): return period (years)

    Returns:
        dict: return-period SWE (mm) and load (kPa, psf), plus the fitted GEV parameters,
        or None values if there are too few snowy years to fit
    """
    peaks = np.asarray(peaks, dtype=float)
    nonzero = peaks[peaks > 0]
    p_zero = 1 - len(nonzero) / len(peaks)
    non_exceedance = 1 - 1 / return_period

    di = {
        "n_years": len(peaks),
        "n_snow_free_years": int(len(peaks) - len(nonzero)),
        "mean_peak_swe_mm": round(float(peaks.mean()), 1),
    }
    if non_exceedance <= p_zero:
        swe = 0.0
        location = scale = shape = None
    elif len(nonzero) < min_gev_sample_size:
        swe = None
        location = scale = shape = None
    else:
        location, scale, shape = fit_gev_lmoments(nonzero)
        conditional = (non_exceedance - p_zero) / (1 - p_zero)
        swe = max(gev_quantile(location, scale, shape, conditional), 0.0)

    if swe is None:
        di.update({"swe_mm": None, "load_kpa": None, "load_psf": None})
    else:
        # 1 mm of SWE is 1 kg/m^2 of water
        load_pa = swe * gravity
        di.update(
            {
                "swe_mm": round(swe, 1),
                "load_kpa": round(load_pa / 1000, 2),
                "load_psf": round(load_pa / pa_per_psf, 1),
            }
        )
    di["gev"] = {
        "method": "L-moments",
        "location": None if location is None else round(location, 2),
        "scale": None if scale is None else round(scale, 2),
        "shape": None if shape is None else round(shape, 4),
    }
    return di


def package_design(time, swe, outflow):
    """
    Summarize daily Snow-17 output as a design snow load for each era that the run fully covers.
    Eras are ranges of water years, so the historical run gets 1966-2014 and future runs get
    2040-2069 and 2070-2099.
    """
    annual = package_annual(time, swe, outflow)
    di = {}
    for start, end in design_eras:
        years = [str(year) for year in range(start, end + 1)]
        if not all(year in annual for year in years):
            continue
        peaks = [annual[year]["peak_swe"] for year in years]
        di[f"{start}-{end}"] = design_snow_load(peaks)
    return di


@routes.route("/snow17/")
@routes.route("/snow17/point/")
def snow17_about():
    return render_template("/documentation/snow17.html")


@routes.route("/snow17/point/<lat>/<lon>")
def snow17_point(lat, lon):
    """
    Run the Snow-17 snow accumulation and ablation model at a point, forced by
    CMIP6 downscaled daily data, for each requested model/scenario.

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        dict: Snow-17 output for each model/scenario, plus the inputs used to run it

    Notes:
        example request (all models/scenarios): /snow17/point/64.84/-147.72
        example request (specific model): /snow17/point/64.84/-147.72?models=GFDL-ESM4
        example request (specific scenario): /snow17/point/64.84/-147.72?scenarios=ssp585
        example request (water year summary): /snow17/point/64.84/-147.72?summary=annual
        example request (design snow load): /snow17/point/64.84/-147.72?summary=design
        example request (parameter override): /snow17/point/64.84/-147.72?scf=1.2&mfmax=1.5
    """
    validation = latlon_is_numeric_and_in_geodetic_range(lat, lon)
    if validation == 400:
        return render_template("400/bad_request.html"), 400
    lat, lon = float(lat), float(lon)

    if request.args.get("models"):
        models = request.args.get("models").split(",")
        if not all(model in all_snow17_models for model in models):
            return render_template("400/bad_request.html"), 400
    else:
        models = all_snow17_models

    if request.args.get("scenarios"):
        scenarios = request.args.get("scenarios").split(",")
        if not all(scenario in all_snow17_scenarios for scenario in scenarios):
            return render_template("400/bad_request.html"), 400
    else:
        scenarios = all_snow17_scenarios

    summary = request.args.get("summary")
    if summary not in (None, "annual", "design"):
        return render_template("400/bad_request.html"), 400

    try:
        params = parse_snow17_params(request.args)
        elevation = request.args.get("elevation")
        if elevation is not None:
            elevation = float(elevation)
            elevation_source = "user"
    except ValueError:
        return render_template("400/bad_request.html"), 400

    # Future scenarios are seeded with the model state at the end of the historical run,
    # so the historical forcing is always fetched even if it isn't requested.
    requested = {
        model: [s for s in snow17_options[model] if s in scenarios] for model in models
    }
    combos = [
        (model, scenario)
        for model, model_scenarios in requested.items()
        if model_scenarios
        for scenario in snow17_options[model]
        if scenario == "historical" or scenario in model_scenarios
    ]
    if not combos:
        return render_template("400/bad_request.html"), 400

    try:
        # All CMIP6 downscaled coverages share the same extent, so check against one.
        bbox_cov_id = get_cov_id("pr", *combos[0])
        metadata = asyncio.run(describe_via_wcps(bbox_cov_id))
        bbox = construct_latlon_bbox_from_coverage_bounds(metadata)
        within_bounds = validate_latlon_in_bboxes(lat, lon, [bbox], [bbox_cov_id])
        if within_bounds == 404:
            return render_template("404/no_data.html"), 404
        if within_bounds == 422:
            return (
                render_template(
                    "422/invalid_latlon_outside_coverage.html", bboxes=[bbox]
                ),
                422,
            )

        x, y = project_latlon(lat, lon, 3338)
        forcings = asyncio.run(fetch_all_forcings(combos, x, y))
    except ValueError:
        return render_template("400/bad_request.html"), 400
    except FETCH_ERRORS + DATA_ERRORS as exc:
        if hasattr(exc, "status") and exc.status == 404:
            return render_template("404/no_data.html"), 404
        logger.exception("Error fetching Snow-17 forcing data")
        return render_template("500/server_error.html"), 500

    # A point inside the bbox but off the data grid (e.g. ocean) comes back as all nodata.
    if all(f is None or np.isnan(f["pr"]).all() for f in forcings.values()):
        return render_template("404/no_data.html"), 404

    if elevation is None:
        elevation = get_elevation(lat, lon)
        elevation_source = "ASTER GDEM 1km mean"
        if elevation is None:
            elevation = 0.0
            elevation_source = "default (elevation lookup failed)"

    package = {
        "annual": package_annual,
        "design": package_design,
    }.get(summary, package_daily)
    data = {}
    skipped = []
    cold_started = []
    for model, model_scenarios in requested.items():
        if not model_scenarios:
            continue
        historical_state = None
        # Historical comes first in snow17_options, so its end state is ready for the SSPs.
        for scenario in snow17_options[model]:
            if scenario != "historical" and scenario not in model_scenarios:
                continue
            forcing = forcings[(model, scenario)]
            if forcing is None or any(
                np.isnan(forcing[var]).any() for var in snow17_forcing_vars
            ):
                skipped.append(f"{model} {scenario}")
                continue
            # If the historical run was skipped, future scenarios start with no snow.
            state = None if scenario == "historical" else historical_state
            if scenario != "historical" and state is None:
                cold_started.append(f"{model} {scenario}")
            swe, outflow, end_state = run_snow17_for_forcing(
                forcing, lat, elevation, params, state
            )
            if scenario == "historical":
                historical_state = end_state
            if scenario in model_scenarios:
                data.setdefault(model, {})[scenario] = package(
                    forcing["time"], swe, outflow
                )

    if skipped:
        logger.warning(f"Skipped Snow-17 runs with missing forcing data: {skipped}")
    if not data:
        return render_template("404/no_data.html"), 404

    return {
        "metadata": {
            "model": "Snow-17",
            "forcing": "CMIP6 downscaled daily pr and mean of tasmax/tasmin",
            "units": {
                "swe": "mm",
                "outflow": "mm/day",
                "peak_swe": "mm",
                "snow_days": "days with SWE >= 1 mm",
                "total_outflow": "mm",
                "swe_mm": "mm",
                "load_kpa": "kPa",
                "load_psf": "lb/ft^2",
            },
            "summary": summary or "daily",
            **(
                {
                    "design": {
                        "return_period_years": design_return_period,
                        "statistic": "annual peak SWE, by water year",
                        "distribution": "GEV fit to nonzero peaks by L-moments, "
                        "adjusted for snow-free years",
                        "load": "ground snow load, SWE x g",
                    }
                }
                if summary == "design"
                else {}
            ),
            "latitude": lat,
            "longitude": lon,
            "elevation_m": round(elevation, 1),
            "elevation_source": elevation_source,
            "parameters": params,
            "skipped": skipped,
            "cold_started": cold_started,
        },
        "data": data,
    }

