import numpy as np
import pandas as pd
import pytest

from snow17_model import snow17, DEFAULT_PARAMS
from routes.snow17 import (
    design_snow_load,
    fit_gev_lmoments,
    gev_quantile,
    package_design,
)


def test_snow17_accumulates_when_cold():
    """All precipitation below the snow threshold accumulates as SWE with no outflow."""
    days = np.arange(1, 31)
    prec = np.full(30, 5.0)
    tair = np.full(30, -10.0)
    swe, outflow, _ = snow17(days, prec, tair, lat=64.8, **DEFAULT_PARAMS)
    assert np.allclose(swe, np.cumsum(prec))
    assert np.all(outflow == 0)


def test_snow17_melts_out_and_conserves_mass():
    """A snowpack melts out in a warm spell, and water in roughly equals water out."""
    days = np.arange(60, 240)
    prec = np.concatenate([np.full(60, 4.0), np.zeros(120)])
    tair = np.concatenate([np.full(60, -8.0), np.full(120, 10.0)])
    swe, outflow, state = snow17(days, prec, tair, lat=64.8, **DEFAULT_PARAMS)
    assert swe.max() > 200
    assert swe[-1] == 0
    assert state["w_i"] == 0 and state["w_q"] == 0
    assert np.isclose(outflow.sum(), prec.sum(), rtol=0.01)


def test_snow17_state_continuation():
    """Running in two pieces from carried-over state matches one continuous run."""
    rng = np.random.default_rng(0)
    days = np.tile(np.arange(1, 366), 2)
    prec = rng.gamma(0.5, 4, len(days))
    tair = -15 * np.cos(2 * np.pi * (days - 15) / 365) + rng.normal(0, 3, len(days))

    swe, outflow, _ = snow17(days, prec, tair, lat=64.8, **DEFAULT_PARAMS)
    swe_a, outflow_a, state = snow17(
        days[:365], prec[:365], tair[:365], lat=64.8, **DEFAULT_PARAMS
    )
    swe_b, outflow_b, _ = snow17(
        days[365:], prec[365:], tair[365:], lat=64.8, state=state, **DEFAULT_PARAMS
    )
    assert np.allclose(swe, np.concatenate([swe_a, swe_b]))
    assert np.allclose(outflow, np.concatenate([outflow_a, outflow_b]))


def test_snow17_route_rejects_bad_params(client):
    # Out-of-range parameters and models without temperature data are rejected by the route
    assert client.get("/snow17/point/64.84/-147.72?tipm=2").status_code == 400
    assert client.get("/snow17/point/64.84/-147.72?pxtemp1=5").status_code == 400
    assert client.get("/snow17/point/64.84/-147.72?models=CESM2").status_code == 400
    assert client.get("/snow17/point/abc/-147.72").status_code == 400
    # Malformed parameters are rejected by the app-wide query parameter schema
    assert client.get("/snow17/point/64.84/-147.72?scf=abc").status_code == 422
    assert client.get("/snow17/point/64.84/-147.72?summary=weekly").status_code == 422


def test_gev_lmoments_recovers_known_distribution():
    """L-moment GEV fit recovers the parameters and 50-year quantile of a known GEV."""
    stats = pytest.importorskip("scipy.stats")
    true = {"c": 0.1, "loc": 100, "scale": 30}
    sample = stats.genextreme.rvs(size=20000, random_state=1, **true)
    location, scale, shape = fit_gev_lmoments(sample)
    assert np.isclose(location, true["loc"], rtol=0.02)
    assert np.isclose(scale, true["scale"], rtol=0.03)
    assert np.isclose(shape, true["c"], atol=0.02)
    assert np.isclose(
        gev_quantile(location, scale, shape, 0.98),
        stats.genextreme.ppf(0.98, **true),
        rtol=0.02,
    )


def test_design_snow_load_units_and_snow_free_years():
    # A constant 100 mm peak is 100 kg/m^2, about 0.98 kPa or 20.5 psf
    constant = design_snow_load([100.0] * 30)
    assert constant["swe_mm"] == 100.0
    assert constant["load_kpa"] == 0.98
    assert constant["load_psf"] == 20.5

    # The 50-year value is 0 when at least 98% of years have no snow, and can't be fit
    # when fewer than 10 years have snow
    assert design_snow_load([0.0] * 30)["swe_mm"] == 0.0
    sparse = design_snow_load([50.0] * 5 + [0.0] * 50)
    assert sparse["swe_mm"] is None

    # Snow-free years lower the 50-year value relative to the snowy years alone
    rng = np.random.default_rng(2)
    snowy = list(rng.gamma(9, 15, 30))
    assert (
        design_snow_load(snowy[:27] + [0.0] * 3)["swe_mm"]
        < design_snow_load(snowy)["swe_mm"]
    )


def test_package_design_eras():
    """Only eras fully covered by complete water years are summarized."""
    time = pd.date_range("1965-01-01", "2014-12-31", freq="D")
    swe = np.where(time.month.isin([12, 1, 2, 3]), 100.0, 0.0)
    design = package_design(time, swe, np.zeros(len(time)))
    assert list(design) == ["1966-2014"]
    assert design["1966-2014"]["n_years"] == 49
