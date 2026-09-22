import csv
import io
import json

import pytest

from tests.json_compare import assert_json_allclose

# The full unfiltered response nests model -> scenario -> date -> var, with a
# full daily time series (potentially 80+ years) per model/scenario pair --
# tens of MB even after keeping only a handful of top-level model keys. Bound
# every level (not just the top one) so the saved fixture stays well under
# the 25MB cap while still exercising real multi-level structure and values.
_N_MODELS = 5
_N_SCENARIOS = 2
_N_DATES = 5


def _bounded_subset(data):
    """Deterministic, deeply-bounded slice of a model->scenario->date->var dict."""
    subset = {}
    for model in list(data.keys())[:_N_MODELS]:
        subset[model] = {}
        for scenario in list(data[model].keys())[:_N_SCENARIOS]:
            subset[model][scenario] = dict(list(data[model][scenario].items())[:_N_DATES])
    return subset


@pytest.mark.timeout(600)
def test_cmip6_downscaled_point_fairbanks(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at Fairbanks, AK.

    Unfiltered, this endpoint fetches every variable x model x scenario
    combination sequentially (a separate describe + getcoverage request per
    combo), which can legitimately take several minutes -- give it more room
    than pytest-timeout's default before it's considered hung/failed.
    """
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Full payload exceeds the 25MB fixture cap, so only a deeply-bounded
    # subset (see _bounded_subset above) is compared against a saved fixture.
    assert isinstance(actual_data, dict) and actual_data
    actual_subset = _bounded_subset(actual_data)

    with open("tests/routes/cmip6_downscaled/point/json/cmip6_downscaled_point_fairbanks_subset.json") as f:
        expected_subset = json.load(f)

    assert_json_allclose(actual_subset, expected_subset)


@pytest.mark.timeout(600)
def test_cmip6_downscaled_point_ocean(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at an ocean point; within the coverage's extent, so real data is expected.

    Unfiltered, this endpoint fetches every variable x model x scenario
    combination sequentially (a separate describe + getcoverage request per
    combo), which can legitimately take several minutes -- give it more room
    than pytest-timeout's default before it's considered hung/failed.
    """
    response = client.get("/cmip6_downscaled/point/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Full payload exceeds the 25MB fixture cap, so only a deeply-bounded
    # subset (see _bounded_subset above) is compared against a saved fixture.
    assert isinstance(actual_data, dict) and actual_data
    actual_subset = _bounded_subset(actual_data)

    with open("tests/routes/cmip6_downscaled/point/json/cmip6_downscaled_point_ocean_subset.json") as f:
        expected_subset = json.load(f)

    assert_json_allclose(actual_subset, expected_subset)


def test_cmip6_downscaled_point_attu(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at Attu, AK; outside the coverage's extent."""
    response = client.get("/cmip6_downscaled/point/52.8339/173.1794")
    assert response.status_code == 422


@pytest.mark.timeout(600)
def test_cmip6_downscaled_point_dawson_city(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at Dawson City, Yukon.

    Unfiltered, this endpoint fetches every variable x model x scenario
    combination sequentially (a separate describe + getcoverage request per
    combo), which can legitimately take several minutes -- give it more room
    than pytest-timeout's default before it's considered hung/failed.
    """
    response = client.get("/cmip6_downscaled/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Full payload exceeds the 25MB fixture cap, so only a deeply-bounded
    # subset (see _bounded_subset above) is compared against a saved fixture.
    assert isinstance(actual_data, dict) and actual_data
    actual_subset = _bounded_subset(actual_data)

    with open("tests/routes/cmip6_downscaled/point/json/cmip6_downscaled_point_dawson_city_subset.json") as f:
        expected_subset = json.load(f)

    assert_json_allclose(actual_subset, expected_subset)


def test_cmip6_downscaled_point_vars(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon>?vars= at Fairbanks, AK returns a parseable response."""
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164?vars=tasmax")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_cmip6_downscaled_point_models(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon>?models= at Fairbanks, AK returns a parseable response."""
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164?models=7ModelAvg")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_cmip6_downscaled_point_scenarios(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon>?scenarios= at Fairbanks, AK returns a parseable response."""
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164?scenarios=ssp585")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_cmip6_downscaled_point_csv(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
