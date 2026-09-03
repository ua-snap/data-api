import csv
import io
import json


def test_cmip6_downscaled_point_fairbanks(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Full payload exceeds the 25MB fixture cap, so only the first 5 top-level
    # model keys are compared against a saved subset.
    assert isinstance(actual_data, dict) and actual_data
    subset_keys = list(actual_data.keys())[:5]
    actual_subset = {k: actual_data[k] for k in subset_keys}

    with open("tests/routes/cmip6_downscaled/point/json/cmip6_downscaled_point_fairbanks_subset.json") as f:
        expected_subset = json.load(f)

    assert actual_subset == expected_subset


def test_cmip6_downscaled_point_ocean(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at an ocean point; within the coverage's extent, so real data is expected."""
    response = client.get("/cmip6_downscaled/point/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Full payload exceeds the 25MB fixture cap, so only the first 5 top-level
    # model keys are compared against a saved subset.
    assert isinstance(actual_data, dict) and actual_data
    subset_keys = list(actual_data.keys())[:5]
    actual_subset = {k: actual_data[k] for k in subset_keys}

    with open("tests/routes/cmip6_downscaled/point/json/cmip6_downscaled_point_ocean_subset.json") as f:
        expected_subset = json.load(f)

    assert actual_subset == expected_subset


def test_cmip6_downscaled_point_attu(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at Attu, AK; outside the coverage's extent."""
    response = client.get("/cmip6_downscaled/point/52.8339/173.1794")
    assert response.status_code == 422


def test_cmip6_downscaled_point_dawson_city(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/cmip6_downscaled/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Full payload exceeds the 25MB fixture cap, so only the first 5 top-level
    # model keys are compared against a saved subset.
    assert isinstance(actual_data, dict) and actual_data
    subset_keys = list(actual_data.keys())[:5]
    actual_subset = {k: actual_data[k] for k in subset_keys}

    with open("tests/routes/cmip6_downscaled/point/json/cmip6_downscaled_point_dawson_city_subset.json") as f:
        expected_subset = json.load(f)

    assert actual_subset == expected_subset


def test_cmip6_downscaled_point_csv(client):
    """Tests /cmip6_downscaled/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/cmip6_downscaled/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
