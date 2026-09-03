import csv
import io
import json


def test_cmip6_point_fairbanks(client):
    """Tests /cmip6/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/cmip6/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/cmip6/point/json/cmip6_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_cmip6_point_ocean(client):
    """Tests /cmip6/point/<lat>/<lon> at an ocean point; within the coverage's global extent, so real data is expected."""
    response = client.get("/cmip6/point/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/cmip6/point/json/cmip6_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_cmip6_point_attu(client):
    """Tests /cmip6/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/cmip6/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/cmip6/point/json/cmip6_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_cmip6_point_dawson_city(client):
    """Tests /cmip6/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/cmip6/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/cmip6/point/json/cmip6_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_cmip6_point_reykjavik(client):
    """Tests /cmip6/point/<lat>/<lon> at Reykjavik, Iceland; this coverage is pan-Arctic in extent, so real data is expected."""
    response = client.get("/cmip6/point/64.1466/-21.9426")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/cmip6/point/json/cmip6_point_reykjavik.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_cmip6_point_csv(client):
    """Tests /cmip6/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/cmip6/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
