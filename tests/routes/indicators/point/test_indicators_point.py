import csv
import io
import json


def test_indicators_cmip5_point(client):
    """
    Tests the /indicators/cmip5/point/<lat>/<lon>/ endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/indicators/cmip5/point/65.06/-146.16/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip5_point_65.06_-146.16.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip5_point_fairbanks(client):
    """Tests /indicators/cmip5/point/<lat>/<lon>/ at Fairbanks, AK."""
    response = client.get("/indicators/cmip5/point/64.8378/-147.7164/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip5_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip5_point_ocean(client):
    """Tests /indicators/cmip5/point/<lat>/<lon>/ at an ocean point; outside the NCAR 12km coverage, so nodata is expected."""
    response = client.get("/indicators/cmip5/point/66.95/-165/")
    assert response.status_code == 404


def test_indicators_cmip5_point_attu(client):
    """Tests /indicators/cmip5/point/<lat>/<lon>/ at Attu, AK; outside the NCAR 12km coverage, so nodata is expected."""
    response = client.get("/indicators/cmip5/point/52.8339/173.1794/")
    assert response.status_code == 404


def test_indicators_cmip5_point_dawson_city(client):
    """Tests /indicators/cmip5/point/<lat>/<lon>/ at Dawson City, Yukon."""
    response = client.get("/indicators/cmip5/point/64.0625/-139.431/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip5_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip6_point_fairbanks(client):
    """Tests /indicators/cmip6/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/indicators/cmip6/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip6_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip6_point_ocean(client):
    """Tests /indicators/cmip6/point/<lat>/<lon> at an ocean point; within this pan-Arctic coverage's extent, so real data is expected."""
    response = client.get("/indicators/cmip6/point/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip6_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip6_point_attu(client):
    """Tests /indicators/cmip6/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/indicators/cmip6/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip6_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip6_point_dawson_city(client):
    """Tests /indicators/cmip6/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/indicators/cmip6/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip6_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip6_point_reykjavik(client):
    """Tests /indicators/cmip6/point/<lat>/<lon> at Reykjavik, Iceland; this coverage is pan-Arctic in extent, so real data is expected."""
    response = client.get("/indicators/cmip6/point/64.1466/-21.9426")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/point/json/indicators_cmip6_point_reykjavik.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip6_point_summarize(client):
    """Tests /indicators/cmip6/point/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/indicators/cmip6/point/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_indicators_cmip6_point_csv(client):
    """Tests /indicators/cmip6/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/indicators/cmip6/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_indicators_cmip5_point_csv(client):
    """Tests /indicators/cmip5/point/<lat>/<lon>/?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/indicators/cmip5/point/64.8378/-147.7164/?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
