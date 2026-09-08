import csv
import io
import json


def test_hydrology_point_fairbanks(client):
    """Tests /hydrology/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/hydrology/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/hydrology/point/json/hydrology_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_hydrology_point_ocean(client):
    """Tests /hydrology/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/hydrology/point/66.95/-165")
    assert response.status_code == 404


def test_hydrology_point_attu(client):
    """Tests /hydrology/point/<lat>/<lon> at Attu, AK; outside the coverage, so nodata is expected."""
    response = client.get("/hydrology/point/52.8339/173.1794")
    assert response.status_code == 404


def test_hydrology_point_dawson_city(client):
    """Tests /hydrology/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/hydrology/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open(
        "tests/routes/hydrology/point/json/hydrology_point_dawson_city.json"
    ) as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_hydrology_point_summarize(client):
    """Tests /hydrology/point/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/hydrology/point/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_hydrology_point_csv(client):
    """Tests /hydrology/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/hydrology/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
