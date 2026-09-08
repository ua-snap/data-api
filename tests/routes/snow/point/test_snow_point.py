import csv
import io
import json


def test_snow_point_fairbanks(client):
    """Tests /snow/snowfallequivalent/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/snow/snowfallequivalent/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/snow/point/json/snow_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_snow_point_ocean(client):
    """Tests /snow/snowfallequivalent/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/snow/snowfallequivalent/66.95/-165")
    assert response.status_code == 404


def test_snow_point_attu(client):
    """Tests /snow/snowfallequivalent/<lat>/<lon> at Attu, AK."""
    response = client.get("/snow/snowfallequivalent/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/snow/point/json/snow_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_snow_point_dawson_city(client):
    """Tests /snow/snowfallequivalent/<lat>/<lon> at Dawson City, Yukon; outside the coverage, so nodata is expected."""
    response = client.get("/snow/snowfallequivalent/64.0625/-139.431")
    assert response.status_code == 404


def test_snow_point_summarize(client):
    """Tests /snow/snowfallequivalent/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/snow/snowfallequivalent/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_snow_point_csv(client):
    """Tests /snow/snowfallequivalent/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/snow/snowfallequivalent/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
