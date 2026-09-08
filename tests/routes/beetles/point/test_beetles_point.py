import csv
import io
import json


def test_beetles_point(client):
    """
    Tests the /beetles/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/beetles/point/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/beetles/point/json/beetles_point_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_beetles_point_fairbanks(client):
    """Tests /beetles/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/beetles/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/beetles/point/json/beetles_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_beetles_point_ocean(client):
    """Tests /beetles/point/<lat>/<lon> at an ocean point; nodata is expected."""
    response = client.get("/beetles/point/66.95/-165")
    assert response.status_code == 404


def test_beetles_point_attu(client):
    """Tests /beetles/point/<lat>/<lon> at Attu, AK; nodata is expected."""
    response = client.get("/beetles/point/52.8339/173.1794")
    assert response.status_code == 404


def test_beetles_point_dawson_city(client):
    """Tests /beetles/point/<lat>/<lon> at Dawson City, Yukon; nodata is expected."""
    response = client.get("/beetles/point/64.0625/-139.431")
    assert response.status_code == 404


def test_beetles_point_csv(client):
    """Tests /beetles/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/beetles/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
