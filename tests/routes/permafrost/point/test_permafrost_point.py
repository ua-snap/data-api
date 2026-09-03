import csv
import io
import json


def test_permafrost_point_gipl_fairbanks(client):
    """Tests /permafrost/point/gipl/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/permafrost/point/gipl/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/permafrost/point/json/permafrost_point_gipl_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_permafrost_point_gipl_ocean(client):
    """Tests /permafrost/point/gipl/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/permafrost/point/gipl/66.95/-165")
    assert response.status_code == 404


def test_permafrost_point_gipl_attu(client):
    """Tests /permafrost/point/gipl/<lat>/<lon> at Attu, AK; outside the GIPL coverage extent."""
    response = client.get("/permafrost/point/gipl/52.8339/173.1794")
    assert response.status_code == 404


def test_permafrost_point_gipl_dawson_city(client):
    """Tests /permafrost/point/gipl/<lat>/<lon> at Dawson City, Yukon; outside the GIPL coverage extent."""
    response = client.get("/permafrost/point/gipl/64.0625/-139.431")
    assert response.status_code == 404


def test_permafrost_point_all_fairbanks(client):
    """Tests /permafrost/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/permafrost/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/permafrost/point/json/permafrost_point_all_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_permafrost_point_all_ocean(client):
    """Tests /permafrost/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/permafrost/point/66.95/-165")
    assert response.status_code == 404


def test_permafrost_point_all_attu(client):
    """Tests /permafrost/point/<lat>/<lon> at Attu, AK; outside coverage extent."""
    response = client.get("/permafrost/point/52.8339/173.1794")
    assert response.status_code == 404


def test_permafrost_point_all_dawson_city(client):
    """Tests /permafrost/point/<lat>/<lon> at Dawson City, Yukon; outside coverage extent."""
    response = client.get("/permafrost/point/64.0625/-139.431")
    assert response.status_code == 404


def test_permafrost_point_gipl_csv(client):
    """Tests /permafrost/point/gipl/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/permafrost/point/gipl/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_permafrost_point_all_csv(client):
    """Tests /permafrost/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/permafrost/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
