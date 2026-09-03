import json


def test_elevation_point(client):
    """
    Tests the /elevation/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/elevation/point/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/point/json/elevation_point_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_point_fairbanks(client):
    """Tests /elevation/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/elevation/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/point/json/elevation_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_point_ocean(client):
    """Tests /elevation/point/<lat>/<lon> at an ocean point."""
    response = client.get("/elevation/point/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/point/json/elevation_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_point_attu(client):
    """Tests /elevation/point/<lat>/<lon> at Attu, AK; outside the DEM coverage, so nodata is expected."""
    response = client.get("/elevation/point/52.8339/173.1794")
    assert response.status_code == 404


def test_elevation_point_dawson_city(client):
    """Tests /elevation/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/elevation/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/point/json/elevation_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
