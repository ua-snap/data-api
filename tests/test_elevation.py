import json


def test_elevation_area(client):
    """
    Tests the /elevation/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/elevation/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/elevation_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_area_era5_4km(client):
    """
    Tests the /elevation/area/era5_4km_GMU23 endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/elevation/area/era5_4km/GMU23")
    assert response.status_code == 200
    actual_data = response.get_json()
    with open("tests/elevation_area_era5_4km_GMU23.json") as f:
        expected_data = json.load(f)
    assert actual_data == expected_data


def test_elevation_point_era5_4km(client):
    """
    Tests the /elevation/point/era5_4km endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/elevation/point/era5_4km/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()
    with open("tests/elevation_point_era5_4km_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)
    assert actual_data == expected_data