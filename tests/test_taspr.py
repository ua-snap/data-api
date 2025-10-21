import json


def test_temperature_area(client):
    """
    Tests the /temperature/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/temperature/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_area(client):
    """
    Tests the /precipitation/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/precipitation/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_area(client):
    """
    Tests the /taspr/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/taspr/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/taspr_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data