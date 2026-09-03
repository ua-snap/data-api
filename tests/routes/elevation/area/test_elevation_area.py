import json


def test_elevation_area(client):
    """
    Tests the /elevation/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/elevation/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/area/json/elevation_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_area_19080309(client):
    """Tests /elevation/area/<id> for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/elevation/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/area/json/elevation_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_area_ytpa21(client):
    """Tests /elevation/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/elevation/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/area/json/elevation_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_elevation_area_huc10(client):
    """Tests /elevation/area/<id> for a HUC10-level area ID."""
    response = client.get("/elevation/area/1903010300")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/elevation/area/json/elevation_area_1903010300.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
