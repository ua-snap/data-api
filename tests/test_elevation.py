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
