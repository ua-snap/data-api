import json


def test_beetles_area(client):
    """
    Tests the /beetles/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/beetles/area/19020302")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/beetles_area_19020302.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data