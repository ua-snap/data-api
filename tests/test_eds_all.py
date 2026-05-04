import json


def test_eds_all(client):
    """
    Tests the /eds/all/<lat>/<lon>/ endpoint to ensure the output
    remains consistent with production for the given latitude and longitude.
    """
    response = client.get("/eds/all/62.27/-154.61")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/eds_point.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
