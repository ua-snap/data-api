import json


def test_indicators_cmip5_area(client):
    """
    Tests the /indicators/cmip5/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/indicators/cmip5/area/1903040601")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/indicators_cmip5_area_1903040601.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
