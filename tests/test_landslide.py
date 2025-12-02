import json


def test_landslide_ak91(client):
    """
    Tests the /landslide/AK91 endpoint to ensure the output
    remains consistent with production for Craig, AK.
    """
    response = client.get("/landslide/AK91")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/landslide_risk_AK91.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_landslide_ak182(client):
    """
    Tests the /landslide/AK182 endpoint to ensure the output
    remains consistent with production for Kasaan, AK.
    """
    response = client.get("/landslide/AK182")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/landslide_risk_AK182.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_landslide_invalid_community(client):
    """
    Tests the /landslide/<id> endpoint with an invalid community ID
    to ensure proper error handling.
    """
    response = client.get("/landslide/INVALID")
    assert response.status_code == 400


def test_landslide_valid_but_unsupported_community(client):
    """
    Tests the /landslide/<id> endpoint with a valid community ID
    that is not supported for landslide data.
    """
    response = client.get(
        "/landslide/AK124"
    )  # Fairbanks - valid place but not supported for landslides
    assert response.status_code == 400
