import json


def test_landslide_ak91(client):
    """
    Tests the /landslide/AK91 endpoint to ensure the output
    contains all required keys for Craig, AK.
    """
    response = client.get("/landslide/AK91")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Required top-level keys
    required_keys = [
        "community",
        "expires_at",
        "hour",
        "precipitation_24hr",
        "precipitation_2days",
        "precipitation_3days",
        "precipitation_inches",
        "precipitation_mm",
        "risk_24hr",
        "risk_2days",
        "risk_3days",
        "risk_is_elevated_from_previous",
        "risk_level",
        "risk_probability",
        "timestamp",
    ]

    # Check that all required keys exist
    for key in required_keys:
        assert key in actual_data, f"Missing required key: {key}"

    # Required community keys
    required_community_keys = [
        "alt_name",
        "country",
        "id",
        "is_coastal",
        "latitude",
        "longitude",
        "name",
        "ocean_lat1",
        "ocean_lon1",
        "region",
        "tags",
        "type",
    ]

    # Check that community data exists and has required keys
    assert "community" in actual_data
    community_data = actual_data["community"]
    for key in required_community_keys:
        assert key in community_data, f"Missing required community key: {key}"

    # Verify community ID is correct for this endpoint
    assert community_data["id"] == "AK91"
    assert community_data["name"] == "Craig"


def test_landslide_ak182(client):
    """
    Tests the /landslide/AK182 endpoint to ensure the output
    contains all required keys for Kasaan, AK.
    """
    response = client.get("/landslide/AK182")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Required top-level keys
    required_keys = [
        "community",
        "expires_at",
        "hour",
        "precipitation_24hr",
        "precipitation_2days",
        "precipitation_3days",
        "precipitation_inches",
        "precipitation_mm",
        "risk_24hr",
        "risk_2days",
        "risk_3days",
        "risk_is_elevated_from_previous",
        "risk_level",
        "risk_probability",
        "timestamp",
    ]

    # Check that all required keys exist
    for key in required_keys:
        assert key in actual_data, f"Missing required key: {key}"

    # Required community keys
    required_community_keys = [
        "alt_name",
        "country",
        "id",
        "is_coastal",
        "latitude",
        "longitude",
        "name",
        "ocean_lat1",
        "ocean_lon1",
        "region",
        "tags",
        "type",
    ]

    # Check that community data exists and has required keys
    assert "community" in actual_data
    community_data = actual_data["community"]
    for key in required_community_keys:
        assert key in community_data, f"Missing required community key: {key}"

    # Verify community ID is correct for this endpoint
    assert community_data["id"] == "AK182"
    assert community_data["name"] == "Kasaan"


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
