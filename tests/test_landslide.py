import json
import pytest
from unittest.mock import Mock

######################################
#  1. Database Unreachable Test Case #
######################################


def test_landslide_get_landslide_db_connection_failed(client, monkeypatch):
    """
    Tests the /landslide/<id> endpoint when get_landslide_db_row raises an exception,
    expecting a 502 error (upstream unreachable).
    """

    # Mock get_landslide_db_row to raise an exception
    def mock_get_landslide_db_row(place_name):
        raise Exception("Database connection failed")

    monkeypatch.setattr(
        "routes.landslide.get_landslide_db_row", mock_get_landslide_db_row
    )

    response = client.get("/landslide/AK91")
    assert response.status_code == 502


#################################################
# 2. Stale Data and Processing Error Test Cases #
#################################################


def test_landslide_package_data_stale_datetime(client, monkeypatch):
    """
    Tests the /landslide/<id> endpoint when the data has a stale expires_at datetime,
    expecting a 200 status code with error_code 409 in the JSON response.
    """

    def mock_get_landslide_db_row(place_name):
        return [
            {
                "ts": "2023-12-04T10:00:00Z",
                "expires_at": "2023-12-03T18:00:00Z",
                "hour": 10,
                "precip": 25.4,
                "precip_inches": 1.0,
                "precip24hr": 50.8,
                "precip2days": 76.2,
                "precip3days": 101.6,
                "risk_level": "moderate",
                "risk_prob": 0.6,
                "risk24hr": 0.5,
                "risk2days": 0.7,
                "risk3days": 0.8,
                "risk_is_elevated_from_previous": True,
            }
        ]

    def mock_get_place_data(community_id):
        return {
            "id": "AK91",
            "name": "Craig",
            "alt_name": "",
            "country": "United States",
            "is_coastal": True,
            "latitude": 55.4756,
            "longitude": -133.1481,
            "ocean_lat1": 55.5,
            "ocean_lon1": -133.2,
            "region": "Southeast",
            "tags": ["community"],
            "type": "community",
        }

    monkeypatch.setattr(
        "routes.landslide.get_landslide_db_row", mock_get_landslide_db_row
    )
    monkeypatch.setattr("routes.landslide.get_place_data", mock_get_place_data)

    response = client.get("/landslide/AK91")
    assert response.status_code == 409


def test_landslide_general_exception_in_processing(client, monkeypatch):
    """
    Tests the /landslide/<id> endpoint when a general exception occurs during processing,
    expecting a 500 error.
    """

    def mock_get_landslide_db_row(place_name):
        return [
            {
                "ts": "2023-12-04T10:00:00Z",
                "expires_at": "2023-12-04T18:00:00Z",
                "hour": 10,
                "precip": 25.4,
                "precip_inches": 1.0,
                "precip24hr": 50.8,
                "precip2days": 76.2,
                "precip3days": 101.6,
                "risk_level": "moderate",
                "risk_prob": 0.6,
                "risk24hr": 0.5,
                "risk2days": 0.7,
                "risk3days": 0.8,
                "risk_is_elevated_from_previous": True,
            }
        ]

    def mock_get_place_data(community_id):
        return {
            "id": "AK91",
            "name": "Craig",
            "alt_name": "",
            "country": "United States",
            "is_coastal": True,
            "latitude": 55.4756,
            "longitude": -133.1481,
            "ocean_lat1": 55.5,
            "ocean_lon1": -133.2,
            "region": "Southeast",
            "tags": ["community"],
            "type": "community",
        }

    def mock_package_landslide_data(place_id):
        exc = Exception("Unexpected error during place data processing")
        raise exc

    monkeypatch.setattr(
        "routes.landslide.get_landslide_db_row", mock_get_landslide_db_row
    )
    monkeypatch.setattr("routes.landslide.get_place_data", mock_get_place_data)
    monkeypatch.setattr(
        "routes.landslide.package_landslide_data", mock_package_landslide_data
    )

    response = client.get("/landslide/AK91")
    assert response.status_code == 500


def test_landslide_bad_datetime_parsing_error(client, monkeypatch):
    """
    Tests the /landslide/<id> endpoint when the expires_at datetime is malformed
    and causes a parsing error, expecting a 500 error.
    """

    def mock_get_landslide_db_row(place_name):
        return [
            {
                "ts": "2023-12-04T10:00:00Z",
                "expires_at": "12-04-2023T18:00:00Z",
                "hour": 10,
                "precip": 25.4,
                "precip_inches": 1.0,
                "precip24hr": 50.8,
                "precip2days": 76.2,
                "precip3days": 101.6,
                "risk_level": "moderate",
                "risk_prob": 0.6,
                "risk24hr": 0.5,
                "risk2days": 0.7,
                "risk3days": 0.8,
                "risk_is_elevated_from_previous": True,
            }
        ]

    def mock_get_place_data(community_id):
        return {
            "id": "AK91",
            "name": "Craig",
            "alt_name": "",
            "country": "United States",
            "is_coastal": True,
            "latitude": 55.4756,
            "longitude": -133.1481,
            "ocean_lat1": 55.5,
            "ocean_lon1": -133.2,
            "region": "Southeast",
            "tags": ["community"],
            "type": "community",
        }

    monkeypatch.setattr(
        "routes.landslide.get_landslide_db_row", mock_get_landslide_db_row
    )

    monkeypatch.setattr("routes.landslide.get_place_data", mock_get_place_data)

    response = client.get("/landslide/AK91")
    assert response.status_code == 500


###################################
# 3. Valid Community Test Cases   #
###################################


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


#################################################
# 4. Invalid / Unsupported Community Test Cases #
#################################################


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
