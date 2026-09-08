import csv
import io
import json


def test_alfresco_flammability_area(client):
    """
    Tests the /alfresco/flammability/area/<id> endpoint to ensure the output
    remains consistent after refactoring.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/flammability/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Load the expected response from the JSON file
    with open("tests/routes/alfresco/area/json/alfresco_flammability_area_19080309.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert actual_data == expected_data


def test_alfresco_veg_type_area(client):
    """
    Tests the /alfresco/veg_type/area/<id> endpoint to ensure the output
    remains consistent after refactoring.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/veg_type/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Load the expected response from the JSON file
    with open("tests/routes/alfresco/area/json/alfresco_veg_type_area_19080309.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert actual_data == expected_data


def test_alfresco_flammability_area_ytpa21(client):
    """Tests the /alfresco/flammability/area/<id> endpoint for the YTPA21 (Yukon) polygon."""
    response = client.get("/alfresco/flammability/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/alfresco/area/json/alfresco_flammability_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_alfresco_flammability_area_invalid_huc10(client):
    """Tests the /alfresco/flammability/area/<id> endpoint for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/alfresco/flammability/area/1903010300")
    assert response.status_code == 422


def test_alfresco_veg_type_area_ytpa21(client):
    """Tests the /alfresco/veg_type/area/<id> endpoint for the YTPA21 (Yukon) polygon."""
    response = client.get("/alfresco/veg_type/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/alfresco/area/json/alfresco_veg_type_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_alfresco_veg_type_area_invalid_huc10(client):
    """Tests the /alfresco/veg_type/area/<id> endpoint for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/alfresco/veg_type/area/1903010300")
    assert response.status_code == 422


def test_alfresco_area_csv(client):
    """Tests /alfresco/flammability/area/<id>?format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/alfresco/flammability/area/19080309?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
