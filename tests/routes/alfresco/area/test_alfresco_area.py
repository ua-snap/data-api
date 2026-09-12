import csv
import io
import json

from tests.json_compare import assert_json_allclose


def test_alfresco_flammability_area(client):
    """
    Tests the /alfresco/flammability/area/<id> endpoint to ensure the output
    remains consistent after refactoring.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/flammability/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Load the expected response from the JSON file
    with open("tests/routes/alfresco/area/json/alfresco_flammability_area_1908031103.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert_json_allclose(actual_data, expected_data)


def test_alfresco_veg_type_area(client):
    """
    Tests the /alfresco/veg_type/area/<id> endpoint to ensure the output
    remains consistent after refactoring.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/veg_type/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Load the expected response from the JSON file
    with open("tests/routes/alfresco/area/json/alfresco_veg_type_area_1908031103.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert_json_allclose(actual_data, expected_data)


def test_alfresco_flammability_area_ytpa21(client):
    """Tests the /alfresco/flammability/area/<id> endpoint for the YTPA21 (Yukon) polygon."""
    response = client.get("/alfresco/flammability/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/alfresco/area/json/alfresco_flammability_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


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

    assert_json_allclose(actual_data, expected_data)


def test_alfresco_veg_type_area_invalid_huc10(client):
    """Tests the /alfresco/veg_type/area/<id> endpoint for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/alfresco/veg_type/area/1903010300")
    assert response.status_code == 422


def test_alfresco_area_csv(client):
    """Tests /alfresco/flammability/area/<id>?format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/alfresco/flammability/area/1908031103?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
