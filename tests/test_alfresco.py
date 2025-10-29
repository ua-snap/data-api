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
    with open("tests/alfresco_flammability_area_19080309.json") as f:
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
    with open("tests/alfresco_veg_type_area_19080309.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert actual_data == expected_data


def test_alfresco_flammability_local(client):
    """
    Tests the /alfresco/flammability/local/<lat>/<lon> endpoint to ensure the output
    remains consistent after refactoring.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/flammability/local/65.4844/-145.4036")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Load the expected response from the JSON file
    with open("tests/alfresco_flammability_local_65.4844_-145.4036.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert actual_data == expected_data


def test_alfresco_veg_type_local(client):
    """
    Tests the /alfresco/veg_type/local/<lat>/<lon> endpoint to ensure the output
    remains consistent after refactoring.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/veg_type/local/65.4844/-145.4036")
    assert response.status_code == 200
    actual_data = response.get_json()

    # Load the expected response from the JSON file
    with open("tests/alfresco_veg_type_local_65.4844_-145.4036.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert actual_data == expected_data


def test_alfresco_flammability_invalid_area(client):
    """
    Tests the /alfresco/flammability/area/<id> endpoint to ensure that a 422 error is returned for an invalid area.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/flammability/area/foobar")
    assert response.status_code == 422


def test_alfresco_veg_type_invalid_area(client):
    """
    Tests the /alfresco/veg_type/area/<id> endpoint to ensure that a 422 error is returned for an invalid area.
    """
    # Get the actual response from the endpoint
    response = client.get("/alfresco/veg_type/area/foobar")
    assert response.status_code == 422


def test_alfresco_flammability_invalid_local_str(client):
    """Tests the /alfresco/flammability/local/<lat>/<lon> endpoint to ensure that a 400 error is returned for a string input."""
    # Get the actual response from the endpoint
    response = client.get("/alfresco/flammability/local/foobar")
    assert response.status_code == 400


def test_alfresco_veg_type_invalid_local_str(client):
    """Tests the /alfresco/veg_type/local/<lat>/<lon> endpoint to ensure that a 400 error is returned for a string input."""
    # Get the actual response from the endpoint
    response = client.get("/alfresco/veg_type/local/foobar")
    assert response.status_code == 400
