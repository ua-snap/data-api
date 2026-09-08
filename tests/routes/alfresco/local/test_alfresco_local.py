import csv
import io
import json


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
    with open("tests/routes/alfresco/local/json/alfresco_flammability_local_65.4844_-145.4036.json") as f:
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
    with open("tests/routes/alfresco/local/json/alfresco_veg_type_local_65.4844_-145.4036.json") as f:
        expected_data = json.load(f)

    # Compare the actual data against the expected data
    assert actual_data == expected_data


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


def test_alfresco_flammability_local_fairbanks(client):
    """Tests the /alfresco/flammability/local/<lat>/<lon> endpoint at Fairbanks, AK."""
    response = client.get("/alfresco/flammability/local/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/alfresco/local/json/alfresco_flammability_local_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_alfresco_flammability_local_ocean(client):
    """Tests the /alfresco/flammability/local/<lat>/<lon> endpoint at an ocean point; no HUC12 intersects, so nodata is expected."""
    response = client.get("/alfresco/flammability/local/66.95/-165")
    assert response.status_code == 404


def test_alfresco_flammability_local_attu(client):
    """Tests the /alfresco/flammability/local/<lat>/<lon> endpoint at Attu, AK; outside the HUC12 coverage."""
    response = client.get("/alfresco/flammability/local/52.8339/173.1794")
    assert response.status_code == 404


def test_alfresco_flammability_local_dawson_city(client):
    """Tests the /alfresco/flammability/local/<lat>/<lon> endpoint at Dawson City, Yukon; outside the HUC12 coverage."""
    response = client.get("/alfresco/flammability/local/64.0625/-139.431")
    assert response.status_code == 404


def test_alfresco_veg_type_local_fairbanks(client):
    """Tests the /alfresco/veg_type/local/<lat>/<lon> endpoint at Fairbanks, AK."""
    response = client.get("/alfresco/veg_type/local/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/alfresco/local/json/alfresco_veg_type_local_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_alfresco_veg_type_local_ocean(client):
    """Tests the /alfresco/veg_type/local/<lat>/<lon> endpoint at an ocean point; no HUC12 intersects, so nodata is expected."""
    response = client.get("/alfresco/veg_type/local/66.95/-165")
    assert response.status_code == 404


def test_alfresco_veg_type_local_attu(client):
    """Tests the /alfresco/veg_type/local/<lat>/<lon> endpoint at Attu, AK; outside the HUC12 coverage."""
    response = client.get("/alfresco/veg_type/local/52.8339/173.1794")
    assert response.status_code == 404


def test_alfresco_veg_type_local_dawson_city(client):
    """Tests the /alfresco/veg_type/local/<lat>/<lon> endpoint at Dawson City, Yukon; outside the HUC12 coverage."""
    response = client.get("/alfresco/veg_type/local/64.0625/-139.431")
    assert response.status_code == 404


def test_alfresco_local_csv(client):
    """Tests /alfresco/flammability/local/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/alfresco/flammability/local/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
