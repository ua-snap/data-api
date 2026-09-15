import csv
import io
import json

from tests.json_compare import assert_json_allclose


def test_temperature_area(client):
    """
    Tests the /temperature/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/temperature/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/temperature_area_19010208.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_precipitation_area(client):
    """
    Tests the /precipitation/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/precipitation/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/precipitation_area_19010208.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_taspr_area(client):
    """
    Tests the /taspr/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/taspr/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/taspr_area_19010208.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_temperature_area_1908031103(client):
    """Tests /temperature/area/<id> for the 1908031103 (Rock Creek) HUC10 polygon."""
    response = client.get("/temperature/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/temperature_area_1908031103.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_temperature_area_ytpa21(client):
    """Tests /temperature/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/temperature/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/temperature_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_temperature_area_invalid_huc10(client):
    """Tests /temperature/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/temperature/area/1903010300")
    assert response.status_code == 422


def test_precipitation_area_1908031103(client):
    """Tests /precipitation/area/<id> for the 1908031103 (Rock Creek) HUC10 polygon."""
    response = client.get("/precipitation/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/precipitation_area_1908031103.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_precipitation_area_ytpa21(client):
    """Tests /precipitation/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/precipitation/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/precipitation_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_precipitation_area_invalid_huc10(client):
    """Tests /precipitation/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/precipitation/area/1903010300")
    assert response.status_code == 422


def test_taspr_area_1908031103(client):
    """Tests /taspr/area/<id> for the 1908031103 (Rock Creek) HUC10 polygon."""
    response = client.get("/taspr/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/taspr_area_1908031103.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_taspr_area_ytpa21(client):
    """Tests /taspr/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/taspr/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/taspr_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_taspr_area_invalid_huc10(client):
    """Tests /taspr/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/taspr/area/1903010300")
    assert response.status_code == 422


def test_temperature_area_csv(client):
    """Tests /temperature/area/<id>?format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/temperature/area/1908031103?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_precipitation_area_csv(client):
    """Tests /precipitation/area/<id>?format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/precipitation/area/1908031103?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_taspr_area_csv(client):
    """Tests /taspr/area/<id>?format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/taspr/area/1908031103?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
