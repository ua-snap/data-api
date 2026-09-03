import csv
import io
import json


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

    assert actual_data == expected_data


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

    assert actual_data == expected_data


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

    assert actual_data == expected_data


def test_temperature_area_19080309(client):
    """Tests /temperature/area/<id> for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/temperature/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/temperature_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_area_ytpa21(client):
    """Tests /temperature/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/temperature/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/temperature_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_area_invalid_huc10(client):
    """Tests /temperature/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/temperature/area/1903010300")
    assert response.status_code == 422


def test_precipitation_area_19080309(client):
    """Tests /precipitation/area/<id> for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/precipitation/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/precipitation_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_area_ytpa21(client):
    """Tests /precipitation/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/precipitation/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/precipitation_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_area_invalid_huc10(client):
    """Tests /precipitation/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/precipitation/area/1903010300")
    assert response.status_code == 422


def test_taspr_area_19080309(client):
    """Tests /taspr/area/<id> for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/taspr/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/taspr_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_area_ytpa21(client):
    """Tests /taspr/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/taspr/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/area/json/taspr_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_area_invalid_huc10(client):
    """Tests /taspr/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/taspr/area/1903010300")
    assert response.status_code == 422


def test_temperature_area_csv(client):
    """Tests /temperature/area/<id>?format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/temperature/area/19080309?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_precipitation_area_csv(client):
    """Tests /precipitation/area/<id>?format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/precipitation/area/19080309?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_taspr_area_csv(client):
    """Tests /taspr/area/<id>?format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/taspr/area/19080309?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
