import csv
import io
import json


def test_indicators_cmip5_area(client):
    """
    Tests the /indicators/cmip5/area/<id>/ endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/indicators/cmip5/area/1903040601/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/area/json/indicators_cmip5_area_1903040601.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip5_area_19080309(client):
    """Tests /indicators/cmip5/area/<id>/ for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/indicators/cmip5/area/19080309/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/area/json/indicators_cmip5_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip5_area_ytpa21(client):
    """Tests /indicators/cmip5/area/<id>/ for the YTPA21 (Yukon) polygon."""
    response = client.get("/indicators/cmip5/area/YTPA21/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/indicators/area/json/indicators_cmip5_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_indicators_cmip5_area_invalid_huc10(client):
    """Tests /indicators/cmip5/area/<id>/ for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/indicators/cmip5/area/1903010300/")
    assert response.status_code == 422


def test_indicators_cmip5_area_csv(client):
    """Tests /indicators/cmip5/area/<id>/?format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/indicators/cmip5/area/19080309/?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
