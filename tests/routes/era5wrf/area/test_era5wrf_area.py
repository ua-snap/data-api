import csv
import io
import json


def test_era5wrf_area_19080309(client):
    """Tests /era5wrf/area/<id> for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/era5wrf/area/19080309")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/area/json/era5wrf_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_era5wrf_area_ytpa21(client):
    """Tests /era5wrf/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/era5wrf/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/area/json/era5wrf_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_era5wrf_area_invalid_huc10(client):
    """Tests /era5wrf/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/era5wrf/area/1903010300")
    assert response.status_code == 404


def test_era5wrf_area_csv(client):
    """Tests /era5wrf/area/<id>?format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/era5wrf/area/19080309?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
