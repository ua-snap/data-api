import csv
import io
import json

from tests.json_compare import assert_json_allclose


def test_era5wrf_area_1908031103(client):
    """Tests /era5wrf/area/<id> for the 1908031103 (Rock Creek) HUC10 polygon."""
    response = client.get("/era5wrf/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/area/json/era5wrf_area_1908031103.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_era5wrf_area_ytpa21(client):
    """Tests /era5wrf/area/<id> for the YTPA21 (Yukon) polygon."""
    response = client.get("/era5wrf/area/YTPA21")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/area/json/era5wrf_area_YTPA21.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_era5wrf_area_invalid_huc10(client):
    """Tests /era5wrf/area/<id> for a HUC10-level area ID; not a valid polygon type for this route."""
    response = client.get("/era5wrf/area/1903010300")
    assert response.status_code == 404


def test_era5wrf_area_vars(client):
    """Tests /era5wrf/area/<id>?vars= for the 1908031103 HUC10 polygon returns a parseable response."""
    response = client.get("/era5wrf/area/1908031103?vars=t2_mean")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_era5wrf_area_csv(client):
    """Tests /era5wrf/area/<id>?format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/era5wrf/area/1908031103?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
