import csv
import io
import json

from tests.json_compare import assert_json_allclose


def test_beetles_area(client):
    """
    Tests the /beetles/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/beetles/area/19020302")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/beetles/area/json/beetles_area_19020302.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_beetles_area_1908031103(client):
    """Tests /beetles/area/<id> for the 1908031103 (Rock Creek) HUC10 polygon."""
    response = client.get("/beetles/area/1908031103")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/beetles/area/json/beetles_area_1908031103.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_beetles_area_ytpa21(client):
    """Tests /beetles/area/<id> for the YTPA21 (Yukon) polygon; outside beetle risk coverage, so nodata is expected."""
    response = client.get("/beetles/area/YTPA21")
    assert response.status_code == 404


def test_beetles_area_invalid_huc10(client):
    """Tests /beetles/area/<id> for a HUC10-level area ID; outside beetle risk coverage, so nodata is expected."""
    response = client.get("/beetles/area/1903010300")
    assert response.status_code == 404


def test_beetles_area_csv(client):
    """Tests /beetles/area/<id>?format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/beetles/area/1908031103?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
