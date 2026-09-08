import csv
import io
import json


def test_demographics_local_ak15(client):
    """
    Tests the /demographics/<community> endpoint for a valid, well-known
    Alaska community ID (AK15, the example from the route's own docstring)
    to ensure the output remains consistent with production.
    """
    response = client.get("/demographics/AK15")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/demographics/local/json/demographics_local_ak15.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_demographics_local_invalid_community(client):
    """Tests /demographics/<community> with an invalid community ID; expects a 400 from validate_community_id's failure path."""
    response = client.get("/demographics/NOTREAL123")
    assert response.status_code == 400


def test_demographics_local_csv(client):
    """Tests /demographics/<community>?format=csv for AK15 returns a parseable CSV."""
    response = client.get("/demographics/AK15?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
