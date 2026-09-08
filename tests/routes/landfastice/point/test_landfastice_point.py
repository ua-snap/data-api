import csv
import io
import json


def test_landfastice_point_fairbanks(client):
    """Tests /landfastice/point/<lat>/<lon>/ at Fairbanks, AK; outside the coverage (ocean-only), so out-of-bounds is expected."""
    response = client.get("/landfastice/point/64.8378/-147.7164/")
    assert response.status_code == 422


def test_landfastice_point_ocean(client):
    """Tests /landfastice/point/<lat>/<lon>/ at an ocean point in the Bering Strait."""
    response = client.get("/landfastice/point/66.95/-165/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/landfastice/point/json/landfastice_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_landfastice_point_attu(client):
    """Tests /landfastice/point/<lat>/<lon>/ at Attu, AK; outside the coverage (ocean-only), so out-of-bounds is expected."""
    response = client.get("/landfastice/point/52.8339/173.1794/")
    assert response.status_code == 422


def test_landfastice_point_dawson_city(client):
    """Tests /landfastice/point/<lat>/<lon>/ at Dawson City, Yukon; outside the coverage (ocean-only), so out-of-bounds is expected."""
    response = client.get("/landfastice/point/64.0625/-139.431/")
    assert response.status_code == 422


def test_landfastice_point_csv(client):
    """Tests /landfastice/point/<lat>/<lon>/?format=csv at an ocean point (the only in-bounds location for this route) returns a parseable CSV."""
    response = client.get("/landfastice/point/66.95/-165/?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
