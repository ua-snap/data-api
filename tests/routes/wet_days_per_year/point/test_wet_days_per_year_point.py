import csv
import io
import json


def test_wet_days_per_year_historical_fairbanks(client):
    """Tests /wet_days_per_year/historical/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/wet_days_per_year/historical/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/wet_days_per_year/point/json/wet_days_per_year_historical_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_wet_days_per_year_historical_ocean(client):
    """Tests /wet_days_per_year/historical/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/wet_days_per_year/historical/point/66.95/-165")
    assert response.status_code == 404


def test_wet_days_per_year_historical_attu(client):
    """Tests /wet_days_per_year/historical/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/wet_days_per_year/historical/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/wet_days_per_year/point/json/wet_days_per_year_historical_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_wet_days_per_year_historical_dawson_city(client):
    """Tests /wet_days_per_year/historical/point/<lat>/<lon> at Dawson City, Yukon; outside the coverage, so nodata is expected."""
    response = client.get("/wet_days_per_year/historical/point/64.0625/-139.431")
    assert response.status_code == 404


def test_wet_days_per_year_projected_fairbanks(client):
    """Tests /wet_days_per_year/projected/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/wet_days_per_year/projected/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/wet_days_per_year/point/json/wet_days_per_year_projected_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_wet_days_per_year_projected_ocean(client):
    """Tests /wet_days_per_year/projected/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/wet_days_per_year/projected/point/66.95/-165")
    assert response.status_code == 404


def test_wet_days_per_year_projected_attu(client):
    """Tests /wet_days_per_year/projected/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/wet_days_per_year/projected/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/wet_days_per_year/point/json/wet_days_per_year_projected_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_wet_days_per_year_projected_dawson_city(client):
    """Tests /wet_days_per_year/projected/point/<lat>/<lon> at Dawson City, Yukon; outside the coverage, so nodata is expected."""
    response = client.get("/wet_days_per_year/projected/point/64.0625/-139.431")
    assert response.status_code == 404


def test_wet_days_per_year_hp_fairbanks(client):
    """Tests /wet_days_per_year/hp/point/<lat>/<lon> at Fairbanks, AK (combined historical+projected variant)."""
    response = client.get("/wet_days_per_year/hp/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/wet_days_per_year/point/json/wet_days_per_year_hp_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_wet_days_per_year_all_fairbanks(client):
    """Tests /wet_days_per_year/all/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/wet_days_per_year/all/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/wet_days_per_year/point/json/wet_days_per_year_all_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_wet_days_per_year_historical_csv(client):
    """Tests /wet_days_per_year/historical/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/wet_days_per_year/historical/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_wet_days_per_year_projected_csv(client):
    """Tests /wet_days_per_year/projected/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/wet_days_per_year/projected/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_wet_days_per_year_hp_csv(client):
    """Tests /wet_days_per_year/hp/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/wet_days_per_year/hp/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_wet_days_per_year_all_csv(client):
    """Tests /wet_days_per_year/all/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/wet_days_per_year/all/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
