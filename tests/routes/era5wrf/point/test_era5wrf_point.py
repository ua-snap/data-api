import csv
import io
import json


def test_era5wrf_point_fairbanks(client):
    """Tests /era5wrf/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/era5wrf/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/point/json/era5wrf_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_era5wrf_point_ocean(client):
    """Tests /era5wrf/point/<lat>/<lon> at an ocean point; within the coverage's bbox, so real data is expected."""
    response = client.get("/era5wrf/point/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/point/json/era5wrf_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_era5wrf_point_attu(client):
    """Tests /era5wrf/point/<lat>/<lon> at Attu, AK; outside the coverage's bbox."""
    response = client.get("/era5wrf/point/52.8339/173.1794")
    assert response.status_code == 422


def test_era5wrf_point_vars(client):
    """Tests /era5wrf/point/<lat>/<lon>?vars= at Fairbanks, AK returns a parseable response."""
    response = client.get("/era5wrf/point/64.8378/-147.7164?vars=t2_mean")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_era5wrf_point_csv(client):
    """Tests /era5wrf/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/era5wrf/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_era5wrf_point_dawson_city(client):
    """Tests /era5wrf/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/era5wrf/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/era5wrf/point/json/era5wrf_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
