import csv
import io
import json


def test_fire_weather_point_fairbanks(client):
    """Tests /fire_weather/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/fire_weather/point/64.8378/-147.7164?op=3_day_rolling_average")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/fire_weather/point/json/fire_weather_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_fire_weather_point_ocean(client):
    """Tests /fire_weather/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/fire_weather/point/66.95/-165?op=3_day_rolling_average")
    assert response.status_code == 404


def test_fire_weather_point_attu(client):
    """Tests /fire_weather/point/<lat>/<lon> at Attu, AK; outside the coverage, so nodata is expected."""
    response = client.get("/fire_weather/point/52.8339/173.1794?op=3_day_rolling_average")
    assert response.status_code == 404


def test_fire_weather_point_dawson_city(client):
    """Tests /fire_weather/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/fire_weather/point/64.0625/-139.431?op=3_day_rolling_average")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/fire_weather/point/json/fire_weather_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_fire_weather_point_csv(client):
    """Tests /fire_weather/point/<lat>/<lon>?op=3_day_rolling_average&format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/fire_weather/point/64.8378/-147.7164?op=3_day_rolling_average&format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
