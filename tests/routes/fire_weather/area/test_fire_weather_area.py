import csv
import io
import json

from tests.json_compare import assert_json_allclose


def test_fire_weather_area_1908031103(client):
    """Tests /fire_weather/area/<id> for the 1908031103 (Rock Creek) HUC10 polygon."""
    response = client.get("/fire_weather/area/1908031103?op=3_day_rolling_average")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/fire_weather/area/json/fire_weather_area_1908031103.json") as f:
        expected_data = json.load(f)

    assert_json_allclose(actual_data, expected_data)


def test_fire_weather_area_ytpa21(client):
    """Tests /fire_weather/area/<id> for the YTPA21 (Yukon) polygon; outside the fire weather coverage extent."""
    response = client.get("/fire_weather/area/YTPA21?op=3_day_rolling_average")
    assert response.status_code == 404


def test_fire_weather_area_invalid_huc10(client):
    """Tests /fire_weather/area/<id> for a HUC10-level area ID; outside the fire weather coverage extent."""
    response = client.get("/fire_weather/area/1903010300?op=3_day_rolling_average")
    assert response.status_code == 404


def test_fire_weather_area_vars(client):
    """Tests /fire_weather/area/<id>?vars= for the 1908031103 HUC10 polygon returns a parseable response."""
    response = client.get("/fire_weather/area/1908031103?op=3_day_rolling_average&vars=bui")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_fire_weather_area_op(client):
    """Tests /fire_weather/area/<id>?op= for the 1908031103 HUC10 polygon with a non-default operation returns a parseable response."""
    response = client.get("/fire_weather/area/1908031103?op=summer_fire_danger_rating_days")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_fire_weather_area_csv(client):
    """Tests /fire_weather/area/<id>?op=3_day_rolling_average&format=csv for the 1908031103 HUC10 polygon returns a parseable CSV."""
    response = client.get("/fire_weather/area/1908031103?op=3_day_rolling_average&format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
