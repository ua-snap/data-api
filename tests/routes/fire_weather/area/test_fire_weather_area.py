import csv
import io
import json


def test_fire_weather_area_19080309(client):
    """Tests /fire_weather/area/<id> for the 19080309 (Tolovana River) HUC8 polygon."""
    response = client.get("/fire_weather/area/19080309?op=3_day_rolling_average")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/fire_weather/area/json/fire_weather_area_19080309.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_fire_weather_area_ytpa21(client):
    """Tests /fire_weather/area/<id> for the YTPA21 (Yukon) polygon; outside the fire weather coverage extent."""
    response = client.get("/fire_weather/area/YTPA21?op=3_day_rolling_average")
    assert response.status_code == 404


def test_fire_weather_area_invalid_huc10(client):
    """Tests /fire_weather/area/<id> for a HUC10-level area ID; outside the fire weather coverage extent."""
    response = client.get("/fire_weather/area/1903010300?op=3_day_rolling_average")
    assert response.status_code == 404


def test_fire_weather_area_csv(client):
    """Tests /fire_weather/area/<id>?op=3_day_rolling_average&format=csv for the 19080309 HUC8 polygon returns a parseable CSV."""
    response = client.get("/fire_weather/area/19080309?op=3_day_rolling_average&format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
