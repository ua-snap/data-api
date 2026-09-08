import csv
import io
import json


def test_temperature_anomalies_point_fairbanks(client):
    """Tests /temperature_anomalies/point/<lat>/<lon>/ at Fairbanks, AK."""
    response = client.get("/temperature_anomalies/point/64.8378/-147.7164/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/temperature_anomalies/point/json/temperature_anomalies_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_anomalies_point_ocean(client):
    """Tests /temperature_anomalies/point/<lat>/<lon>/ at an ocean point; within the coverage's extent, so real data is expected."""
    response = client.get("/temperature_anomalies/point/66.95/-165/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/temperature_anomalies/point/json/temperature_anomalies_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_anomalies_point_attu(client):
    """Tests /temperature_anomalies/point/<lat>/<lon>/ at Attu, AK."""
    response = client.get("/temperature_anomalies/point/52.8339/173.1794/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/temperature_anomalies/point/json/temperature_anomalies_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_anomalies_point_dawson_city(client):
    """Tests /temperature_anomalies/point/<lat>/<lon>/ at Dawson City, Yukon."""
    response = client.get("/temperature_anomalies/point/64.0625/-139.431/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/temperature_anomalies/point/json/temperature_anomalies_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_anomalies_point_reykjavik(client):
    """Tests /temperature_anomalies/point/<lat>/<lon>/ at Reykjavik, Iceland; Berkeley Earth's historical dataset is global, so real data is expected."""
    response = client.get("/temperature_anomalies/point/64.1466/-21.9426/")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/temperature_anomalies/point/json/temperature_anomalies_point_reykjavik.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_anomalies_point_csv(client):
    """Tests /temperature_anomalies/point/<lat>/<lon>/?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/temperature_anomalies/point/64.8378/-147.7164/?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
