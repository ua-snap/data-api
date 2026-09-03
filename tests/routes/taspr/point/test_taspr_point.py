import csv
import io
import json


def test_temperature_point(client):
    """
    Tests the /temperature/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/temperature/point/62.27/-154.61")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_point_62.27_-154.61.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_point(client):
    """
    Tests the /precipitation/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/precipitation/point/62.27/-154.61")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/precipitation_point_62.27_-154.61.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_point(client):
    """
    Tests the /taspr/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/taspr/point/62.27/-154.61")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/taspr_point_62.27_-154.61.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_point_fairbanks(client):
    """Tests /temperature/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/temperature/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_point_ocean(client):
    """Tests /temperature/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/temperature/point/66.95/-165")
    assert response.status_code == 404


def test_temperature_point_attu(client):
    """Tests /temperature/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/temperature/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_point_dawson_city(client):
    """Tests /temperature/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/temperature/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_point_fairbanks(client):
    """Tests /precipitation/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/precipitation/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/precipitation_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_point_ocean(client):
    """Tests /precipitation/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/precipitation/point/66.95/-165")
    assert response.status_code == 404


def test_precipitation_point_attu(client):
    """Tests /precipitation/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/precipitation/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/precipitation_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_point_dawson_city(client):
    """Tests /precipitation/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/precipitation/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/precipitation_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_point_fairbanks(client):
    """Tests /taspr/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/taspr/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/taspr_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_point_ocean(client):
    """Tests /taspr/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/taspr/point/66.95/-165")
    assert response.status_code == 404


def test_taspr_point_attu(client):
    """Tests /taspr/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/taspr/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/taspr_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_frequency_point_fairbanks(client):
    """Tests /precipitation/frequency/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/precipitation/frequency/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/precipitation_frequency_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_frequency_point_ocean(client):
    """Tests /precipitation/frequency/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/precipitation/frequency/point/66.95/-165")
    assert response.status_code == 404


def test_precipitation_frequency_point_attu(client):
    """Tests /precipitation/frequency/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/precipitation/frequency/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/precipitation_frequency_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_frequency_point_dawson_city(client):
    """Tests /precipitation/frequency/point/<lat>/<lon> at Dawson City, Yukon; outside the coverage, so nodata is expected."""
    response = client.get("/precipitation/frequency/point/64.0625/-139.431")
    assert response.status_code == 404


def test_tas2km_point_fairbanks(client):
    """Tests /tas2km/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/tas2km/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/tas2km_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_tas2km_point_ocean(client):
    """Tests /tas2km/point/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/tas2km/point/66.95/-165")
    assert response.status_code == 404


def test_tas2km_point_attu(client):
    """Tests /tas2km/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/tas2km/point/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/tas2km_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_tas2km_point_dawson_city(client):
    """Tests /tas2km/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/tas2km/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/tas2km_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_jan_point_fairbanks(client):
    """Tests /temperature/jan/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/temperature/jan/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_jan_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_jan_point_ocean(client):
    """Tests /temperature/jan/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/temperature/jan/66.95/-165")
    assert response.status_code == 404


def test_temperature_jan_point_attu(client):
    """Tests /temperature/jan/<lat>/<lon> at Attu, AK."""
    response = client.get("/temperature/jan/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_jan_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_jan_point_dawson_city(client):
    """Tests /temperature/jan/<lat>/<lon> at Dawson City, Yukon; outside the coverage, so nodata is expected."""
    response = client.get("/temperature/jan/64.0625/-139.431")
    assert response.status_code == 404


def test_temperature_july_point_fairbanks(client):
    """Tests /temperature/july/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/temperature/july/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_july_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_july_point_ocean(client):
    """Tests /temperature/july/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/temperature/july/66.95/-165")
    assert response.status_code == 404


def test_temperature_july_point_attu(client):
    """Tests /temperature/july/<lat>/<lon> at Attu, AK."""
    response = client.get("/temperature/july/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/temperature_july_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_july_point_dawson_city(client):
    """Tests /temperature/july/<lat>/<lon> at Dawson City, Yukon; outside the coverage, so nodata is expected."""
    response = client.get("/temperature/july/64.0625/-139.431")
    assert response.status_code == 404


def test_taspr_point_dawson_city(client):
    """Tests /taspr/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/taspr/point/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/taspr/point/json/taspr_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_point_csv(client):
    """Tests /temperature/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/temperature/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_precipitation_point_csv(client):
    """Tests /precipitation/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/precipitation/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_taspr_point_csv(client):
    """Tests /taspr/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/taspr/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_tas2km_point_csv(client):
    """Tests /tas2km/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/tas2km/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_temperature_jan_csv(client):
    """Tests /temperature/jan/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/temperature/jan/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_temperature_july_csv(client):
    """Tests /temperature/july/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/temperature/july/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_precipitation_frequency_point_csv(client):
    """Tests /precipitation/frequency/point/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/precipitation/frequency/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
