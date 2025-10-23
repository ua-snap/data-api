import json


def test_temperature_area(client):
    """
    Tests the /temperature/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/temperature/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_area(client):
    """
    Tests the /precipitation/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/precipitation/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_area(client):
    """
    Tests the /taspr/area/<id> endpoint to ensure the output
    remains consistent with production for the given area ID.
    """
    response = client.get("/taspr/area/19010208")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/taspr_area_19010208.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_point(client):
    """
    Tests the /temperature/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/temperature/point/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_point_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_point(client):
    """
    Tests the /precipitation/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/precipitation/point/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_point_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_taspr_point(client):
    """
    Tests the /taspr/point/<lat>/<lon> endpoint to ensure the output
    remains consistent with production for the given point.
    """
    response = client.get("/taspr/point/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/taspr_point_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_frequency_point(client):
    """
    Tests the /precipitation/frequency/point/<lat>/<lon> endpoint to ensure
    the output remains consistent with production for the given point.
    """
    response = client.get("/precipitation/frequency/point/65.028/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_frequency_point_65.028_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_yearly(client):
    """
    Tests the /temperature/<lat>/<lon> endpoint (annual mean temperature)
    for JSON parity with production.
    """
    response = client.get("/temperature/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_yearly(client):
    """
    Tests the /precipitation/<lat>/<lon> endpoint (annual total precipitation)
    for JSON parity with production.
    """
    response = client.get("/precipitation/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_jan_yearly(client):
    """
    Tests the /temperature/jan/<lat>/<lon> endpoint for JSON parity.
    """
    response = client.get("/temperature/jan/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_jan_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_july_yearly(client):
    """
    Tests the /temperature/july/<lat>/<lon> endpoint for JSON parity.
    """
    response = client.get("/temperature/july/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_july_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_year_range(client):
    """
    Tests the /temperature/<lat>/<lon>/<start>/<end> endpoint for JSON parity.
    """
    response = client.get("/temperature/65.0628/-146.1627/1940/2060")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_65.0628_-146.1627_1940_2060.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_year_range(client):
    """
    Tests the /precipitation/<lat>/<lon>/<start>/<end> endpoint for JSON parity.
    """
    response = client.get("/precipitation/65.0628/-146.1627/1940/2060")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_65.0628_-146.1627_1940_2060.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_jan_year_range(client):
    """
    Tests the /temperature/jan/<lat>/<lon>/<start>/<end> endpoint for JSON parity.
    """
    response = client.get("/temperature/jan/65.0628/-146.1627/1940/2060")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_jan_65.0628_-146.1627_1940_2060.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_july_year_range(client):
    """
    Tests the /temperature/july/<lat>/<lon>/<start>/<end> endpoint for JSON parity.
    """
    response = client.get("/temperature/july/65.0628/-146.1627/1940/2060")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_july_65.0628_-146.1627_1940_2060.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_tas2km_point(client):
    """
    Tests the /tas2km/point/<lat>/<lon> endpoint for JSON parity.
    """
    response = client.get("/tas2km/point/65.0628/-146.1627")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/tas2km_point_65.0628_-146.1627.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_yearly_mmm(client):
    """
    Tests /temperature/<lat>/<lon>?summarize=mmm for JSON parity with production.
    """
    response = client.get("/temperature/65.0628/-146.1627?summarize=mmm")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_65.0628_-146.1627_mmm.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_yearly_mmm(client):
    """
    Tests /precipitation/<lat>/<lon>?summarize=mmm for JSON parity with production.
    """
    response = client.get("/precipitation/65.0628/-146.1627?summarize=mmm")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_65.0628_-146.1627_mmm.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_temperature_year_range_mmm(client):
    """
    Tests /temperature/<lat>/<lon>/<start>/<end>?summarize=mmm for JSON parity.
    """
    response = client.get("/temperature/65.0628/-146.1627/1940/2060?summarize=mmm")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/temperature_65.0628_-146.1627_1940_2060_mmm.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_precipitation_year_range_mmm(client):
    """
    Tests /precipitation/<lat>/<lon>/<start>/<end>?summarize=mmm for JSON parity.
    """
    response = client.get("/precipitation/65.0628/-146.1627/1940/2060?summarize=mmm")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/precipitation_65.0628_-146.1627_1940_2060_mmm.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
