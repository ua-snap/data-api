import json


def test_eds_all(client):
    """
    Tests the /eds/all/<lat>/<lon>/ endpoint to ensure the output
    remains consistent with production for the given latitude and longitude.
    """
    response = client.get("/eds/all/62.27/-154.61")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/eds/point/json/eds_point.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_eds_all_fairbanks(client):
    """Tests /eds/all/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/eds/all/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/eds/point/json/eds_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_eds_all_ocean(client):
    """Tests /eds/all/<lat>/<lon> at an ocean point."""
    response = client.get("/eds/all/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/eds/point/json/eds_point_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_eds_all_attu(client):
    """Tests /eds/all/<lat>/<lon> at Attu, AK."""
    response = client.get("/eds/all/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/eds/point/json/eds_point_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_eds_all_dawson_city(client):
    """Tests /eds/all/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/eds/all/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/eds/point/json/eds_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
