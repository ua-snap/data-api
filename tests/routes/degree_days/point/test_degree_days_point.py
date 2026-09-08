import csv
import io
import json


def test_degree_days_heating_fairbanks(client):
    """Tests /degree_days/heating/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/degree_days/heating/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_heating_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_heating_ocean(client):
    """Tests /degree_days/heating/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/heating/66.95/-165")
    assert response.status_code == 404


def test_degree_days_heating_attu(client):
    """Tests /degree_days/heating/<lat>/<lon> at Attu, AK; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/heating/52.8339/173.1794")
    assert response.status_code == 404


def test_degree_days_heating_dawson_city(client):
    """Tests /degree_days/heating/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/degree_days/heating/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_heating_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_below_zero_fairbanks(client):
    """Tests /degree_days/below_zero/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/degree_days/below_zero/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_below_zero_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_below_zero_ocean(client):
    """Tests /degree_days/below_zero/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/below_zero/66.95/-165")
    assert response.status_code == 404


def test_degree_days_below_zero_attu(client):
    """Tests /degree_days/below_zero/<lat>/<lon> at Attu, AK; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/below_zero/52.8339/173.1794")
    assert response.status_code == 404


def test_degree_days_below_zero_dawson_city(client):
    """Tests /degree_days/below_zero/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/degree_days/below_zero/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_below_zero_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_thawing_index_fairbanks(client):
    """Tests /degree_days/thawing_index/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/degree_days/thawing_index/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_thawing_index_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_thawing_index_ocean(client):
    """Tests /degree_days/thawing_index/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/thawing_index/66.95/-165")
    assert response.status_code == 404


def test_degree_days_thawing_index_attu(client):
    """Tests /degree_days/thawing_index/<lat>/<lon> at Attu, AK; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/thawing_index/52.8339/173.1794")
    assert response.status_code == 404


def test_degree_days_thawing_index_dawson_city(client):
    """Tests /degree_days/thawing_index/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/degree_days/thawing_index/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_thawing_index_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_freezing_index_fairbanks(client):
    """Tests /degree_days/freezing_index/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/degree_days/freezing_index/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_freezing_index_point_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_freezing_index_ocean(client):
    """Tests /degree_days/freezing_index/<lat>/<lon> at an ocean point; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/freezing_index/66.95/-165")
    assert response.status_code == 404


def test_degree_days_freezing_index_attu(client):
    """Tests /degree_days/freezing_index/<lat>/<lon> at Attu, AK; outside the coverage, so nodata is expected."""
    response = client.get("/degree_days/freezing_index/52.8339/173.1794")
    assert response.status_code == 404


def test_degree_days_freezing_index_dawson_city(client):
    """Tests /degree_days/freezing_index/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/degree_days/freezing_index/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/degree_days/point/json/degree_days_freezing_index_point_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_degree_days_heating_summarize(client):
    """Tests /degree_days/heating/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/degree_days/heating/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_degree_days_below_zero_summarize(client):
    """Tests /degree_days/below_zero/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/degree_days/below_zero/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_degree_days_thawing_index_summarize(client):
    """Tests /degree_days/thawing_index/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/degree_days/thawing_index/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_degree_days_freezing_index_summarize(client):
    """Tests /degree_days/freezing_index/<lat>/<lon>?summarize=mmm at Fairbanks, AK returns a parseable response."""
    response = client.get("/degree_days/freezing_index/64.8378/-147.7164?summarize=mmm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_degree_days_heating_csv(client):
    """Tests /degree_days/heating/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/degree_days/heating/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_degree_days_below_zero_csv(client):
    """Tests /degree_days/below_zero/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/degree_days/below_zero/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_degree_days_thawing_index_csv(client):
    """Tests /degree_days/thawing_index/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/degree_days/thawing_index/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_degree_days_freezing_index_csv(client):
    """Tests /degree_days/freezing_index/<lat>/<lon>?format=csv at Fairbanks, AK returns a parseable CSV."""
    response = client.get("/degree_days/freezing_index/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
