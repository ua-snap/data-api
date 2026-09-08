import csv
import io
import json


def test_places_search_fairbanks(client):
    """Tests /places/search/<lat>/<lon> nearby-communities-and-areas search at Fairbanks, AK."""
    response = client.get("/places/search/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/vectordata/local/json/vectordata_places_search_fairbanks.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_places_search_ocean(client):
    """Tests /places/search/<lat>/<lon> at an ocean point; still returns 200 with any nearby areas found."""
    response = client.get("/places/search/66.95/-165")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/vectordata/local/json/vectordata_places_search_ocean.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_places_search_attu(client):
    """Tests /places/search/<lat>/<lon> at Attu, AK (positive longitude, past the antimeridian)."""
    response = client.get("/places/search/52.8339/173.1794")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/vectordata/local/json/vectordata_places_search_attu.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_places_search_dawson_city(client):
    """Tests /places/search/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/places/search/64.0625/-139.431")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/vectordata/local/json/vectordata_places_search_dawson_city.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_places_type_corporations(client):
    """Tests /places/<type> with a valid type value (corporations)."""
    response = client.get("/places/corporations")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/vectordata/local/json/vectordata_places_type_corporations.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_places_type_csv(client):
    """Tests /places/<type>?format=csv for the corporations type returns a parseable CSV."""
    response = client.get("/places/corporations?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_places_type_invalid(client):
    """Tests /places/<type> with an invalid type value; the route has no explicit type validation, so it falls through to an empty result list with a 200 rather than an error."""
    response = client.get("/places/invalidtype")
    assert response.status_code == 200
    assert response.get_json() == []


def test_places_search_communities(client):
    """Tests /places/search/communities with extent and substring query params."""
    response = client.get("/places/search/communities?extent=alaska&substring=fair")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/vectordata/local/json/vectordata_places_search_communities.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data
