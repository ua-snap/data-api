def test_fire_point_fairbanks(client):
    """Tests /fire/point/<lat>/<lon> at Fairbanks, AK."""
    response = client.get("/fire/point/64.8378/-147.7164")
    assert response.status_code == 200
    # Fire data changes regularly, so we only verify the response is parseable
    # JSON rather than comparing against a static fixture.
    assert response.get_json() is not None


def test_fire_point_ocean(client):
    """Tests /fire/point/<lat>/<lon> at an ocean point."""
    response = client.get("/fire/point/66.95/-165")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_fire_point_attu(client):
    """Tests /fire/point/<lat>/<lon> at Attu, AK."""
    response = client.get("/fire/point/52.8339/173.1794")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_fire_point_dawson_city(client):
    """Tests /fire/point/<lat>/<lon> at Dawson City, Yukon."""
    response = client.get("/fire/point/64.0625/-139.431")
    assert response.status_code == 200
    assert response.get_json() is not None
