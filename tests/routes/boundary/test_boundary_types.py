def test_boundary_area_huc8(client):
    """Tests /boundary/area/<id> for a HUC-8 polygon returns parseable JSON."""
    response = client.get("/boundary/area/19070506")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_huc10(client):
    """Tests /boundary/area/<id> for a HUC-10 polygon returns parseable JSON."""
    response = client.get("/boundary/area/1901010301")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_huc12(client):
    """Tests /boundary/area/<id> for a HUC-12 polygon returns parseable JSON."""
    response = client.get("/boundary/area/190202011303")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_protected_area(client):
    """Tests /boundary/area/<id> for an Alaska/BC/Yukon protected area polygon returns parseable JSON."""
    response = client.get("/boundary/area/NPS12")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_corporation(client):
    """Tests /boundary/area/<id> for an Alaska Native corporation polygon returns parseable JSON."""
    response = client.get("/boundary/area/NC8")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_climate_division(client):
    """Tests /boundary/area/<id> for an Alaska climate division polygon returns parseable JSON."""
    response = client.get("/boundary/area/CD3")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_ecoregion(client):
    """Tests /boundary/area/<id> for an Alaska EPA Level III ecoregion polygon returns parseable JSON."""
    response = client.get("/boundary/area/AKECO1")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_fire_zone(client):
    """Tests /boundary/area/<id> for a fire management zone polygon returns parseable JSON."""
    response = client.get("/boundary/area/FIRE2")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_ethnolinguistic_region(client):
    """Tests /boundary/area/<id> for an Alaska ethnolinguistic region polygon returns parseable JSON."""
    response = client.get("/boundary/area/EL4")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_borough(client):
    """Tests /boundary/area/<id> for an Alaska borough polygon returns parseable JSON."""
    response = client.get("/boundary/area/BORO4")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_census_area(client):
    """Tests /boundary/area/<id> for an Alaska census area polygon returns parseable JSON."""
    response = client.get("/boundary/area/CENS3")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_game_management_unit(client):
    """Tests /boundary/area/<id> for a game management unit polygon returns parseable JSON."""
    response = client.get("/boundary/area/GMU5")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_first_nation(client):
    """Tests /boundary/area/<id> for a first nation polygon returns parseable JSON."""
    response = client.get("/boundary/area/FNTT6")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_yt_fire_district(client):
    """Tests /boundary/area/<id> for a Yukon fire district polygon returns parseable JSON."""
    response = client.get("/boundary/area/YTFD4")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_yt_game_management_subzone(client):
    """Tests /boundary/area/<id> for a Yukon game management subzone polygon returns parseable JSON."""
    response = client.get("/boundary/area/YTGMA2")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_boundary_area_yt_watershed(client):
    """Tests /boundary/area/<id> for a Yukon watershed polygon returns parseable JSON."""
    response = client.get("/boundary/area/YTHYDRO1")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None
