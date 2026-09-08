def test_places_local_all(client):
    """Tests /places/all returns parseable JSON."""
    response = client.get("/places/all")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_communities(client):
    """Tests /places/communities returns parseable JSON."""
    response = client.get("/places/communities")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_hucs(client):
    """Tests /places/hucs returns parseable JSON."""
    response = client.get("/places/hucs")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_protected_areas(client):
    """Tests /places/protected_areas returns parseable JSON."""
    response = client.get("/places/protected_areas")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_corporations(client):
    """Tests /places/corporations returns parseable JSON."""
    response = client.get("/places/corporations")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_climate_divisions(client):
    """Tests /places/climate_divisions returns parseable JSON."""
    response = client.get("/places/climate_divisions")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_ecoregions(client):
    """Tests /places/ecoregions returns parseable JSON."""
    response = client.get("/places/ecoregions")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_fire_zones(client):
    """Tests /places/fire_zones returns parseable JSON."""
    response = client.get("/places/fire_zones")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_ethnolinguistic_regions(client):
    """Tests /places/ethnolinguistic_regions returns parseable JSON."""
    response = client.get("/places/ethnolinguistic_regions")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_boroughs(client):
    """Tests /places/boroughs returns parseable JSON."""
    response = client.get("/places/boroughs")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_census_areas(client):
    """Tests /places/census_areas returns parseable JSON."""
    response = client.get("/places/census_areas")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_game_management_units(client):
    """Tests /places/game_management_units returns parseable JSON."""
    response = client.get("/places/game_management_units")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_first_nations(client):
    """Tests /places/first_nations returns parseable JSON."""
    response = client.get("/places/first_nations")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_yt_fire_districts(client):
    """Tests /places/yt_fire_districts returns parseable JSON."""
    response = client.get("/places/yt_fire_districts")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_yt_game_management_subzones(client):
    """Tests /places/yt_game_management_subzones returns parseable JSON."""
    response = client.get("/places/yt_game_management_subzones")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None


def test_places_local_yt_watersheds(client):
    """Tests /places/yt_watersheds returns parseable JSON."""
    response = client.get("/places/yt_watersheds")
    assert response.status_code == 200
    data = response.get_json(silent=True)
    assert data is not None
