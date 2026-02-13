def test_search_documentation(client):
    """
    Tests the /search/ documentation endpoint to ensure it returns a 200 status
    and contains the expected documentation content.
    """
    response = client.get("/search/")
    assert response.status_code == 200
    
    # Check that the response contains expected documentation content
    html = response.data.decode("utf-8")
    assert "Places Search" in html
    assert "/places/search/" in html
    assert "Geographic Search by Coordinates" in html
    assert "Community Search by Name" in html
