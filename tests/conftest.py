import pytest
from application import application


@pytest.fixture
def client():
    """Create a Flask test client."""
    with application.test_client() as client:
        yield client
