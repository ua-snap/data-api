# Data API for SNAP@IARC

## Installing

Running the API requires Python 3.11. 

Use `pipenv` to create a virtual environment from the repo's `Pipfile` using:
```
pipenv install
```

Alternatively, create a new `conda` environment like so:

```
conda create -n api python=3.11
conda activate api
conda install -c conda-forge flask flask-cors gunicorn aiohttp requests marshmallow numpy xarray h5py h5netcdf rioxarray rasterio pyproj shapely geopandas rtree fiona jaro-winkler pytest
```

## Running application

Set `flask` application environment variables:

```
export FLASK_APP=application.py
export FLASK_DEBUG=True
```


Review environment variables found in `config.py`, and reset them for development if necessary (e.g., `export API_GS_BASE_URL=https://gs-dev.earthmaps.io/geoserver/`).


Start the application via `pipenv`:

```
pipenv run flask run
```

Or alternatively, from your `conda` environment:

```
flask run
```

## Query API endpoints

Example Permafrost Query:

- http://localhost:5000/permafrost/point/gipl/65.0628/-146.1627
- http://localhost:5000/permafrost/point/gipl/62.906/-148.345

Example Fire Query:

- http://localhost:5000/fire/point/65.0628/-146.1627
- http://localhost:5000/fire/point/62.906/-148.345

Example Precipitation Query:

- http://localhost:5000/precipitation/point/65.028/-146.1627
- http://localhost:5000/precipitation/point/62.906/-148.345


## Updating Production API + Varnish Cache

Due to the configuration of our services, we need to both update our API instance + restart our Varnish cache when necessary.

Prior to deploying, update the `requirements.txt`:

```bash
pipenv run pip freeze > requirements.txt
git commit -am'update requirements.txt'
```

Instructions for this can be found here:

- [SNAP Data API Instructions](https://docs.google.com/document/d/1Z31-mkDE0skITOuOOMBQwuO2I8jUDuApm7VX-A9v1LA/edit?usp=sharing)

## Test Suite
### Running Tests
Run `pytest` or `pytest -v` from the root directory of this repository.

The test client is created from the same Flask "app" object that a local development instance uses (see `conftest.py`). Basically everything is the same, except there is no actual network socket opened. Stuff that is inbound to the API happens in-memory (no server or port), but the outbound stuff (requests to Rasdaman and Geoserver) still creates normal HTTP requests.

### Adding Tests
#### Area Query JSON Integrity Blueprint
- test name must be prefixed with `test_`
- match the name to the routing, exactly
  - e.g. `test_alfresco_flammability_area` maps to `/alfresco/flammability/area`
- assert the expected HTTP status code
- assert the response from the local client maps to the reference JSON
   - add reference JSON via `curl`
   - `curl -sS https://earthmaps.io/taspr/area/19010208 -o tests/taspr_area_19010208.json`
   - ensure the reference JSON file name maps exactly to the route

### Test Guidance
 - Keep it simple
 - Favor integration-scope over unit-scope
 - Ask: is this test useful?
 - Consider testing overhead (e.g., the largest polygons could be annoying test cases because of their lengthy durations)
 - Don't add tests to just add tests, 100% coverage not realistically the goal
 - Let the test suite evolve organically: fiddling with a tricky bit of code and want to be able to move with more confidence? Good signal to add a test
 - Consider adding conjugate, non-happy path tests, e.g., does `/route/area/null*$(!*)` yield the expected status code?