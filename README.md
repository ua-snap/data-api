# Data API for SNAP@IARC

## Installing

Install Micromamba via brew:

```
brew install micromamba
```

Create a new `mamba` environment like so:

```
<<<<<<< HEAD
conda create -n api python=3.11
conda activate api
conda install -c conda-forge flask flask-cors gunicorn aiohttp requests marshmallow numpy xarray h5py h5netcdf rioxarray rasterio pyproj shapely geopandas rtree fiona jaro-winkler pytest
=======
micromamba env create -f environment.yml
```

This creates a Mamba environment called api-env which you can activate:

```
micromamba activate api-env
>>>>>>> ae2cba1 (Updates README to explicitly use Micromamba for building API environment.)
```

## Running application

Set `flask` application environment variables:

```
export FLASK_APP=application.py
export FLASK_DEBUG=True
```

Review environment variables found in `config.py`, and reset them for development if necessary (e.g., `export API_GS_BASE_URL=https://gs-dev.earthmaps.io/geoserver/`).

Start the application via your `mamba` environment:

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

## Creating and updating Production API + Varnish Cache

To create a new ElasticBeanstalk API instance, do the following:

```
git checkout production
git pull
eb init <-- Choose us-west-2 and data-api-production for application
eb create --it r7g.large --single
```

To update the current API instance, do the following:

```
git checkout production
git pull
eb deploy
```

Explicit instructions for creating and updating the API can be found here:

- [SNAP Data API Instructions](https://docs.google.com/document/d/18-pEC-Rri3EQcNXaHajhqMYmRc_LBX1p3wWKoYnC874/edit?tab=t.jzrka8gsdrfw)

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
