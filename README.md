# Data API for SNAP@IARC

## Installing

Install Micromamba via brew:

```
brew install micromamba
```

Create a new `mamba` environment like so:

```
micromamba env create -f environment.yml
```

This creates a Mamba environment called api-env which you can activate with:

```
micromamba activate api-env
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

#### Place Route Tests

To add pytests for routes that serve community locations or area/polygon boundaries (e.g. `/places/<type>`, `/boundary/area/<id>`), invoke the `pytests-for-place-routes` Claude skill (`.claude/skills/pytests-for-place-routes/SKILL.md`). Ask Claude (via GitHub Copilot or Claude Code) to "add tests for place routes" or "test the `<route>` endpoint". These tests only confirm a 200 status and a parseable JSON response since the underlying dataset is more likely to change over time (places manually added or removed) and unlikely to change due to postprocessing operations, etc.

#### Data Route Tests

To add pytests for route reponses (expected JSON and/or HTTP status codes), invoke the `pytests-for-data-routes` Claude skill (`.claude/skills/pytests-for-data-routes/SKILL.md`). Ask Claude (via GitHub Copilot or Claude Code) to "add pytest tests for the `<route>` endpoint" or "backfill missing point/area tests for `<route>`". The skill defines the standard test locations/areas and pytest directory structure, so it can generate the test files directly.

#### Test Guidance

- Keep it simple
- Favor integration-scope over unit-scope
- Ask: is this test useful?
- Consider testing overhead (e.g., the largest polygons could be annoying test cases because of their lengthy durations)
- Don't add tests to just add tests, 100% coverage not realistically the goal
- Let the test suite evolve organically: fiddling with a tricky bit of code and want to be able to move with more confidence? Good signal to add a test
- Consider adding conjugate, non-happy path tests, e.g., does `/route/area/null*$(!*)` yield the expected status code?
