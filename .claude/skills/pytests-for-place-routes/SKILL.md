---
name: pytests-for-place-routes
description: 'Use when asked to generate, add, or write smoke-test pytests for a data-api route/endpoint that serves community locations or area/polygon boundaries (e.g. /places/<type>, /boundary/area/<id>). Unlike pytests-for-data-routes, these tests only validate that the live response is parseable JSON with a 200 status — no golden-fixture comparison. Trigger phrases: "add smoke tests for place routes", "test the places endpoint", "test area polygon endpoints", "add pytests for boundary/places".'
---

# Pytests for Place Routes

## When to Use
- The user asks for smoke-test pytest coverage of routes that return reference/lookup data for communities, place types, or area/polygon boundaries — not modeled climate/environmental data.
- Typical candidates: `/places/<type>` (list of communities or areas of a given type) and `/boundary/area/<id>` (GeoJSON polygon for a given place ID). These payloads are large, static reference datasets where a full golden-fixture comparison (as used by the `pytests-for-data-routes` skill) is unnecessary overhead — a smoke test that confirms the endpoint returns valid, parseable JSON is sufficient.
- Do not use this skill for point/area endpoints that return modeled data values (temperature, precipitation, streamflow, etc.) — use the `pytests-for-data-routes` skill for those instead.

## What the test validates
For each endpoint under test:
1. Hit the real endpoint through the Flask test client (see `tests/conftest.py`'s `client` fixture) — do not hand-write or save expected JSON.
2. Assert `response.status_code == 200`.
3. Assert the response body is parseable JSON, e.g.:
   ```python
   data = response.get_json(silent=True)
   assert data is not None
   ```
4. That's it — no fixture files, no field-by-field comparison, no schema validation. If a status code other than 200 is genuinely expected for a particular input (e.g. an invalid ID), assert that exact status code instead, per the same live-observation rule as `pytests-for-data-routes`.

## Step-by-step procedure
1. Read `templates/documentation/<route>.html` and enumerate every endpoint listed in the "Example URL" column of its service-endpoint table(s) — these are the only endpoints to test. Do not fabricate additional inputs/IDs beyond what the documentation page itself lists as examples.
2. For routes with a single parameterized endpoint (e.g. `/boundary/area/<var_id>`) documented with multiple example IDs (one per supported place/polygon type), write one test function per documented example ID — each is exercising a materially different code path (different polygon/place type).
3. For routes with multiple distinct endpoint paths (e.g. `/places/<type>` with many example `<type>` values), write one test function per documented example URL.
4. Hit each documented example live, and record the actual status code — do not assume 200 for every input; some example IDs may legitimately 404/422 (verify live, don't guess).
5. Name test functions `test_<route>_<folder>_<slug>`, where `<slug>` is a short identifier derived from the type or ID under test (e.g. `test_places_local_corporations`, `test_boundary_area_huc8`).
6. Directory layout: `tests/routes/<route>/<folder>/test_<route>_<folder>.py`, where `<folder>` matches the route's own URL path segment (e.g. `area` for `/boundary/area/<id>`, since that folder/naming convention may already exist from the `pytests-for-data-routes` skill — in that case add a **new, separate file** here rather than merging into the existing golden-fixture test file, e.g. `test_boundary_area_types.py`). If the endpoint has no clean point/area/stream URL segment (e.g. `/places/<type>`), use `local` as the folder name. No `json/` fixture subfolder is needed, since there are no fixtures to save.
7. Run `micromamba run -n api-env pytest tests/routes/<route> -q` and confirm everything passes before reporting completion.
