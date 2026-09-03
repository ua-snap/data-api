---
name: pytests-for-data-routes
description: 'Use when asked to generate, add, or write new pytest tests for a data-api route/endpoint. Defines the standard set of point locations and area IDs to test, the expected pass/fail behavior for each, and the tests/routes/<route>/{point,area,local}/ + json/ fixture directory layout. Trigger phrases: "generate tests for", "add pytest tests for", "write endpoint tests", "test the <route> endpoint".'
---

# New Pytests for Route

## When to Use
- The user asks for new pytest tests covering a route in `routes/*.py` (point, area, local, and/or stream endpoints).
- The user asks to backfill missing point, area, or stream coverage for an existing route's tests.

## Required test locations

### Point / local endpoints
Test these four locations in the route's point-based (or local-based, for routes like `alfresco` that only expose `local` instead of `point`) test script:

| Location | Coordinates | Notes |
|---|---|---|
| Fairbanks, Alaska | `lat=64.8378, lon=-147.7164` | In-bounds interior Alaska land point (forested, near tree line); generally returns real data across most coverages. |
| Ocean point | `lat=66.95, lon=-165` | Bering Strait ocean point; many terrestrial coverages will legitimately return a nodata/404 here — that is the expected, correct behavior, not a bug. |
| Attu, Alaska | `lat=52.8339, lon=173.1794` | Westernmost Aleutians; note the **positive** longitude (past the antimeridian). Useful for coverage-boundary edge cases. Public coordinate — the formerly-used community ID `AK26` was confirmed absent from the app's own place registry as of 2026-09-03. |
| Dawson City, Yukon | `lat=64.0625, lon=-139.431` | Yukon, Canada — useful for confirming whether a coverage is Alaska-only vs. pan-Arctic. Resolved from community ID `YT16` via `fetch_data.get_place_data` on 2026-09-03. |
| Reykjavík, Iceland | `lat=64.1466, lon=-21.9426` | **Conditional, 5th location** — only test this if the route's coverage is pan-Arctic/international in scope (see below). Confirmed live against this repo's endpoints on 2026-09-04. |

Use these fixed coordinates directly — do not re-resolve them from community IDs.

#### When to add the Reykjavík location
Only add a Reykjavík test to a route's point/local test script if the route's documentation (`templates/documentation/<route>.html`) explicitly describes pan-Arctic, circumpolar, or global coverage, or the underlying dataset is known to be global (e.g. a global reanalysis/CMIP6 product not clipped to North America). Confirm by actually hitting the live endpoint at the Reykjavík coordinates first — a route is only "applicable" if it returns something other than a coverage-out-of-bounds error (i.e. not the same 422/404 you'd expect from a point far outside a regional Alaska/Yukon-only coverage). Do not add the Reykjavík test to routes whose documentation states a regional extent (e.g. "most of Alaska and parts of western Canada", "North American boreal ecoregion", "central Alaska landmass and immediately neighboring Canada") — for those, Reykjavík would just be a redundant out-of-bounds case already covered by the Attu/ocean locations.

As of 2026-09-04, confirmed pan-Arctic/international routes in this repo (live-tested, real 200 data at Reykjavík): `cmip6` (`/cmip6/point`), `seaice` (`/seaice/point`), `temperature_anomalies` (`/temperature_anomalies/point`), and the CMIP6 variant of `indicators` (`/indicators/cmip6/point` only — NOT `/indicators/cmip5/point`, which is regional and returns 422 at Reykjavík). Re-verify live for any other route before assuming applicability; do not assume pan-Arctic scope from a route's name alone (e.g. `arctic_hydrology` is Alaska-stream-based, not a lat/lon endpoint, and `permafrost`/`era5wrf`/`fire_weather`/`landfastice` all reference "Arctic" in their citations but are regionally clipped to Alaska/NW Canada and return 422/404 at Reykjavík).

### Area endpoints (only if the route supports area queries)
Test these three area IDs in the route's area-based test script:

| Area ID | Notes |
|---|---|
| `19080309` | Alaska HUC8 watershed. |
| `YTPA21` | Yukon, Canada polygon. |
| `1903010300` | HUC10-level watershed that crosses the antimeridian. |

Do not invent additional locations or substitute different ones. If a route has no area endpoint at all, skip the area section entirely rather than writing a test against a nonexistent path.

### Stream-based endpoints (route uses `<stream_id>` instead of lat/lon)
Some routes (e.g. `arctic_hydrology`, `conus_hydrology`) index by a stream network segment ID rather than a coordinate or polygon. Use these fixed stream IDs:

| Route family | Stream ID | Notes |
|---|---|---|
| CONUS (`conus_hydrology`) | `50101` | Columbia River — a large, well-gauged CONUS stream segment. Confirmed live against this repo's endpoints on 2026-09-04. |
| Arctic (`arctic_hydrology`) | `81014458` | Alaska/Canada stream segment (MERIT Hydro network). Confirmed live against this repo's endpoints on 2026-09-04. |

Only test the stream-based endpoints that are actually **listed as an example URL in `templates/documentation/<route>.html`** (look for `<a href="/<route>/...">` entries in the "Example URL" columns of the service-endpoint tables) — by default, do not fabricate tests for undocumented/internal-looking endpoints in `routes/*.py`. The one confirmed exception in this repo: `arctic_hydrology/hydroviz/<stream_id>` and `conus_hydrology/hydroviz/<stream_id>` are undocumented on the doc pages (they power an internal hydroviz webapp, not a publicly-listed example) but the user has explicitly asked for them to be tested anyway (2026-09-04) — both return 200 with real payloads at the fixed stream IDs above, and are covered in `tests/routes/<route>/stream/test_<route>_stream.py` alongside the documented endpoints. If asked to test other undocumented endpoints, confirm with the user first rather than assuming exclusion or inclusion. `conus_hydrology/gage_info` takes no ID at all (it's a flat list endpoint) — treat it as its own single test case with no stream ID substitution.

Do not invent additional stream IDs or substitute different ones (e.g. do not reuse the doc page's own example ID like `81000004` or `50563` — use the fixed IDs from the table above instead).

### CSV export tests (`?format=csv`)
If the route file (`routes/<route>.py`) contains `request.args.get("format") == "csv"` anywhere for a given endpoint, that endpoint supports CSV export and needs exactly **one** additional test — do not add the full 4/5-location matrix for CSV, just a single smoke test at Fairbanks (`lat=64.8378, lon=-147.7164`), or the route's single fixed stream ID for stream-based endpoints.

This test only needs to confirm the response is a valid, parseable CSV — it does **not** need a saved fixture and does **not** need its contents compared:
- Append `&format=csv` (or `?format=csv` if no other query params) to the endpoint URL. If the endpoint requires another query param to produce meaningful output (e.g. `fire_weather`'s `?op=...`), chain both per the doc page's own examples (e.g. `?op=3_day_rolling_average&format=csv`).
- Assert `response.status_code == 200`.
- Assert the response is parseable as CSV, e.g.:
  ```python
  import csv
  import io

  rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
  assert len(rows) > 0
  ```
- Do not assert on `content_type`/mimetype specifics or exact row contents — different endpoints attach metadata header rows before the data rows, so the shape varies; just confirm it parses into at least one row without raising and returns 200.

Name this test `test_<route>_<point|area|local|stream>_csv` and place it in the same test file as the other tests for that endpoint type (no separate file/folder needed).

### Fixture size limits
JSON fixtures must never exceed **25MB** on disk (never let one anywhere near the 50MB danger zone). Before saving a fixture, check the serialized size of the live response (e.g. `len(json.dumps(actual_data))`, or just check the saved file's size with `os.path.getsize`/`ls -la` and delete/redo it if it's too big).

If a live response is **under 25MB**: save it as a full golden fixture as normal, and assert full equality against it (existing behavior, unchanged).

If a live response is **over 25MB** (this mostly affects large-polygon area/zonal-stats endpoints or endpoints returning many nested models/scenarios/dates): do not save the full payload. Instead:
1. Take a bounded, deterministic subset of the response that still exercises real structure — e.g. the first 5 top-level keys of a dict (`{k: actual_data[k] for k in list(actual_data)[:5]}`), or the first 5 items of a list. Pick whatever slice keeps the saved subset well under 25MB.
2. Save that subset as the fixture, named with a `_subset` suffix (e.g. `<route>_point_<location-slug>_subset.json`) so it's obvious at a glance that it isn't the full response.
3. In the test, take the *same* slice from the live response and assert that slice equals the saved subset — do not compare the full live response against the subset fixture (they won't match). Also assert something cheap about the overall shape of the full response (e.g. `isinstance(actual_data, dict)` and it's non-empty) so a gross regression (empty response, wrong type) is still caught even though the full payload isn't diffed.
4. Add a one-line comment in the test explaining why only a subset is compared (payload exceeds the 25MB fixture cap).

## Directory and fixture layout
Follow the existing `tests/routes/<route>/{point,area,local}/` split. Store expected JSON fixtures in a **`json/` subdirectory** nested under the point/area/local folder (this is distinct from earlier tests in this repo that stored fixtures flat — new tests use the `json/` subfolder):

```
tests/routes/<route>/point/test_<route>_point.py
tests/routes/<route>/point/json/<route>_point_<location-slug>.json
tests/routes/<route>/area/test_<route>_area.py
tests/routes/<route>/area/json/<route>_area_<id>.json
tests/routes/<route>/local/test_<route>_local.py
tests/routes/<route>/local/json/<route>_local_<location-slug>.json
tests/routes/<route>/stream/test_<route>_stream.py
tests/routes/<route>/stream/json/<route>_stream_<endpoint-name>.json
```

Location slugs: `fairbanks`, `ocean`, `attu`, `dawson_city`, and (when applicable) `reykjavik`. For stream-based endpoints, name fixtures/tests after the endpoint segment instead of a location slug (e.g. `<route>_stream_stats.json`, `<route>_stream_wt_modeled_climatology.json`), since there's only ever one stream ID per route family.

## Step-by-step procedure
1. Read `routes/<route>.py` and `templates/documentation/<route>.html` to find the actual point/area/local/stream URL patterns (e.g. `/<route>/point/<lat>/<lon>`, `/<route>/area/<var_id>`, `/<route>/stats/<stream_id>`), any accepted coverages, and the documented spatial extent — this tells you which validators (`validate_latlon`, `validate_var_id`, bbox checks, `stream_id.isdigit()`) apply and whether the Reykjavík location is applicable (see above). For stream-based routes, enumerate every endpoint from the doc page's "Example URL" table entries (not every route decorator in `routes/*.py` — some are undocumented/internal). While reading the route file, also check for `request.args.get("format") == "csv"` to know whether a CSV smoke test is needed (see above).
2. Use the fixed lat/lon coordinates (or stream IDs) from the tables above directly as request inputs — no place-ID resolution step is needed. Include Reykjavík only if step 1 confirms pan-Arctic/international scope.
3. For every location/area, hit the real endpoint through the Flask test client (see `tests/conftest.py`'s `client` fixture) — do not hand-write expected JSON.
4. Classify the live response:
   - **200 with a real payload under 25MB**: save the actual JSON response as a golden fixture under the matching `json/` folder, and assert the live response equals the loaded fixture (matches the existing golden-file pattern used elsewhere in `tests/routes`).
   - **200 with a real payload of 25MB or larger**: do not save the full payload — save and assert against a bounded subset instead (see "Fixture size limits" above).
   - **Non-200 (400/404/422/502/etc.)**: this is expected for out-of-bounds points, nodata ocean points, or coverages that don't extend into Canada — assert the exact status code the live endpoint actually returns. Do not guess the code; run the request and observe it.
   - **Endpoint/operation not supported at all** (no such route): don't fabricate a test for it.
5. Write the test file(s) using the observed status codes/fixtures, one test function per location/area/stream-endpoint, named `test_<route>_<point|area|local>_<location-slug>` (or `test_<route>_stream_<endpoint-name>` for stream-based endpoints, e.g. `test_conus_hydrology_stream_gage_info`), plus one `test_<route>_<point|area|local|stream>_csv` per CSV-capable endpoint (see above).
6. Run `micromamba run -n api-env pytest tests/routes/<route> -q` and confirm everything passes before reporting completion.
