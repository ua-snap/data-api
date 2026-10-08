# Delta change method in Arctic-EDS

**Questions.**
- Does Arctic-EDS apply the delta change method?
- If not, can Rasdaman/WCPS do the math for us?
- How much would the numbers in the app change?

**Method under review** (from the issue):

| Variable type | GCM delta | Future value |
|---|---|---|
| Not zero-bounded (temperature, degree days) | `G_future − G_hist` | `Baseline + delta` |
| Zero-bounded (precipitation, snowfall, wet days) | `G_future / G_hist` | `Baseline × delta`, with a multiplier cap |

The method needs each GCM's own historical run (`G_hist`). Where a coverage doesn't contain it, the method can't be applied without ingesting the GCM historical runs from source. Nothing in this report infers or substitutes `G_hist`.

All numbers come from 24 test sites across 7 Alaska regions, pulled from the production Rasdaman (`zeus.snap.uaf.edu`) on 2026-10-08. Every "current" value was checked against the live `earthmaps.io/eds/all` response, and all of them reproduce it.

---

## TL;DR

1. **The delta change method is not applied anywhere in the API or the app.** Each plate shows a baseline (CRU-TS, Daymet or ERA-Interim) next to raw GCM-future statistics, and the frontend subtracts one from the other ([evidence](#a-is-the-delta-change-method-applied)). The downscaling used to produce the source data is a separate step, not this method.
2. **Only the degree-day components can use the method with the data in Rasdaman today** (freezing index, thawing index, heating degree days). Their coverages contain each GCM's 1980–2009 run. The temperature, precipitation, snowfall and wet-day coverages contain **no GCM historical data**; this was checked at every site. Those four components need the GCM historical runs ingested from source first.
3. **WCPS can do the math server-side.** For the degree days:
   - Additive and multiplicative (capped) versions each run in one request per point, about 0.2 s, and match a Python implementation exactly.
   - The cap logic was verified where it actually binds.
   - Statewide grids take one request each, under 1 s ([Part B](#b-can-wcps-do-it-server-side)).
4. **For degree days the effect on the displayed numbers is modest.** The GCMs' 1980–2009 runs already sit close to Daymet. Mid-century shifts in the % change the app displays (pp = percentage points):

   | Index | Typical shift | Worst site |
   |---|---|---|
   | Freezing index | ~3 pp | 8 pp (Kodiak: −58% becomes −50%) |
   | Thawing index | ~2 pp | 6.8 pp (Utqiagvik: +76% becomes +69%) |
   | Heating degree days | ~1 pp | 1.8 pp |

   Individual model runs are biased more than the ensemble mean (−16% to +14%), so the min/max ranges the app shows move more than the means.
5. **The 3× cap never binds for degree days at the annual scale.** It would bind on 0.25% of thawing-index model-years only if that index were treated multiplicatively. Degree days are naturally additive (sums of temperature), so the additive form is used here.

---

## A. Is the delta change method applied?

**No.** The evidence is laid out below by data availability, then by code.

### What each component shows today, and whether G_hist exists

`G_hist` availability was checked by querying every GCM slice over the historical years at all 24 sites ([`check_gcm_historical.py`](check_gcm_historical.py), [`data/gcm_historical_availability.csv`](data/gcm_historical_availability.csv)).

| EDS component | Coverage | "Historical" shown | "Future" shown | GCM historical values found | Delta change possible today? |
|---|---|---|---|---|---|
| Temperature | `tas_2km_historical_wcs` / `tas_2km_projected_wcs` | CRU-TS 4.0 2 km, 1901–2015 | AR5 2 km 5ModelAvg/GFDL/NCAR, 2006–2100 | **None.** The projected coverage starts in 2006; the historical one has no model axis | **No.** Needs GCM historical ingest |
| Precipitation | `annual_precip_totals_mm` | CRU-TS 4.0 2 km, 1901–2015 | AR5 2 km, 5 GCMs × 3 RCPs | **0 of 60,480** (GCM slices, 1901–2005) | **No.** Needs GCM historical ingest |
| Snowfall (SFE) | `mean_annual_snowfall_mm` | CRU-TS 3.1, 1910–2009 | AR5, 5 GCMs × 3 RCPs, 2010–2099 | **0 of 4,800** (GCM slices, 1910–2009) | **No.** Needs GCM historical ingest |
| Wet days per year | `wet_days_per_year` | ERA-Interim WRF 20 km, 1980–2009 | GFDL-CM3 / NCAR-CCSM4 WRF, 2006–2100 | **0 of 1,248** (GCM slices, 1980–2005) | **No.** Needs WRF GCM historical ingest |
| Freezing index | `air_freezing_index_Fdays` | Daymet, 1980–2009 | NCAR 12 km, 9 GCMs × 2 RCPs | **12,420 of 19,440**¹ | **Yes** |
| Thawing index | `air_thawing_index_Fdays` | Daymet, 1980–2009 | NCAR 12 km, 9 GCMs × 2 RCPs | **12,420 of 19,440**¹ | **Yes** |
| Heating degree days | `heating_degree_days_Fdays` | Daymet, 1980–2009 | NCAR 12 km, 9 GCMs × 2 RCPs | **12,420 of 19,440**¹ | **Yes** |

¹ Every GCM × RCP track holds 1950–2005 values. The remaining empty cells are the unused `historical` scenario index for GCMs and Ketchikan, which falls outside the NCAR 12 km grid.

The other components (permafrost, hydrology, elevation, precipitation frequency) are model outputs or static layers, not baseline-vs-GCM comparisons, and are out of scope.

### Code evidence (API)

The summaries are plain min/mean/max statistics over raw coverage values. Nothing references a model's own historical period.

- **Precipitation.** [taspr.py:1442](../routes/taspr.py#L1442) computes the historical statistics from CRU-TS. [taspr.py:1456–1469](../routes/taspr.py#L1456-L1469) then pools every GCM × RCP annual value in each era and takes min/mean/max.
- **Temperature.** [taspr.py:767–860](../routes/taspr.py#L767) averages the projected `tas_2km` values per era. No historical model run is requested, and none exists in the coverage.
- **Degree days.** [degree_days.py:534–560](../routes/degree_days.py#L534-L560) takes Daymet 1980–2009 statistics as `modeled_baseline` and pools raw GCM values per era. The GCM 1980–2009 values are in the coverage but are never read.
- **Snowfall.** [snow.py:85–116](../routes/snow.py#L85-L116) takes CRU decades as historical and pools all GCM decades as projected.
- **Wet days.** [wet_days_per_year.py:44](../routes/wet_days_per_year.py#L44) splits the coverage by year range: 1980–2009 is ERA-Interim and 2006–2100 is the GCMs.
- A search across all EDS routes for `delta|anomal|bias|ratio` finds nothing relevant. Delta and anomaly logic exists only in unrelated endpoints (`temperature_anomalies.py`, `arctic_hydrology.py`, `conus_hydrology.py`, `fire_weather.py`).

### Code evidence (Arctic-EDS frontend, `ua-snap/arctic-eds@a25b445`)

- `app/components/Diff.vue` computes `future − past` (abs) or `(future − past) / past` (pct). It is display arithmetic on the API's baseline and future means, not a delta-change adjustment.
- The degree-day plates use `kind="pct"` and the temperature and precipitation plates use `kind="abs"`. So the "change" a user reads is `GCM_future − Baseline`, which mixes the climate signal with any difference between each GCM's historical run and the baseline.

### Downscaling is not the delta change method

The source datasets were statistically downscaled and bias-adjusted before ingest. That step corrects the GCM fields against observations, but it does not make the app's displayed change equal `G_future − G_hist`. The delta change method is a separate step applied when summarizing for the app, and it needs `G_hist`.

One thing to check: the Arctic-EDS temperature, precipitation and snowfall plates say the data were *"bias corrected via the delta method"*. If the downscaling method was QDM, that wording may need updating so readers don't confuse it with the delta change method.

---

## B. Can WCPS do it server-side?

**Yes, wherever `G_hist` is in the coverage.** [`wcps_server_side.py`](wcps_server_side.py) runs the full calculation in Rasdaman at every test site and checks it against the same formula in numpy on the same data ([`data/wcps_validation.csv`](data/wcps_validation.csv)).

| Index | Formula run in WCPS (mean over 9 GCMs × 2 RCPs, 2040–2069) | Mean time per point | Max \|WCPS − Python\| |
|---|---|---|---|
| Freezing / thawing / HDD | additive: `B + (G_fut − G_hist)` | 0.20 s | 0.000 |
| Freezing / thawing / HDD | multiplicative: `B × min(G_fut / G_hist, 3)` | 0.20 s | 0.000 |

The 3× cap never binds on era means at these sites (the largest is 2.49×), so the cap was also tested with tighter caps that do bind ([`data/wcps_cap_check.csv`](data/wcps_cap_check.csv)). At Utqiagvik's thawing index, WCPS and Python agree exactly:

| Cap | Runs clipped | WCPS = Python | Uncapped |
|---|---|---|---|
| 1.5× | 9 of 18 | 1076.06 | 1240.38 |
| 1.3× | 14 of 18 | 970.06 | 1240.38 |

Example: the additive delta for one point, all 9 GCMs × 2 RCPs, in one request:

```
for $c in (air_freezing_index_Fdays) return encode(
  (double)avg($c[model(0),scenario(0),year(1980:2009),X(x),Y(y)])
  + (condense + over $m model(1:9), $s scenario(1:2)
     using (double)avg($c[model($m),scenario($s),year(2040:2069),X(x),Y(y)])
         - (double)avg($c[model($m),scenario($s),year(1980:2009),X(x),Y(y)])) / 18.0,
  "application/json")
```

For the multiplicative form, the body becomes `min(G_fut / G_hist, 3.0)`. `switch case … return … default return …` and boolean masks also work.

**Statewide grids** take one request each: the 12 km adjustment field `Daymet − ensemble G_hist` took 0.7–0.8 s and 0.6 MB per index ([Fig. 5](#figures)).

### Rasdaman quirks found (relevant to any future WCPS implementation)

1. **Mixed-length `avg()` fails.** Combining `avg()` over subsets of different lengths (e.g. 115 years vs 30 years) raises *"axes not compatible"*. Workaround: cast each aggregate, as in `(double)avg(...)`.
2. **`condense` over a `year` iterator mis-indexes.** It sends geo years to the wrong grid index, and over an ANSI date axis it hung for more than 2 minutes. Workaround: write out explicit sums of year slices and send the query by POST.
3. **`tas_2km_projected_wcs` has an irregular scenario axis.** Its coefficients are `0, 2`, so RCP 8.5 is `scenario(2)` even though the metadata encoding labels it `"1"`.
4. **GeoTIFF axis order differs by coverage.** The NCAR 12 km coverages come back with X/Y transposed; the AR5 2 km coverages do not.

### What's needed for the other four components

WCPS is not the blocker; the inputs are missing. Each of these needs its GCM historical runs ingested, either as new coverages or as additional slices in the existing ones, covering the same period as the baseline the app shows:

| Component | GCM historical runs to ingest | Baseline the app shows |
|---|---|---|
| Temperature | AR5 GCMs, downscaled the same way as the projections | 1901–2015 |
| Precipitation | AR5 GCMs, downscaled the same way as the projections | 1901–2015 |
| Snowfall | AR5 GCMs | 1910–2009 |
| Wet days | WRF-downscaled GFDL-CM3 and NCAR-CCSM4 | 1980–2009 |

After that, the same query pattern applies unchanged.

---

## C. How big is the difference? (degree days)

### Method

| Term | Definition |
|---|---|
| `B` | Daymet 1980–2009 mean (the "modeled baseline" the app shows) |
| `G_hist` | Each GCM × RCP track's own 1980–2009 mean |

Every future model-year is adjusted as `max(B + (G_year − G_hist), 0)`, then summarized into min/mean/max per era exactly as the API does today. The multiplicative form with 1.5×, 2× and 3× caps was also computed for comparison ([`data/summary_long.csv`](data/summary_long.csv)).

### Results: mid-century (2040–2069), 23 sites

The full per-site, per-era table is in [`data/comparison.csv`](data/comparison.csv).

| Index | Mean value shift, delta − current (median, range) | Median \|shift\| in displayed % change | Max \|shift\| | Where it's largest |
|---|---|---|---|---|
| Freezing index | +80 °F·days (+16 to +279), +4.5% | 2.9 pp | 8.0 pp | Kodiak (−58% → −50%), southern coast |
| Thawing index | −59 °F·days (−83 to −12), −1.5% | 1.9 pp | 6.8 pp | Utqiagvik (+76% → +69%), Deadhorse |
| Heating degree days | +148 °F·days (+61 to +330), +1.4% | 1.2 pp | 1.8 pp | Fairly uniform statewide |

Directionally, the GCM ensemble's 1980–2009 runs are:

| Index | Ensemble 1980–2009 vs Daymet | Effect of the delta method |
|---|---|---|
| Freezing index | slightly lower (−1% to −8%) | projected freezing-index losses shrink |
| Heating degree days | slightly lower (−0.6% to −1.8%) | projected HDD losses shrink |
| Thawing index | slightly higher (+0.4% to +6.8%) | projected thawing-index gains shrink |

The additive shift is the same in every era, so it matters proportionally more early in the century, when the climate signal is smaller.

**Ranges move more than means** ([Fig. 6](#figures)). Each GCM run gets its own offset, and individual runs are biased by −16% to +8% (freezing index), −4% to +14% (thawing index) and −4% to +2% (HDD) relative to Daymet ([Fig. 3](#figures)). The median shifts in the reported minimum are:

| Index | Median shift in minimum | Notes |
|---|---|---|
| Freezing index | 4.3% | Near-zero minimums at southern coastal sites swing from −100% (floored to 0) to +152% (Kodiak: 9 → 23 °F·days) |
| Thawing index | −2.7% | Range −8.0% to +0.7% |
| Heating degree days | 2.2% | Range −0.8% to +6.0% |

### Multiplier caps and thresholds

Degree days are sums of temperature, so the additive form fits the issue's rule for non-zero-bounded variables. If they were treated multiplicatively ([Fig. 4](#figures)), here is the share of model-years above each cap:

| Index | > 1.5× | > 2× | > 3× |
|---|---|---|---|
| Thawing index | 18.9% | 2.9% | 0.25% |
| Freezing index | 0.6% | 0.05% | 0 |
| Heating degree days | 0 | 0 | 0 |

Freezing-index ratios reach 0.00 at some sites, where the future freezing index goes to zero. Along the far southern coast and in the Aleutians, the Daymet freezing-index baseline itself approaches zero (masked in Fig. 5). That is the tiny-denominator case the issue describes: a ratio method there would need a threshold, while the additive method does not.

---

## Recommendations

1. **Degree days: apply the delta change method (additive, floor at 0).** All inputs are in Rasdaman, and the WCPS query exists and is validated. The effect on displayed means is modest (≤ 8 pp) and larger on the min/max ranges.
2. **Temperature, precipitation, snowfall, wet days: ingest GCM historical runs before doing anything.** Until then these plates can't use the method, and we shouldn't approximate `G_hist`. Ingesting the runs is the decision point; the WCPS side is ready.
3. **Keep a multiplier cap (3×) and a minimum-denominator threshold as defaults** in any shared implementation for zero-bounded variables. They aren't exercised by the degree-day data, but they will matter for precipitation and wet days once those inputs exist, especially at monthly or daily scales.
4. **Do points in WCPS and precompute grids.** Point queries add about 0.2 s per component, and 12 km statewide fields take under 1 s each.
5. **Review the plate wording** that says the data are "bias corrected via the delta method", so it isn't read as the delta change method.

---

## Figures

**Fig. 1: Mid-century % change shown in the app, current (blue) vs delta change method (orange).**
![](figures/fig1_change_current_vs_delta.png)

**Fig. 2: Shift in the displayed % change, by site and index.** The same in every era.
![](figures/fig2_pp_shift_heatmap.png)

**Fig. 3: Each GCM's 1980–2009 bias against Daymet.** This is what the delta method removes.
![](figures/fig3_gcm_historical_bias.png)

**Fig. 4: How often multiplicative caps would bind if degree days were treated as ratios.**
![](figures/fig4_ratio_caps.png)

**Fig. 5: Statewide adjustments, each computed server-side in one WCPS request.**
![](figures/fig5_wcps_statewide_maps.png)

**Fig. 6: Freezing-index min/mean/max as the app reports it, current vs delta.**
![](figures/fig6_freezing_index_mmm.png)

---

## Side observation (not investigated)

In the projected temperature summary, [taspr.py:823](../routes/taspr.py#L823) sets `monthly_max = max(monthly_mean_values)`, with the matching line for min. So the projected monthly "tasmax"/"tasmin" columns are the max/min of monthly *means*, not of tasmax/tasmin. That may be intentional, but it's worth confirming.

## Reproducing

Use the `api-env` conda environment and set `PROJ_DATA` to its `share/proj` directory:

```sh
cd delta_change_method
python fetch_site_data.py        # ~8 min; caches point cubes for all EDS coverages to data/raw/
python check_gcm_historical.py   # which coverages contain GCM historical runs
python analyze.py                # degree days: current vs delta summaries -> data/*.csv
python wcps_server_side.py       # Part B: server-side validation, cap check, statewide GeoTIFFs
python make_figures.py           # figures/
```

| File | Purpose |
|---|---|
| `sites.py` | The 24 test sites |
| `wcps.py` | WCPS helpers |
| `data/raw/` | Cached point cubes |
| `data/maps/*.tif` | Statewide grids (git-ignored; regenerate with `wcps_server_side.py`) |
