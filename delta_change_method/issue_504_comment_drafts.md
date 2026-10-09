<!--
Drafts for comments on ua-snap/arctic-eds#504, one per Arctic-EDS dataset, following the
structure of the Temperature and Precipitation comment. Each comment sits between the
"=== COMMENT ===" markers below. Lines like [attach: figures/...] mark where to drag in an
image from delta_change_method/figures/ (GitHub will replace them with <img> tags).
-->

=== COMMENT: Degree days ===

## **Degree Days: Freezing Index, Thawing Index, Heating Degree Days:**

### Sources
In Arctic-EDS, we use this dataset:
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/f9831074-cd3f-4c06-8601-687bd2911b7e

derived from the NCAR 12 km Alaska Near Surface Meteorology Daily Averages (1950–2099) dataset:
- https://doi.org/10.5065/c3kn-2y77
- https://doi.org/10.1016/j.cliser.2022.100312

### Does the source data use the delta method?
**No.** The 9 GCMs (ACCESS1-3, CanESM2, CCSM4, CSIRO-Mk3-6-0, GFDL-ESM2M, inmcm4, MIROC5, MPI-ESM-MR, MRI-CGCM3) were statistically downscaled and bias corrected with BCSD (bias-corrected spatial disaggregation), trained against Daymet. That's a different method from the delta method. The degree days in Rasdaman are computed from those downscaled GCM values directly.

### Does Arctic-EDS use the delta method?
**No.** The app shows Daymet 1980–2009 as the "modeled baseline" and compares it against raw (BCSD) GCM values for each future era.

**The good news:** this is the one dataset where we can apply the delta method with what's already in Rasdaman. The coverages (`air_freezing_index_Fdays`, `air_thawing_index_Fdays`, `heating_degree_days_Fdays`) contain each GCM's own 1950–2005 run (stored under the RCP tracks) alongside the Daymet baseline. Nothing needs to be ingested.

### How different are the GCM historical runs from the Daymet baseline?
For each test site, we compared each GCM run's average over 1980–2009 with Daymet's average over the same years (18 runs: 9 GCMs × 2 RCP tracks). Because BCSD was trained on Daymet, the models land close to Daymet, but not exactly on it:

| Index | Average of all 18 runs vs Daymet (range across sites) | Single runs vs Daymet (range across all runs and sites) |
|---|---|---|
| Freezing index | −1% to −8% | −16% to +8% |
| Thawing index | +0.4% to +6.8% | −4% to +14% |
| Heating degree days | −0.6% to −1.8% | −4% to +2% |

That difference between each model's historical simulation and Daymet is the bias the delta method removes.
- The averaged column is small, which is why the app's mean values shift only modestly.
- Single runs miss by more, in both directions, which is why the min/max ranges shift more: each run gets its own correction.

[attach: figures/fig3_gcm_historical_bias.png]

[attach: figures/fig5_wcps_statewide_maps.png]

### What happens to the change signal if we apply the delta method?
Additive method: `Daymet 1980–2009 + (GCM future − GCM 1980–2009)`, per model and scenario, floored at 0. At mid-century (2040–2069), across 23 test sites:

| Index | Shift in displayed % change (median) | Largest shift |
|---|---|---|
| Freezing index | 2.9 pp | 8.0 pp (Kodiak: −58% becomes −50%) |
| Thawing index | 1.9 pp | 6.8 pp (Utqiagvik: +76% becomes +69%) |
| Heating degree days | 1.2 pp | 1.8 pp |

The delta method trims the projected change for all three indices. The shift is the same in every era, so it matters most early in the century. Min/max ranges move more than means, because each model run gets its own correction. At southern coastal sites, the near-zero freezing-index minimums swing by more than 100% (Kodiak: 9 → 23 °F·days).

[attach: figures/fig1_change_current_vs_delta.png]

[attach: figures/fig2_pp_shift_heatmap.png]

[attach: figures/fig6_freezing_index_mmm.png]

We used the additive method (adding a difference) for degree days, not the multiplicative one (multiplying by a ratio). Ratios cause trouble here. Along the far southern coast and in the Aleutians, the freezing index is close to zero, so dividing by it produces very large, unstable multipliers, the same divide-by-small-number problem described in this issue for precipitation. The additive method avoids that, and degree days are sums of temperature, so adding a difference is the natural fit anyway.

[attach: figures/fig4_ratio_caps.png]

### How do we fix this?

In order of difficulty....

#### Option 1:
1. Apply the additive delta in the API at query time with WCPS. The queries are written and validated in `delta_change_method/wcps_server_side.py` on the `delta_change_method` branch: they match a Python implementation exactly and take ~0.2 s per point.
2. Keep the min/mean/max summaries, computed from the delta-adjusted model-years.
3. Revise app and API documentation.

#### Option 2:
1. Precompute delta-adjusted degree-day coverages (one WCPS request per index statewide, under 1 s each at 12 km) and ingest them as new Rasdaman coverages.
2. Point the API at the new coverages.
3. Revise app and API documentation.

#### Option 3:
1. Leave the values as they are (BCSD already bias-corrects against Daymet, so the shifts are modest), and document that the change shown is GCM future vs Daymet rather than a delta-method change.

=== COMMENT: Precipitation frequency ===

## **Precipitation Frequency:**

### Sources
In Arctic-EDS, we use this dataset:
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/304b6d89-961e-417d-b6ba-4139c7fe5ff6

described in:
- https://uaf-snap.org/wp-content/uploads/2021/05/dot-precip_FINAL-REPORT_20210526.pdf
- https://doi.org/10.1175/JAMC-D-21-0106.1

code:
- https://github.com/ua-snap/precip-dot

### Does the source data use the delta method?
**Yes**, exactly the multiplicative version described in this issue. Annual-maximum precipitation frequencies were computed from WRF runs driven by GFDL-CM3 and NCAR-CCSM4: each GCM's own historical run (1979–2005) and its RCP 8.5 future. In the `precip-dot` pipeline:
- `pipeline/deltas.py` divides each GCM's future by its own historical run (`proj_ds['pf'] /= hist_ds['pf']`).
- `pipeline/multiply.py` multiplies those ratios (warped to 1 km) onto NOAA Atlas 14.

Two notes:
- **The ratios are not capped.** A later `fudge` step only forces values to increase with duration and return interval.
- The DOT report says in one place that ERA-Interim-driven WRF was the "modeled historical" data, but the code uses each GCM's historical run.

### Does Arctic-EDS use the delta method?
**N/A**. The app shows only the delta-adjusted future values (three eras), with no historical baseline beside them. So there's no baseline mismatch to fix. NOAA Atlas 14 (the baseline the ratios were applied to) isn't in Rasdaman.

### How do we fix this?

#### Option 1:
1. Nothing to fix. Revise app and API documentation to say the values are delta-method projections relative to NOAA Atlas 14.

#### Option 2 (only if we want to show change from a historical baseline):
1. Add NOAA Atlas 14 (Alaska) to Rasdaman, since it's the baseline the deltas were applied to.
2. Have the API return it alongside the projections, and show the change in the app.
3. Revise app and API documentation.

=== COMMENT: Snowfall ===

## **Snowfall:**

### Sources
In Arctic-EDS, we use these datasets:
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/557db5d5-dbeb-470a-a9c4-b80d78aa8668 (historical, CRU TS 3.1, 771 m)
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/7c0c1a65-794e-4770-aa72-4628d357808e (projected, CMIP5/AR5, 771 m)

from these papers:
- https://doi.org/10.1002/hyp.9934
- https://doi.org/10.3390/w10050668

### Does the source data use the delta method?
**Unknown.** Snowfall equivalent (SFE) is computed as snow-day fraction × precipitation, from SNAP's 771 m downscaled AR5 temperature and precipitation. The catalog records don't say how those 771 m inputs were downscaled or against which baseline. If they followed the 2 km approach, the baseline would likely be PRISM 1971–2000 (SNAP's 771 m products use that period), but I couldn't confirm it. SFE is also a nonlinear function of temperature, so even with delta-downscaled inputs, the SFE values themselves aren't a simple delta.

### Does Arctic-EDS use the delta method?
**No**, but it also doesn't currently display a change. The snowfall section shows only a CSV preview of historical (CRU TS 3.1, 1910–2009) and projected (2010–2099) decades. The API computes a historical-vs-projected summary that the app doesn't render.

Rasdaman (`mean_annual_snowfall_mm`) has no GCM historical SFE (0 of 4,800 values checked across 24 sites), so the delta method can't be computed from what we have.

### How do we fix this?

In order of difficulty....

#### Option 1:
1. Find out how the 771 m AR5 inputs were downscaled (method and baseline period). Does anyone who worked on this remember?
2. If they were delta-downscaled onto PRISM 1971–2000, treat it like temperature/precipitation. Any baseline shown in the app should be 1971–2000, either from the CRU decades already in the coverage (1970s–1990s, i.e. 1970–1999) or from PRISM.
3. Revise app and API documentation.

#### Option 2:
1. If we want a true delta-method SFE, go back to the source data, compute SFE for each GCM's historical run, and ingest it.
2. Apply the multiplicative delta (with a cap) to the chosen baseline in the API.
3. Revise app and API documentation.

=== COMMENT: Hydrology ===

## **Hydrology:**

### Sources
In Arctic-EDS, we use this dataset:
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/2610188c-aa38-4f47-8987-b36ec72cdd0d

driven by:
- https://doi.org/10.5065/c3kn-2y77 (NCAR 12 km Alaska Near Surface Meteorology)

### Does the source data use the delta method?
**No.** VIC hydrologic model outputs are driven by GCM meteorology that was downscaled and bias corrected with BCSD (trained on Daymet), not the delta method. The outputs are model runs for each GCM, 1950–2099.

### Does Arctic-EDS use the delta method?
**No**, and the hydrology section doesn't display a change. It shows a CSV preview and download (decadal monthly values for all models, 1950–2099). The API's EDS summary does compute eras, but its "historical" era is each GCM's own 1950–2009 run.

### What's in Rasdaman?
- **GCM historical runs:** ✓ (all GCMs × 2 RCPs × 12 months × the six 1950–2009 decades, at every test site inside the grid).
- **Observed baseline:** ✗. There's no Daymet-forced (observation-driven) VIC run in the `hydrology` coverage.

So we can compute each model's own change (`GCM future − GCM historical`), but we have no observed baseline to add it to. The second half of the delta method isn't possible with current data.

Side note: the app text lists 9 GCMs, but the Rasdaman coverage has 10. It includes HadGEM2-ES, and lists GFDL-ESM2M where the app says "GFDL ESM2".

### How do we fix this?

In order of difficulty....

#### Option 1:
1. Nothing to fix in what's displayed today. Revise documentation to say the values are VIC outputs forced by BCSD-downscaled GCMs.

#### Option 2 (if we want to show change):
1. Show model-relative change (`GCM future − GCM 1950–2009`), which is available now from the existing coverage via WCPS.
2. Revise app and API documentation.

#### Option 3 (if we want the full delta method):
1. Add a Daymet-forced VIC run to Rasdaman as the observed baseline.
2. Apply the delta (additive for temperature/storage variables, multiplicative with a cap for flux variables like runoff) in the API.
3. Revise app and API documentation.

=== COMMENT: Permafrost ===

## **Permafrost:**

### Sources
In Arctic-EDS, we use this dataset:
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/c24a957b-8a56-40bf-bc09-43a567182d36

### Does the source data use the delta method?
**No.** These are GIPL 2.0 permafrost model outputs (mean annual ground temperature at several depths, permafrost top/base, talik thickness), driven by downscaled and bias-corrected AR5 climate (5-model average, GFDL-CM3, NCAR-CCSM4; RCP 4.5 and 8.5). They're model runs, not delta-adjusted values.

### Does Arctic-EDS use the delta method?
**No**, and it can't with current data. The GIPL coverage (`crrel_gipl_outputs_nc`) only covers 2021–2120. There's no historical GIPL run and no observed ground-temperature baseline in Rasdaman. The app shows future-era summaries only (2021–2039, 2040–2069, 2070–2099), with no baseline comparison.

### How do we fix this?

#### Option 1:
1. Nothing to fix in what's displayed today. Document that values are model projections with no historical baseline.

#### Option 2 (if we want to show change from a baseline):
1. Add a historical GIPL run or an observed ground-temperature baseline (e.g. Obu et al. 2018 mean annual ground temperature) to Rasdaman.
2. Decide how the delta would be defined for these variables. Ground temperature could be additive; depths and thicknesses need thought.
3. Revise app and API documentation.

=== COMMENT: Wet days per year ===

## **Wet Days Per Year:**

### Sources
- https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/7825535c-edff-4a82-89f3-9183e6cb2b42 (WRF 20 km dynamically downscaled ERA-Interim, GFDL-CM3, NCAR-CCSM4)

### Does the source data use the delta method?
**No.** Wet-day counts are computed directly from raw WRF daily precipitation: ERA-Interim for 1980–2009, and GFDL-CM3 / NCAR-CCSM4 (RCP 8.5) for 2006–2100.

### Does Arctic-EDS use the delta method?
**No**, but wet days aren't currently shown in the report. `/eds/all` returns them, but no report section renders them; they only appear on the maps page, which is being deprecated. Rasdaman has no GCM historical runs for this coverage (0 of 1,248 values checked for 1980–2005), so the delta method can't be computed from what we have.

### How do we fix this?
1. If wet days stay out of the report: nothing to do. Consider dropping them from `/eds/all`.
2. If they're added to the report: ingest the WRF GFDL-CM3 and NCAR-CCSM4 historical runs (as wet-day counts), then apply the multiplicative delta with a cap against the ERA-Interim baseline.

=== COMMENT: Elevation ===

## **Elevation:**

Static terrain summary from the ASTER Global Digital Elevation Model (via GeoServer). Not a climate projection, so the delta method doesn't apply. Nothing to do.
