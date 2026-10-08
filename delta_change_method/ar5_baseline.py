"""Temperature and precipitation: quantify the baseline mismatch in the AR5 2km data.

The AR5/CMIP5 2km temperature and precipitation coverages behind the EDS were produced with
the delta method (Walsh et al. 2018, doi:10.1016/j.envsoft.2018.03.021, Sec. 3.2 and 4):
for every year and calendar month, each GCM's change from its own 1961-1990 historical run
was added to the 2km PRISM 1961-1990 climatology. So the delta change method has already been
applied to these values, and the "change" each value carries is relative to PRISM 1961-1990.

The app instead shows CRU-TS 4.0 1901-2015 statistics as the historical baseline, so the
change a user reads is the model delta plus a baseline offset:

    displayed change = G_fut - B_app = (G_fut - PRISM_6190) + (PRISM_6190 - B_app)

PRISM 1961-1990 is not in Rasdaman. It is represented here by the 1961-1990 mean of the
CRU-TS 4.0 2km historical coverage. That coverage was itself delta-downscaled onto the same
PRISM 1961-1990 climatology, so its 1961-1990 mean equals PRISM at every pixel by construction
(whether the deltas were differences or ratios). This rests on SNAP's description of the CRU
product and has not been checked against the PRISM grids themselves.

Writes data/ar5_baseline_comparison.csv and data/ar5_tas_monthly_offset.csv.

Usage: python ar5_baseline.py   (run fetch_site_data.py first)
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
RAW = HERE / "data" / "raw"
DATA = HERE / "data"

ERAS = {"2010-2039": (2010, 2039), "2040-2069": (2040, 2069), "2070-2099": (2070, 2099)}
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def temperature(site):
    """App annual summary: CRU-TS 1901-2015 vs 5ModelAvg RCP 8.5 era means (°C)."""
    hist = np.array(site["tas_2km_historical_wcs"], dtype=float)  # month x year 1901-2015
    proj = np.array(site["tas_2km_projected_wcs"], dtype=float)  # model x scen x month x year 2006-2100
    hist_annual = np.nanmean(hist, axis=0)
    B_app = np.nanmean(hist_annual)
    ref = np.nanmean(hist_annual[60:90])  # 1961-1990
    annual = np.nanmean(proj[0, 1], axis=0)  # 5ModelAvg, RCP 8.5
    futs = {era: np.nanmean(annual[a - 2006 : b - 2006 + 1]) for era, (a, b) in ERAS.items()}
    monthly = np.nanmean(hist, axis=1) - np.nanmean(hist[:, 60:90], axis=1)  # B_app - ref per month
    return B_app, ref, futs, monthly


def precipitation(site):
    """App summary: CRU-TS 1901-2015 vs all 5 GCMs x 3 RCPs pooled, annual totals (mm)."""
    pr = np.array(site["annual_precip_totals_mm"], dtype=float)  # model x year 1901-2100 x scen
    B_app = np.nanmean(pr[0, :115, 0])
    ref = np.nanmean(pr[0, 60:90, 0])
    futs = {era: np.nanmean(pr[2:7, a - 1901 : b - 1901 + 1, 1:4]) for era, (a, b) in ERAS.items()}
    return B_app, ref, futs, None


def main():
    rows, monthly_rows = [], []
    for fp in sorted(RAW.glob("*.json")):
        site = json.loads(fp.read_text())
        for comp, fn in [("temperature", temperature), ("precipitation", precipitation)]:
            B_app, ref, futs, monthly = fn(site)
            for era, fut in futs.items():
                row = dict(
                    site=site["name"],
                    region=site["region"],
                    component=comp,
                    era=era,
                    future=fut,
                    baseline_app=B_app,
                    baseline_delta_ref=ref,
                    change_displayed=fut - B_app,
                    change_vs_delta_ref=fut - ref,
                    baseline_offset=ref - B_app,
                )
                if comp == "precipitation":
                    row["pct_change_displayed"] = 100 * (fut - B_app) / B_app
                    row["pct_change_vs_delta_ref"] = 100 * (fut - ref) / ref
                    row["pp_shift"] = row["pct_change_vs_delta_ref"] - row["pct_change_displayed"]
                rows.append(row)
            if monthly is not None:
                monthly_rows.append({"site": site["name"], **dict(zip(MONTHS, monthly))})

    df = pd.DataFrame(rows)
    df.to_csv(DATA / "ar5_baseline_comparison.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(DATA / "ar5_tas_monthly_offset.csv", index=False)

    mid = df[df.era == "2040-2069"]
    pd.set_option("display.width", 200)
    print(mid.groupby("component")[["baseline_offset", "change_displayed", "change_vs_delta_ref"]].describe().round(2).T)
    print(mid[mid.component == "precipitation"].pp_shift.describe().round(2))


if __name__ == "__main__":
    main()
