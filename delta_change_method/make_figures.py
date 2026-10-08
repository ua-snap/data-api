"""Render the report figures from the analyze.py / wcps_server_side.py outputs.

Only the degree-day components are shown: they are the only EDS components whose
coverages contain the GCM historical runs the delta change method needs.

Usage: python make_figures.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio as rio
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

from sites import SITES
from wcps import to_3338

HERE = Path(__file__).parent
DATA = HERE / "data"
FIGS = HERE / "figures"
FIGS.mkdir(exist_ok=True)

# reference palette (dataviz skill): categorical slots 1-2, diverging blue<->red, gray mid
CURRENT = "#2a78d6"
DELTA = "#eb6834"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#8a8985"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"
DIVERGING = LinearSegmentedColormap.from_list(
    "bluered", ["#104281", "#3987e5", "#9ec5f4", "#f0efec", "#f4a9a8", "#e34948", "#9b1c1c"]
)
DIVERGING.set_bad("#ffffff")

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "font.size": 10,
        "font.family": "sans-serif",
        "axes.edgecolor": GRID,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "axes.titlesize": 11,
        "axes.titleweight": "semibold",
        "xtick.color": INK2,
        "ytick.color": INK2,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "legend.frameon": False,
    }
)

COMPS = ["freezing_index", "thawing_index", "heating_degree_days"]
LABELS = {
    "freezing_index": "Freezing index",
    "thawing_index": "Thawing index",
    "heating_degree_days": "Heating degree days",
}
ERAS = ["2010-2039", "2040-2069", "2070-2099"]
REGION = {s[0]: s[1] for s in SITES}


def suptitle(fig, text, y=0.99):
    fig.suptitle(text, x=0.01, ha="left", fontsize=12, fontweight="semibold", color=INK, y=y)


def site_axis(ax, sites):
    """Sites top-to-bottom in SITES order, with dotted region separators."""
    ax.set_yticks(range(len(sites)))
    ax.set_yticklabels(sites)
    ax.set_ylim(len(sites) - 0.5, -0.5)
    for i in range(1, len(sites)):
        if REGION[sites[i]] != REGION[sites[i - 1]]:
            ax.axhline(i - 0.5, color=MUTED, lw=0.6, ls=(0, (2, 2)))


def fig_dumbbells(comp, sites):
    """The % change the app displays, current vs delta method, mid-century."""
    df = comp[comp.era == "2040-2069"]
    fig, axes = plt.subplots(1, 3, figsize=(13, 7.6), sharey=True)
    y = np.arange(len(sites))
    for ax, c in zip(axes, COMPS):
        d = df[df.component == c].set_index("site").reindex(sites)
        ax.hlines(y, d.pct_change_current, d.pct_change_delta, color=MUTED, lw=1.5, zorder=1)
        ax.scatter(d.pct_change_current, y, s=36, color=CURRENT, zorder=2, edgecolor=SURFACE, lw=1.2, label="Current (app today)")
        ax.scatter(d.pct_change_delta, y, s=36, color=DELTA, zorder=3, edgecolor=SURFACE, lw=1.2, label="Delta change method")
        ax.axvline(0, color=INK2, lw=0.8)
        ax.set_title(LABELS[c], loc="left")
        ax.set_xlabel("% change vs Daymet 1980–2009 baseline")
        ax.grid(axis="y", visible=False)
        site_axis(ax, sites)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.01, 0.95), ncol=2)
    suptitle(fig, "Mid-century (2040–2069) % change shown in the app, current vs delta change method")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(FIGS / "fig1_change_current_vs_delta.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_pp_heatmap(comp, sites):
    """Percentage-point shift in the displayed % change, sites x component.

    The additive adjustment is the same in every era, so the shift is too (to within
    0.02 pp from the zero floor); mid-century is shown.
    """
    m = comp[comp.era == "2040-2069"].pivot(index="site", columns="component", values="pp_shift").reindex(index=sites, columns=COMPS)
    fig, ax = plt.subplots(figsize=(6.4, 8.6))
    lim = 8
    im = ax.imshow(m.values, cmap=DIVERGING, norm=TwoSlopeNorm(0, -lim, lim), aspect="auto")
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            v = m.values[i, j]
            ax.text(j, i, f"{v:+.1f}", ha="center", va="center", fontsize=8.5, color="white" if abs(v) > 0.6 * lim else INK)
    ax.set_xticks(range(len(COMPS)))
    ax.set_xticklabels([LABELS[c] for c in COMPS], rotation=20, ha="right")
    ax.grid(False)
    site_axis(ax, sites)
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, shrink=0.6, pad=0.03, extend="both")
    cb.set_label("percentage points (delta − current)")
    cb.outline.set_visible(False)
    ax.set_title("Shift in the displayed % change\n(same in every era: the additive adjustment is constant)", loc="left")
    fig.tight_layout()
    fig.savefig(FIGS / "fig2_pp_shift_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_gcm_bias(long, gcm_hist, sites):
    """Each GCM's 1980-2009 mean vs Daymet: the offset the delta method removes."""
    fig, axes = plt.subplots(1, 3, figsize=(13, 7.4), sharey=True)
    rng = np.random.default_rng(0)
    for ax, c in zip(axes, COMPS):
        base = long[long.component == c].groupby("site").baseline.first()
        for i, s in enumerate(sites):
            h = np.array(gcm_hist[c][s])
            pct = 100 * (h - base[s]) / base[s]
            ax.scatter(pct, i + rng.uniform(-0.18, 0.18, pct.size), s=12, color=CURRENT, alpha=0.55, lw=0)
            ax.scatter(pct.mean(), i, s=60, marker="|", color=INK, lw=2)
        ax.axvline(0, color=INK2, lw=0.8)
        ax.set_title(LABELS[c], loc="left")
        ax.set_xlabel("GCM 1980–2009 mean vs Daymet (%)")
        ax.grid(axis="y", visible=False)
        site_axis(ax, sites)
    suptitle(fig, "GCM historical bias vs the Daymet baseline: what the delta method removes\n"
                  "dots = 9 GCMs × 2 RCP tracks; bar = ensemble mean", y=1.02)
    fig.tight_layout()
    fig.savefig(FIGS / "fig3_gcm_historical_bias.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_ratios():
    r = pd.read_csv(DATA / "ratios.csv")
    fig, axes = plt.subplots(1, 3, figsize=(14, 3.7))
    for ax, c in zip(axes, COMPS):
        v = r[r.component == c].ratio
        ax.hist(v, bins=np.arange(0, 3.6, 0.05), color=CURRENT, edgecolor=SURFACE, lw=0.3)
        top = ax.get_ylim()[1]
        for cap, ls in [(1.5, ":"), (2, "--"), (3, "-")]:
            ax.axvline(cap, color=DELTA, lw=1.2, ls=ls)
            ax.text(cap, top * 0.97, f" {cap:g}×\n {100 * (v > cap).mean():.2f}%", color=INK2, fontsize=8, va="top")
        ax.set_title(LABELS[c], loc="left")
        ax.set_xlabel("annual G_future / G_hist")
        ax.set_xlim(0, 3.6)
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("model-years (all sites, all eras)")
    suptitle(fig, "If treated multiplicatively: how often would a cap bind?  (% of model-years above each cap)", y=1.03)
    fig.tight_layout()
    fig.savefig(FIGS / "fig4_ratio_caps.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def read_map(name):
    with rio.open(DATA / "maps" / f"{name}.tif") as src:
        a = src.read(1).astype(float)
        b = src.bounds
    a[(a < -9000) | ~np.isfinite(a)] = np.nan
    # the NCAR 12km coverages come back with X/Y transposed (rows are X)
    return a.T, (b.left, b.right, b.bottom, b.top)


def fig_maps():
    xs, ys = zip(*[to_3338(s[2], s[3]) for s in SITES])
    fig, axes = plt.subplots(1, 3, figsize=(16, 5.4))
    lim = 15
    for ax, c in zip(axes, COMPS):
        adj, ext = read_map(f"{c}_adjustment")
        base, _ = read_map(f"{c}_baseline")
        with np.errstate(invalid="ignore", divide="ignore"):
            pct = 100 * adj / base
        pct[base < 100] = np.nan  # near-zero baselines (e.g. freezing index on the far south coast)
        im = ax.imshow(pct, extent=ext, cmap=DIVERGING, norm=TwoSlopeNorm(0, -lim, lim), interpolation="nearest")
        ax.scatter(xs, ys, s=10, color=INK, lw=0)
        ax.set_xlim(-1.0e6, 1.55e6)
        ax.set_ylim(0.35e6, 2.45e6)
        ax.set_aspect("equal")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.grid(False)
        for s in ax.spines.values():
            s.set_visible(False)
        ax.set_title(f"{LABELS[c]}\nadjustment, % of Daymet baseline", loc="left")
        cb = fig.colorbar(im, ax=ax, shrink=0.75, pad=0.01, extend="both")
        cb.outline.set_visible(False)
        cb.set_label("%")
    suptitle(fig, "Statewide delta-method adjustment (Daymet − GCM-ensemble 1980–2009), "
                  "each computed server-side in one WCPS request (dots = test sites)")
    fig.tight_layout()
    fig.savefig(FIGS / "fig5_wcps_statewide_maps.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_mmm_example(long):
    """Min/mean/max the app shows for the freezing index, current vs delta, for a few sites."""
    sites = ["Utqiagvik", "Fairbanks", "Bethel", "Anchorage", "Kodiak", "Juneau"]
    d = long[(long.component == "freezing_index") & (long.method.isin(["current", "delta"]))]
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=True)
    for ax, era in zip(axes, ERAS):
        for method, color, off, label in [("current", CURRENT, -0.13, "Current min–max"), ("delta", DELTA, 0.13, "Delta method min–max")]:
            e = d[(d.era == era) & (d.method == method)].pivot(index="site", columns="stat", values="value").reindex(sites)
            yy = np.arange(len(sites)) + off
            ax.hlines(yy, e["min"], e["max"], color=color, lw=2.5, alpha=0.9, label=label)
            ax.scatter(e["mean"], yy, color=color, s=40, zorder=3, edgecolor=SURFACE, lw=1.2)
        base = d.groupby("site").baseline.first().reindex(sites)
        ax.scatter(base, np.arange(len(sites)), marker="|", s=160, color=INK, lw=1.5, label="Daymet 1980–2009 mean", zorder=4)
        ax.set_title(era, loc="left")
        ax.set_xlabel("°F·days")
        ax.set_yticks(range(len(sites)))
        ax.set_yticklabels(sites)
        ax.set_ylim(len(sites) - 0.5, -0.5)
        ax.grid(axis="y", visible=False)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.01, 0.92), ncol=3)
    suptitle(fig, "Freezing index: the min / mean / max the app reports, current vs delta method (dots = mean)")
    fig.tight_layout(rect=(0, 0, 1, 0.84))
    fig.savefig(FIGS / "fig6_freezing_index_mmm.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    for old in FIGS.glob("*.png"):
        old.unlink()
    comp = pd.read_csv(DATA / "comparison.csv")
    long = pd.read_csv(DATA / "summary_long.csv")
    gcm_hist = json.loads((DATA / "extras.json").read_text())["gcm_hist"]
    sites = [s[0] for s in SITES if s[0] in set(comp.site)]
    fig_dumbbells(comp, sites)
    fig_pp_heatmap(comp, sites)
    fig_gcm_bias(long, gcm_hist, sites)
    fig_ratios()
    fig_maps()
    fig_mmm_example(long)
    print("figures written to", FIGS)
