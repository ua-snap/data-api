"""
Snow-17 accumulation and ablation model for a single point.

Ported from the UW-Hydro tonic implementation:
https://github.com/UW-Hydro/tonic/blob/master/tonic/models/snow17/snow17.py
which is based on Anderson (2006) and Mark Raleigh's MATLAB code.

Primary citations:
 1. Anderson, E. A. (1973), National Weather Service River Forecast System
    Snow Accumulation and Ablation Model, NOAA Tech. Memo. NWS HYDRO-17,
    217 pp., U.S. Dep. of Commer., Silver Spring, Md.
 2. Anderson, E. A. (1976), A point energy and mass balance model of a snow
    cover, NOAA Tech. Rep. 19, 150 pp., U.S. Dep. of Commer., Silver Spring, Md.

Differences from the tonic version:
 - Fixes the `ait(...)` call in the "not yet ripe" branch, which raises a
   TypeError in tonic. It is meant to be a logical `and`.
 - Clears liquid water held in the pack (w_q) when the ice portion melts out
   completely, so that water isn't double-counted in a later season.
 - Accepts and returns the model state, so a run can continue from where a
   previous run left off (e.g. a future scenario seeded from the historical run).
 - The seasonal melt factor is precomputed for all time steps, and scalar math
   uses the `math` module rather than numpy, for speed.
"""

import math
import numpy as np

# Default parameters, from the tonic implementation. Most of these are calibrated
# for the American River Basin (Shamir & Georgakakos 2007), not Alaska.
DEFAULT_PARAMS = {
    "scf": 1.0,  # gauge under-catch snow correction factor
    "rvs": 1,  # rain vs. snow option: 0 = single threshold, 1 = linear transition, 2 = all snow
    "uadj": 0.04,  # average wind function during rain on snow (mm/mb)
    "mbase": 1.0,  # base temperature above which melt occurs (deg C)
    "mfmax": 1.05,  # maximum melt factor, June 21 (mm/deg C/6 hr)
    "mfmin": 0.6,  # minimum melt factor, December 21 (mm/deg C/6 hr)
    "tipm": 0.1,  # antecedent temperature index parameter (0-1)
    "nmf": 0.15,  # maximum negative melt factor (mm/deg C/6 hr)
    "plwhc": 0.04,  # percent liquid water holding capacity of the pack (max 0.4)
    "pxtemp": 1.0,  # rain/snow threshold for rvs=0 (deg C)
    "pxtemp1": -1.0,  # all snow at or below this temperature for rvs=1 (deg C)
    "pxtemp2": 3.0,  # all rain at or above this temperature for rvs=1 (deg C)
}

# Model state at the start of a cold-start run
INITIAL_STATE = {
    "ait": 0.0,  # antecedent temperature index (deg C)
    "w_q": 0.0,  # liquid water held by the snow (mm)
    "w_i": 0.0,  # water equivalent of the ice portion of the snow cover (mm)
    "deficit": 0.0,  # heat deficit, a.k.a. negative heat storage (mm)
}


def melt_factors(day_of_year, dt, lat, mfmax, mfmin):
    """
    Seasonal variation of the non-rain melt factor for each time step.

    Args:
        day_of_year (array-like): day of year (1-366) for each time step
        dt (float): time step in hours
        lat (float): latitude of the simulation point
        mfmax (float): maximum melt factor (June 21)
        mfmin (float): minimum melt factor (December 21)

    Returns:
        numpy.ndarray: melt factor for each time step
    """
    jday = np.asarray(day_of_year, dtype=float)
    n_mar21 = jday - 80
    sv = (0.5 * np.sin((n_mar21 * 2 * np.pi) / 365)) + 0.5

    if lat < 54:
        # av = 1.0 when lat < 54 deg N
        av = np.ones_like(jday)
    else:
        # av = 0.0 from September 24 to March 18, 1.0 from April 27 to August 15,
        # and varies linearly between 0.0 and 1.0 from 3/19-4/26 and 8/16-9/23.
        av = np.interp(
            jday, [77, 78, 116, 117, 227, 228, 266, 267], [0, 0, 1, 1, 1, 1, 0, 0]
        )

    return (dt / 6) * ((sv * av * (mfmax - mfmin)) + mfmin)


def snow17(
    day_of_year,
    prec,
    tair,
    lat=50,
    elevation=0,
    dt=24,
    state=None,
    scf=1.0,
    rvs=1,
    uadj=0.04,
    mbase=1.0,
    mfmax=1.05,
    mfmin=0.6,
    tipm=0.1,
    nmf=0.15,
    plwhc=0.04,
    pxtemp=1.0,
    pxtemp1=-1.0,
    pxtemp2=3.0,
):
    """
    Run Snow-17 at a point. Precipitation and temperature time steps must match.

    Args:
        day_of_year (array-like): day of year (1-366) for each time step
        prec (array-like): precipitation for each time step (mm)
        tair (array-like): mean air temperature for each time step (deg C)
        lat (float): latitude of the simulation point
        elevation (float): elevation of the simulation point (m)
        dt (float): time step in hours
        state (dict): model state to start from, defaults to INITIAL_STATE
        remaining args: model parameters, see DEFAULT_PARAMS

    Returns:
        tuple: (swe, outflow, state) where swe and outflow are numpy arrays (mm)
        and state is the model state after the last time step
    """
    prec = np.asarray(prec, dtype=float)
    tair = np.asarray(tair, dtype=float)
    if not len(day_of_year) == len(prec) == len(tair):
        raise ValueError("day_of_year, prec, and tair must be the same length")
    if rvs not in (0, 1, 2):
        raise ValueError("Invalid rain vs snow option")

    if state is None:
        state = INITIAL_STATE
    ait = state["ait"]
    w_q = state["w_q"]
    w_i = state["w_i"]
    deficit = state["deficit"]

    nsteps = len(prec)
    model_swe = np.zeros(nsteps)
    outflow = np.zeros(nsteps)

    # Stefan-Boltzman constant (mm/K/hr)
    stefan = 6.12 * (10 ** (-10))
    # atmospheric pressure (mb) where elevation is in HUNDREDS of meters
    # (this is incorrectly stated in the manual)
    p_atm = 33.86 * (
        29.9 - (0.335 * elevation / 100) + (0.00022 * ((elevation / 100) ** 2.4))
    )

    tipm_dt = 1.0 - ((1.0 - tipm) ** (dt / 6))
    mfs = melt_factors(day_of_year, dt, lat, mfmax, mfmin).tolist()

    for i, (precip, t_air_mean) in enumerate(zip(prec.tolist(), tair.tolist())):
        mf = mfs[i]

        # Divide rain and snow
        if rvs == 0:
            fracsnow = 1.0 if t_air_mean <= pxtemp else 0.0
        elif rvs == 1:
            if t_air_mean <= pxtemp1:
                fracsnow = 1.0
            elif t_air_mean >= pxtemp2:
                fracsnow = 0.0
            else:
                fracsnow = (pxtemp2 - t_air_mean) / (pxtemp2 - pxtemp1)
        else:
            fracsnow = 1.0
        fracrain = 1.0 - fracsnow

        # Snow accumulation
        # water equivalent of new snowfall (mm)
        pn = precip * fracsnow * scf
        w_i += pn
        e = 0.0
        # amount of precip (mm) that is rain during this time step
        rain = fracrain * precip

        # Temperature and heat deficit from new snow
        if t_air_mean < 0.0:
            t_snow_new = t_air_mean
            # change in the heat deficit due to snowfall (mm)
            delta_hd_snow = -(t_snow_new * pn) / (80 / 0.5)
            t_rain = pxtemp
        else:
            t_snow_new = 0.0
            delta_hd_snow = 0.0
            t_rain = t_air_mean

        # Antecedent temperature index
        if pn > (1.5 * dt):
            ait = t_snow_new
        else:
            ait = ait + tipm_dt * (t_air_mean - ait)
        if ait > 0:
            ait = 0

        # Heat exchange when no surface melt
        # change in heat deficit due to a temperature gradient (mm)
        delta_hd_t = nmf * (dt / 6.0) * (mf / mfmax) * (ait - t_snow_new)

        # Rain-on-snow melt
        # saturated vapor pressure at t_air_mean (mb)
        e_sat = 2.7489 * (10**8) * math.exp(-4278.63 / (t_air_mean + 242.792))
        # 1.5 mm / 6 hrs
        if rain > (0.25 * dt):
            m_ros1 = max(stefan * dt * (((t_air_mean + 273) ** 4) - (273**4)), 0.0)
            m_ros2 = max(0.0125 * rain * t_rain, 0.0)
            m_ros3 = max(
                8.5
                * uadj
                * (dt / 6.0)
                * (((0.9 * e_sat) - 6.11) + (0.00057 * p_atm * t_air_mean)),
                0.0,
            )
            m_ros = m_ros1 + m_ros2 + m_ros3
        else:
            m_ros = 0.0

        # Non-rain melt
        if rain <= (0.25 * dt) and (t_air_mean > mbase):
            m_nr = (mf * (t_air_mean - mbase)) + (0.0125 * rain * t_rain)
        else:
            m_nr = 0.0

        # Ripeness of the snow cover
        melt = m_ros + m_nr
        if melt <= 0:
            melt = 0.0

        if melt < w_i:
            w_i = w_i - melt
        else:
            melt = w_i + w_q
            w_i = 0.0
            w_q = 0.0

        # liquid water available melted/rained at the snow surface (mm)
        qw = melt + rain
        # liquid water capacity (mm)
        w_qx = plwhc * w_i
        deficit += delta_hd_snow + delta_hd_t

        # limits of heat deficit
        if deficit < 0:
            deficit = 0.0
        elif deficit > 0.33 * w_i:
            deficit = 0.33 * w_i

        # Snow cover is ripe when both (deficit=0) & (w_q = w_qx)
        if w_i > 0.0:
            if (qw + w_q) > ((deficit * (1 + plwhc)) + w_qx):
                # the snow is ripe; excess liquid water leaves the pack (mm)
                e = qw + w_q - w_qx - (deficit * (1 + plwhc))
                w_q = w_qx
                # w_i increases because water refreezes as heat deficit is decreased
                w_i = w_i + deficit
                deficit = 0.0
            elif qw >= deficit:
                # the snow is not yet ripe, but ice is being melted
                e = 0.0
                w_q = w_q + qw - deficit
                w_i = w_i + deficit
                deficit = 0.0
            else:
                # the snow is not yet ripe
                e = 0.0
                w_i = w_i + qw
                deficit = deficit - qw
            swe = w_i + w_q
        else:
            e = qw
            swe = 0.0

        if deficit == 0:
            ait = 0

        model_swe[i] = swe
        outflow[i] = e

    final_state = {"ait": ait, "w_q": w_q, "w_i": w_i, "deficit": deficit}
    return model_swe, outflow, final_state
