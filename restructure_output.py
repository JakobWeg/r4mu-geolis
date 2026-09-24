"""Consolidates a completed run_de.py result directory's per-Gemeinde raw
output (gemeinden/<ags>/*.parquet + vehicle_profiles/<ags>.parquet) into the
eGon-data target format (see 2026-08-20_eMob_M1+N1_Ergebnisformat.xlsx,
sheet "Zielformat"). That spec's own relational design is ALREADY
deduplicated at the profile level: its "ev_pool.ev_id" identifies a distinct
SimBEV driving profile, not a specific sampled vehicle instance - the same
ev_id is meant to recur across several ags in ev_mapping_ev_municipality
(m:n, "jedes EV kann ... mehrfach in ein oder mehreren Gemeinden vorkommen")
and across several locations in ev_mapping_event_location (same reasoning).
This lines up exactly with our own pool-profile dedup (sampling draws with
replacement from a shared pool, so the same profile file gets reused ~65x on
average nationwide - storing its events once per vehicle that happened to
draw it wasted ~68x the necessary space).

  ev_pool                    - one row per DISTINCT SimBEV profile (ev_id,
                                rs7_id, type)
  ev_event                   - one row per distinct profile's timestep
                                (event_id, ev_id, ...target-format columns);
                                many sampled vehicles can share one ev_id, so
                                this is written once per profile, not once
                                per vehicle
  ev_charging_location       - one row per charging location (GeoParquet)
  ev_mapping_ev_municipality - one row per SAMPLED VEHICLE INSTANCE: which
                                Gemeinde drew this ev_id (profile), and how
                                often - the spec has no separate vehicle-
                                instance id, a row IS the instance.
  ev_mapping_event_location  - which location each vehicle instance's
                                charging event resolved to (event_id ->
                                location_id, plus the location's use_case per
                                spec) - the same shared profile event, used by
                                different vehicle instances, can resolve to a
                                different real-world location per instance
                                since placement depends on the instance's own
                                Gemeinde's candidate sites, not on the profile
                                data itself
  ev_count_municipality      - sampled vehicle count per Gemeinde x car type

This is pure post-processing over already-computed run_de.py output - it does
NOT re-run the simulation, so it can rebuild the nationwide result even after
a crash during the old (single-shot, memory-heavy) merge_results() step.

Usage:
    python restructure_output.py --result_dir results/_DE_26-08-25_122428 \
        --vehicle_input_file scenario/input_vehicles_2024.xlsx --out_dir results/normalized
"""
import argparse
import configparser as cp
import ctypes
import ctypes.wintypes
import datetime
import glob
import json
import pathlib
import shutil
import traceback
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import location_registry as lr
import run_de
import vehicle_input as vi


class _ProcessMemoryCountersEx(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.wintypes.DWORD),
        ("PageFaultCount", ctypes.wintypes.DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]


_KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
_PSAPI = ctypes.WinDLL("psapi", use_last_error=True)
_KERNEL32.GetCurrentProcess.restype = ctypes.wintypes.HANDLE
_KERNEL32.GetCurrentProcess.argtypes = []
_PSAPI.GetProcessMemoryInfo.restype = ctypes.wintypes.BOOL
_PSAPI.GetProcessMemoryInfo.argtypes = [ctypes.wintypes.HANDLE, ctypes.POINTER(_ProcessMemoryCountersEx), ctypes.wintypes.DWORD]


def _self_memory_mb():
    """This process's own working-set/private-bytes, in MB, straight from
    the Windows API (GetProcessMemoryInfo on GetCurrentProcess()) - added
    2026-09-14 after Get-Process-based external monitoring reported an
    obviously-wrong flat ~11MB for this exact process while it was actively
    consolidating (confirmed independently: system-wide free memory in the
    same external trace was equally flat, which a real multi-GB-scale
    consolidation run should visibly move) - the same class of unreliable
    external-tool resource reporting this session already found for CPU
    time in this sandboxed environment, now apparently extending to memory
    too. Querying from INSIDE the process being measured sidesteps whatever
    layer intercepts/misreports external queries.

    WinDLL(..., use_last_error=True) plus explicit argtypes/restype on both
    calls, not the bare ctypes.windll.* shorthand - confirmed necessary:
    without them the call returns FALSE on this 64-bit build (ctypes'
    default int argument/return marshaling silently mismatches the real
    HANDLE/BOOL/DWORD signatures), even though GetCurrentProcess() itself
    still returns a plausible-looking value either way.

    Returns (None, None) if the API call itself fails, rather than raising -
    this is diagnostic logging, never worth failing an otherwise-successful
    consolidation run over."""
    try:
        counters = _ProcessMemoryCountersEx()
        counters.cb = ctypes.sizeof(_ProcessMemoryCountersEx)
        handle = _KERNEL32.GetCurrentProcess()
        ok = _PSAPI.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
        if not ok:
            return None, None
        return counters.WorkingSetSize / (1024 * 1024), counters.PrivateUsage / (1024 * 1024)
    except Exception:
        return None, None


# Official BBSR RegioStaR7 codes.
RS7_CODE = {
    "SR_Metro": 71,
    "SR_Gross": 72,
    "SR_Mitte": 73,
    "SR_Klein": 74,
    "LR_Zentr": 75,
    "LR_Mitte": 76,
    "LR_Klein": 77,
}

# run_de.py's own internal use-case dispatch names (also the raw per-Gemeinde
# output filenames, e.g. output_hpc_urban_charging-locations.parquet) don't
# all match the spec Excel's own charging_use_case vocabulary (sheet
# "ev_event": "depot, home_detached, home_apartment, urban_fast,
# highway_fast, street, retail, work") - confirmed the spec Excel defines NO
# use_case column at all for ev_charging_location, so this pipeline's own
# use_case column there (added purely to disambiguate location_id, which is
# only unique WITHIN one use case's own raw output file - see below) had
# drifted to its own internal names instead. Translated here, at the one
# place this column's value is derived, rather than renaming run_de.py's
# internal dispatch/filenames themselves - those are just this pipeline's
# own raw intermediate artifacts, not part of the target format.
USE_CASE_SPEC_NAMES = {
    "hpc_urban": "urban_fast",
    "hpc_highway": "highway_fast",
    "public": "street",
}

# Raw SimBEV pool CSV columns as they appear in vp (vehicle_input.py's
# load_and_tag_profile() renames the source CSV's own "energy_grid" to
# "energy" for use_case.py's placement logic - see COLUMNS_OUTPUT_
# CHARGINGEVENTS in run_de.py, so that's the name it has here too), trimmed
# to only what's actually needed: either read for a derivation below, or a
# spec target column carried straight through. timestamp/use_case were kept
# as non-spec extras at one point but have no other reader anywhere in the
# pipeline - dropped per request to only keep what the target-format spec
# Excel actually asks for, since they added real nationwide-scale storage
# (ev_event has tens of millions of rows) for zero functional benefit.
# "energy" (SimBEV's raw grid-side energy_grid) WAS dropped for the same
# reason, but is read again as of chargingdemand_grid_kWh below - the
# report/table mismatch it caused (see build_evaluation_report.py) was worse
# than the extra storage.

# Every vp column process_gemeinde actually touches, at either full-vp scale
# (vehicle_id/car_type/source_file/location_id/location_use_case/
# pos_in_profile - see their vp[...]/vp.loc[...] usages below) or only
# within the small per-Gemeinde "sub" slice fed into EV_EVENT_RAW_COLUMNS.
# vp's own "timestamp"/"use_case" columns are the same dead weight
# EV_EVENT_RAW_COLUMNS' own comment already documents (no reader anywhere in
# the pipeline); "Type" likewise has no vp["Type"] reader anywhere in this
# file. Passed to read_parquet's columns= below to skip reading AND casting
# them at all - for a megacity's vp (tens of millions of rows) this is a
# real, measured ~30% cut of the read+string-cast cost (confirmed via
# benchmark against a 60M-row real vp file: 31.0s -> 21.1s), for zero
# behavior change, since nothing downstream ever reads these 3 columns.
VP_NEEDED_COLUMNS = [
    "vehicle_id", "car_type", "source_file", "location_id", "location_use_case", "pos_in_profile",
    "event_start", "event_time", "location", "charging_use_case", "soc_start", "soc_end",
    "energy_battery", "energy", "station_charging_capacity", "average_charging_power",
]

EV_EVENT_RAW_COLUMNS = [
    "event_start", "event_time", "location",
    "charging_use_case", "soc_start", "soc_end", "energy_battery", "energy",
    "station_charging_capacity", "average_charging_power",
]

# Target-format columns (spec Excel, sheet "ev_event") derived from the raw
# SimBEV columns above - see _derive_target_event_columns(). Kept in the
# order the spec lists them (event_id/ev_id are prepended separately), except:
# - grid_charging_power_avg_kW/battery_charging_power_avg_kW: the spec
#   Excel's own column here is "battery_charging_capacity_kW" fed straight
#   from raw average_charging_power, but that raw column is actually
#   grid-side (see _derive_target_event_columns' docstring) - renamed and
#   split into an explicit grid/battery pair instead of keeping a
#   battery-labeled column that holds a grid-side value.
# - grid_charging_capacity_kW: dropped entirely rather than kept as a spec-
#   mandated duplicate of nominal_charging_capacity_kW (same value, same
#   column - see that docstring note) - one column says what it is instead
#   of two saying the same thing under different names.
# - chargingdemand_kWh renamed to chargingdemand_battery_kWh, so its name
#   carries the same battery/grid distinction chargingdemand_grid_kWh's
#   name already does, instead of leaving the battery-side one unlabeled.
EV_EVENT_OUTPUT_COLUMNS = [
    "charging_use_case", "location",
    "nominal_charging_capacity_kW",
    "grid_charging_power_avg_kW", "battery_charging_power_avg_kW",
    "soc_start", "soc_end", "chargingdemand_battery_kWh", "chargingdemand_grid_kWh",
    "park_time_timesteps", "park_start_timesteps", "park_end_timesteps",
    "drive_start_timesteps", "drive_end_timesteps", "consumption_kWh",
]

FLOAT_PRECISION = 4  # spec Excel, sheet "ev_event": "Float precision: 4 digits"
EV_EVENT_FLOAT_COLUMNS = [
    "nominal_charging_capacity_kW",
    "grid_charging_power_avg_kW", "battery_charging_power_avg_kW",
    "soc_start", "soc_end", "chargingdemand_battery_kWh", "chargingdemand_grid_kWh", "consumption_kWh",
]
# One calendar year's timestep count (~35,040 for a 15-min-resolution year,
# ~35,136 in a leap year) - fixed by simulation resolution, NOT by how much
# vehicle demand a scenario year has, so uint16 (max 65,535) has ample
# permanent headroom regardless of how large 2037/2045 get. These were int64
# before, dictionary-encoded - dictionary already shrank them to a ~16-bit
# index (since there are ~35,000 distinct values), so a native uint16 mostly
# just drops the redundant dictionary indirection on top of that.
EV_EVENT_TIMESTEP_COLUMNS = [
    "park_time_timesteps", "park_start_timesteps", "park_end_timesteps",
    "drive_start_timesteps", "drive_end_timesteps",
]
# Every ev_event column's dtype, in output column order - used to build a
# correctly-typed EMPTY fallback frame (a Gemeinde with no genuinely new
# profiles this round contributes zero ev_event rows). pd.concat() silently
# promotes ALL columns to object dtype if even one frame in the batch has
# untyped empty columns (confirmed: a bare pd.DataFrame(columns=[...])
# concatenated against a properly-typed frame loses every dtype set below,
# object included) - since most Gemeinden reuse already-seen profiles rather
# than introducing new ones, an untyped empty frame turning up in a batch is
# the common case, not a rare edge case.
# Column ORDER here must match ["event_id", "ev_id"] + EV_EVENT_OUTPUT_COLUMNS
# exactly - this dict's keys become the empty fallback frame's column order,
# and pyarrow's ParquetWriter rejects a table whose column order doesn't
# match the file it's already writing to. EV_EVENT_FLOAT_COLUMNS/
# EV_EVENT_TIMESTEP_COLUMNS group columns by dtype, not by output position
# (consumption_kWh sits at the very end of the output but inside the float
# group), so building this dict by dtype-group first and reordering by name
# after (rather than trying to interleave the dtype groups by hand) is what
# keeps the two in sync as either list changes.
_EV_EVENT_DTYPE_BY_NAME = {
    "event_id": "int64", "ev_id": "int32",
    "charging_use_case": "object", "location": "object",
    **{c: "float32" for c in EV_EVENT_FLOAT_COLUMNS},
    **{c: "uint16" for c in EV_EVENT_TIMESTEP_COLUMNS},
}
EV_EVENT_DTYPES = {c: _EV_EVENT_DTYPE_BY_NAME[c] for c in ["event_id", "ev_id"] + EV_EVENT_OUTPUT_COLUMNS}

# event_id/id columns are pure sequential (or monotonic) surrogate keys -
# every value distinct, so dictionary/RLE encoding (parquet's default)
# can't exploit any repetition and falls back to storing them almost raw.
# DELTA_BINARY_PACKED is parquet's native encoding for exactly this case
# (monotonic integers) and shrinks them to near nothing - confirmed
# empirically on the old per-vehicle-duplicated design: event_id alone
# was 148MB/34.6M rows before this, ~0 after.
# ev_id/location_id are NOT sequential in these tables (each value
# repeats many times) so they stay dictionary-encoded, which suits them.
# NOTE: pyarrow requires use_dictionary to be an explicit allowlist (not True/"all")
# whenever column_encoding is set at all - so every column that should
# KEEP dictionary encoding must be listed by name, or it silently loses
# dictionary encoding too (confirmed: omitting the plain numeric columns
# here by mistake roughly DOUBLED their compressed size).
ROW_GROUP_SIZE = 1_000_000
# EV_EVENT_TIMESTEP_COLUMNS excluded from the dictionary allowlist: with
# ~35,000 distinct values each, dictionary encoding was already just a
# ~16-bit index (dictionary page + index) - almost exactly what the
# native uint16 they're now stored as gives directly, without paying for
# the dictionary page on top. Confirmed the biggest single share of
# ev_event.parquet's size (~48%, 457MB/956MB on the 2024 nationwide run).
EV_EVENT_ENCODING = dict(
    use_dictionary=[c for c in EV_EVENT_OUTPUT_COLUMNS if c not in EV_EVENT_TIMESTEP_COLUMNS] + ["ev_id"],
    column_encoding={"event_id": "DELTA_BINARY_PACKED"},
    version="2.6",
)
# event_id excluded here (unlike in EV_EVENT_ENCODING above): in THIS
# table it references the shared profile-level event, which recurs once
# per vehicle instance that placed it independently - confirmed via the
# 2024 nationwide run: ~56.6M distinct values across 454M rows needs a
# ~26-bit dictionary index, which costs almost as much (1.93GB) as a
# plain fixed-width column would. int32 (see ev_mapping_event_loc_rows -
# bounded by ev_event's own row count, which is capped by the SimBEV pool
# size, not by how much vehicle demand a scenario year has) stored PLAIN
# is smaller (~1.82GB) and simpler. location_id/use_case keep dictionary
# encoding - both already compress far below their naive dictionary-index
# estimate (natural clustering in Gemeinde-processing order helps RLE),
# so dropping it there would be a regression, not an improvement.
MAPPING_EVENT_LOC_ENCODING = dict(
    use_dictionary=["location_id", "use_case"],
    column_encoding={"id": "DELTA_BINARY_PACKED"},
    version="2.6",
)
# Flush explicitly-sized batches (many Gemeinden concatenated together)
# rather than calling write_table() once per Gemeinde. Letting pyarrow's
# ParquetWriter accumulate thousands of separate small write_table()
# calls on its own triggered an internal overflow ("Parquet cannot store
# strings with size 2GB or more", with a suspiciously uint64-wraparound-
# looking byte count) partway through a 10k+-Gemeinde run - some internal
# buffer/dictionary page was never being flushed cleanly across calls.
# Explicit, bounded batches sidestep that regardless of the exact cause,
# and as a side effect compress better (confirmed earlier: one big batch
# beats many tiny ones).
FLUSH_ROWS = 2_000_000


def _derive_target_event_columns(df: pd.DataFrame, eta_cp: float) -> pd.DataFrame:
    """Adds the eGon-data target-format columns on top of the raw SimBEV
    columns (df must already have EV_EVENT_RAW_COLUMNS).

    The spec Excel's own grid_charging_capacity_kW is a genuine duplicate of
    nominal_charging_capacity_kW ("=nominal_charging_capacity_kW") - dropped
    here rather than carried through as a second column holding the same
    value under a different name (see EV_EVENT_OUTPUT_COLUMNS' own comment).
    chargingdemand_battery_kWh/consumption_kWh are the sign-split of
    energy_battery (positive while charging, negative while driving) - the
    spec wants them as two separate always-non-negative columns instead.

    Raw SimBEV's average_charging_power is itself grid-side, not battery-side
    despite feeding what the spec Excel calls "battery_charging_capacity_kW"
    - confirmed two ways: it reaches exactly 100% of station_charging_capacity
    on ~4% of real charging rows (impossible if a <1 efficiency already
    applied), and it tracks energy/duration computed from the raw grid-side
    "energy" column ~19,000x more often than from battery-side energy_battery.
    grid_charging_power_avg_kW carries that raw value under its correct
    label; battery_charging_power_avg_kW derives the actual battery-side
    average by applying eta_cp - the same fixed efficiency factor confirmed
    on the energy side (energy_battery/energy == eta_cp, tightly, for every
    charging_use_case) - since SimBEV doesn't expose a real battery-side
    average power of its own to carry through instead.
    """
    out = df.copy()
    is_driving = (out["location"] == "driving").to_numpy()
    event_end = (out["event_start"] + out["event_time"]).to_numpy()
    energy_battery = out["energy_battery"].to_numpy()

    # SimBEV logs a nonzero energy_battery on the very first event of a
    # profile (event_start == 0) even when soc_start == soc_end - a
    # bookkeeping artifact for the notional energy behind the profile's
    # initial SoC, not a real charging session: no time-varying power flow
    # backs it, so both average_charging_power and the raw grid-side "energy"
    # column stay 0 on these rows regardless of energy_battery. Confirmed
    # across a 2037 sample: every single such row (station_charging_capacity
    # > 0, average_charging_power == 0, energy == 0, energy_battery > 0) has
    # event_start == 0 and soc_start == soc_end. Zeroed here so it can't
    # inflate chargingdemand_battery_kWh with a nonexistent charging event.
    is_initial_soc_artifact = (out["event_start"].to_numpy() == 0) & (out["soc_start"].to_numpy() == out["soc_end"].to_numpy())
    energy_battery = np.where(is_initial_soc_artifact, 0.0, energy_battery)

    out["nominal_charging_capacity_kW"] = out["station_charging_capacity"]
    out["grid_charging_power_avg_kW"] = out["average_charging_power"]
    out["battery_charging_power_avg_kW"] = out["average_charging_power"] * eta_cp
    # grid/battery_charging_power_avg_kW are already 0 on these rows (average_
    # charging_power == 0 is part of what identifies the artifact), but
    # nominal_charging_capacity_kW still carries the real station rating -
    # a nonzero capacity on a row with zero energy and no charging_use_case
    # would be self-contradictory. Zero it too so the row reads as the
    # non-event it is on every column, not just the energy ones.
    out.loc[is_initial_soc_artifact, "nominal_charging_capacity_kW"] = 0
    # A row can carry a real charging_use_case (e.g. "highway_fast") straight
    # from the raw SimBEV data while its own station_charging_capacity is 0 -
    # a genuine parking/park-only stop at that kind of site, or a session
    # clipped to nothing by some upstream edge case, not an actual charging
    # demand. Left as-is, that makes the row look like a charging event
    # needing a location (it's part of why vehicle_input.py's placement
    # input already filters on station_charging_capacity != 0 - such a row
    # is correctly never sent to the placement algorithm at all, so it can
    # never legitimately end up with a location_id). Null out
    # charging_use_case here too, matching genuine driving/non-charging rows,
    # so the exposed table doesn't misrepresent a 0 kW row as a real
    # "highway_fast" charging event with a use case but no location. The
    # initial-SoC artifact above gets the same treatment, since it now has
    # zero chargingdemand_battery_kWh too despite a nonzero
    # station_charging_capacity.
    out.loc[(out["nominal_charging_capacity_kW"] == 0) | is_initial_soc_artifact, "charging_use_case"] = np.nan
    out["chargingdemand_battery_kWh"] = np.clip(energy_battery, 0, None)
    out["consumption_kWh"] = np.clip(-energy_battery, 0, None)
    # "energy" (raw SimBEV energy_grid) is never negative - it's only ever
    # nonzero during an actual charging event, unlike energy_battery which
    # also carries driving consumption as negative values - so there's no
    # consumption-side counterpart to split out here.
    out["chargingdemand_grid_kWh"] = np.clip(out["energy"].to_numpy(), 0, None)
    out["park_time_timesteps"] = np.where(is_driving, 0, out["event_time"])
    out["park_start_timesteps"] = np.where(is_driving, 0, out["event_start"])
    out["park_end_timesteps"] = np.where(is_driving, 0, event_end)
    out["drive_start_timesteps"] = np.where(is_driving, out["event_start"], 0)
    out["drive_end_timesteps"] = np.where(is_driving, event_end, 0)
    out[EV_EVENT_FLOAT_COLUMNS] = out[EV_EVENT_FLOAT_COLUMNS].round(FLOAT_PRECISION)
    # float32 (spec wants 4 decimal digits - single precision has ~7
    # significant digits, comfortably enough) and uint16 (see
    # EV_EVENT_TIMESTEP_COLUMNS above) instead of the default float64/int64
    # pandas infers - halves/quarters the per-value width before compression
    # even gets a chance to run.
    out[EV_EVENT_FLOAT_COLUMNS] = out[EV_EVENT_FLOAT_COLUMNS].astype(np.float32)
    max_timestep = out[EV_EVENT_TIMESTEP_COLUMNS].to_numpy().max() if len(out) else 0
    assert max_timestep <= np.iinfo(np.uint16).max, (
        f"a timestep value ({max_timestep}) no longer fits uint16 - simulation_steps grew "
        f"beyond what EV_EVENT_TIMESTEP_COLUMNS was sized for, widen the dtype")
    out[EV_EVENT_TIMESTEP_COLUMNS] = out[EV_EVENT_TIMESTEP_COLUMNS].astype(np.uint16)
    return out[EV_EVENT_OUTPUT_COLUMNS]


def process_gemeinde(ags: str, gemeinde_dir: pathlib.Path, vehicle_profile_path: pathlib.Path,
                      rs7_code: int, offsets: dict, registry: "lr.LocationRegistry", profile_registry: dict,
                      eta_cp: float):
    """Build this Gemeinde's rows for all 6 tables, using and then advancing
    the shared global id offsets (one dict, mutated in place - this function
    must be called strictly sequentially, not from parallel workers, since
    the whole point is gapless global ids assigned in visitation order).

    registry: assigns location_id - via candidate_uid, not a fresh counter -
    so a location already placed in an earlier scenario year keeps the exact
    same id here (see location_registry.py); ids genuinely new to this
    scenario chain get new ids, continuing wherever the registry left off.

    profile_registry: source_file -> {"ev_id", "event_id_start"} - in-memory
    only (not persisted like the location registry - each scenario year has
    its own pool directory, so profile identity never needs to survive
    across years). A profile already seen (by an earlier vehicle in this
    Gemeinde, or any earlier Gemeinde in this run) is not re-written; only
    genuinely new ones get their ~1600 rows written to ev_event, and only
    genuinely new ones get an ev_pool row.
    """
    # dtype_backend="numpy_nullable", not pandas 3.x's Arrow-backed default -
    # the Arrow-backed string columns' compute kernels have been observed to
    # raise spurious ArrowMemoryError/ArrowException("Unknown error: Wrapping")
    # on arbitrary Gemeinden partway through a long consolidation run (looks
    # like Arrow memory-pool fragmentation after hundreds of prior Gemeinden
    # in the same process, not anything about the failing Gemeinde's own
    # data - a standalone rerun of the same file always succeeds). Plain
    # numpy-backed object/string arrays sidestep that compute kernel
    # entirely for every operation below, not just the one call site that
    # happened to be hit so far.
    vp = pd.read_parquet(vehicle_profile_path, columns=VP_NEEDED_COLUMNS, dtype_backend="numpy_nullable")
    # Explicit astype(object) for every string column - dtype_backend=
    # "numpy_nullable" above only affects NUMERIC/nullable columns;
    # pandas 3.x's global default string dtype (Arrow-backed "str") applies
    # to read_parquet's string output regardless of dtype_backend, so these
    # columns come back Arrow-backed anyway. Confirmed via py-spy that even
    # a bare .to_numpy(dtype=object) call on one of these columns was still
    # slow, landing in pandas/core/arrays/arrow/array.py's ChunkedArray.
    # to_numpy() - one level upstream of every other Arrow-map/isin fix in
    # this function. Casting once here, right after read, makes every later
    # operation on these columns (map, isin, to_numpy, string concatenation)
    # work on plain numpy object arrays instead of repeatedly paying this
    # cost (or worse, hitting the transient ArrowMemoryError this file's
    # other fixes exist for) throughout the rest of process_gemeinde.
    string_cols = vp.select_dtypes(include="string").columns
    if len(string_cols):
        vp = vp.astype({c: object for c in string_cols})

    # --- vehicles -> ev_pool / ev_event / ev_mapping_ev_municipality ---
    vehicles_local = vp[["vehicle_id", "car_type", "source_file"]].drop_duplicates("vehicle_id").reset_index(drop=True)
    n_vehicles = len(vehicles_local)

    # Resolve (or register) each vehicle's pool profile identity. Sampling
    # draws with replacement, so several vehicles - within this Gemeinde
    # and/or across earlier ones - can already share the same source_file.
    # This profile identity IS the target format's ev_pool.ev_id (see module
    # docstring) - the spec's own "ev" is a distinct SimBEV profile, not a
    # sampled vehicle instance.
    # int32: ev_id is bounded by the SimBEV pool's own fixed size (~7,200
    # profiles per RegioStaR7 type), not by how much vehicle demand a
    # scenario year samples - safe regardless of scenario-year scale, and
    # numpy raises OverflowError on assignment below if that bound is ever
    # wrong instead of silently wrapping.
    ev_ids = np.empty(n_vehicles, dtype=np.int32)
    new_source_files = []
    for i, sf in enumerate(vehicles_local["source_file"].to_numpy()):
        entry = profile_registry.get(sf)
        if entry is None:
            entry = {"ev_id": offsets["ev"], "event_id_start": None}
            offsets["ev"] += 1
            profile_registry[sf] = entry
            new_source_files.append(sf)
        ev_ids[i] = entry["ev_id"]
    vehicles_local["ev_id"] = ev_ids

    # Write each newly-seen profile's events, and its own ev_pool row,
    # exactly once - never once per vehicle that happened to draw it.
    event_frames = []
    # Explicit dtypes, not a bare pd.DataFrame(columns=[...]) - see
    # EV_EVENT_DTYPES's comment for why an untyped empty frame silently
    # corrupts every other Gemeinde's dtypes once concatenated together.
    ev_pool_rows = pd.DataFrame({
        "ev_id": pd.array([], dtype="int32"),
        "rs7_id": pd.array([], dtype="int8"),
        "type": pd.array([], dtype="object"),
    })
    if new_source_files:
        representative = vehicles_local.drop_duplicates("source_file").set_index("source_file")
        ev_pool_rows = pd.DataFrame({
            "ev_id": pd.array([profile_registry[sf]["ev_id"] for sf in new_source_files], dtype="int32"),
            # RS7_CODE is a fixed BBSR vocabulary (71-77) that will never
            # grow - int8 has permanent headroom, unlike the other ids above
            # whose safe width depends on scenario-year scale.
            "rs7_id": np.int8(rs7_code),
            "type": [representative.loc[sf, "car_type"] for sf in new_source_files],
        })
        # One grouped pass over vp instead of a fresh `vp.loc[vp["vehicle_id"]
        # == ...]` full-column scan per new profile - that was O(n_new_
        # profiles * n_rows_in_vp), which for a megacity (tens of thousands
        # of distinct sampled vehicles, 100M+ profile rows) took HOURS of
        # genuine (not stuck) computation - confirmed via faulthandler's
        # thread dump landing in pandas/pyarrow's boolean-mask take(). Filter
        # to only the representative vehicles actually needed before
        # grouping, since most of vp's vehicles aren't new profiles.
        representative_vehicle_ids = [representative.loc[sf, "vehicle_id"] for sf in new_source_files]
        # Plain-numpy mask, not vp["vehicle_id"].isin(...) directly - the
        # Arrow-backed boolean-mask/take path (pandas 3.x default string
        # dtype) has been observed to raise a spurious ArrowMemoryError
        # ("realloc of size <garbage huge number> failed") on some Gemeinden
        # for reasons unrelated to this Gemeinde's own data (a rerun of the
        # same file in isolation succeeds) - looks like Arrow memory-pool
        # fragmentation building up over many prior Gemeinden in this same
        # long-running process. .to_numpy(object) sidesteps the Arrow compute
        # kernel entirely for this filter.
        vehicle_id_np = vp["vehicle_id"].to_numpy(dtype=object)
        # Python-set membership via .__contains__, not np.isin(..., dtype=object)
        # directly - confirmed via py-spy the same slow generic path already
        # fixed at the "_key"/used_keys filter below (contains_object=True,
        # use_table_method=False - no hash/table fast path for object
        # arrays). representative_vehicle_ids is usually tiny per Gemeinde,
        # but for whichever Gemeinde happens to be the FIRST to see a large
        # fraction of the nationwide profile pool (a megacity processed
        # early in the alphabetically-sorted run) it can reach into the
        # thousands, against a vehicle_id_np of millions - exactly the size
        # combination that made this the dominant cost for 2037's Hamburg.
        # np.fromiter over a plain Python generator, not pd.Series(...).map()
        # - wrapping vehicle_id_np in a bare pd.Series() re-infers pandas
        # 3.x's default Arrow-backed "str" dtype even though vehicle_id_np
        # is already a plain object array, routing .map() back through the
        # same slow Arrow array path this whole fix exists to avoid
        # (confirmed via py-spy: this line was the new bottleneck for 2037's
        # Hamburg immediately after the first fix). fromiter over a
        # generator calling the set's own __contains__ never touches pandas
        # or Arrow at all.
        rep_id_set = set(representative_vehicle_ids)
        mask = np.fromiter((v in rep_id_set for v in vehicle_id_np), dtype=bool, count=vehicle_id_np.size)
        try:
            vp_by_vehicle = dict(iter(vp.loc[mask].groupby("vehicle_id", sort=False)))
            for sf in new_source_files:
                sub = vp_by_vehicle[representative.loc[sf, "vehicle_id"]].reset_index(drop=True)
                n = len(sub)
                start_id = offsets["event"]
                offsets["event"] += n
                profile_registry[sf]["event_id_start"] = start_id
                rows = _derive_target_event_columns(sub.reindex(columns=EV_EVENT_RAW_COLUMNS), eta_cp)
                rows.insert(0, "ev_id", np.int32(profile_registry[sf]["ev_id"]))
                rows.insert(0, "event_id", np.arange(start_id, start_id + n, dtype=np.int64))
                event_frames.append(rows)
        except Exception:
            # Roll back the provisional (event_id_start=None) entries just
            # added for this Gemeinde's new_source_files - if left in place,
            # a LATER Gemeinde that draws the same pool profile (sampling is
            # with replacement, so the same file is reused ~65x nationwide on
            # average) would see profile_registry.get(sf) return this broken
            # entry instead of None, silently skip re-registering it, and
            # carry a None event_id_start into its own event_id computation -
            # turning one Gemeinde's transient failure into a NaN that
            # eventually reaches the int32 cast below for unrelated
            # Gemeinden. Removing them lets the next Gemeinde that needs sf
            # register it cleanly instead.
            for sf in new_source_files:
                if profile_registry.get(sf, {}).get("event_id_start") is None:
                    del profile_registry[sf]
            raise
    ev_event_rows = (pd.concat(event_frames, ignore_index=True) if event_frames
                      else pd.DataFrame({c: pd.array([], dtype=dt) for c, dt in EV_EVENT_DTYPES.items()}))

    ev_mapping_muni_rows = pd.DataFrame({
        # This "id" is the sampled vehicle instance's own handle - the spec
        # has no separate instance id (a row here IS the instance).
        "id": np.arange(n_vehicles, dtype=np.int64) + offsets["mapping_muni"],
        "ev_id": vehicles_local["ev_id"],
        # AGS as int per the target format - the leading zero of Bundesland
        # codes 01-09 is dropped, same convention as displaying/storing AGS
        # as a plain integer anywhere else (re-zfill(8) if you need it back).
        # int32: Germany's AGS scheme is a fixed, permanently bounded 8-digit
        # code (max ~16 million as an int) - not scenario-year-scale
        # sensitive, safe indefinitely.
        "ags": np.int32(ags),
    })

    # --- locations -> ev_charging_location ---
    loc_frames = []
    for f in sorted(glob.glob(str(gemeinde_dir / "output_*_charging-locations.parquet"))):
        stem = pathlib.Path(f).stem
        # NOT translated to spec naming here - this value also builds
        # registry_keys below, which are persisted in location_registry.
        # parquet and must keep matching what earlier scenario years already
        # wrote there with this same internal name (see USE_CASE_SPEC_NAMES'
        # own comment). Translated only where it lands in the exposed output
        # columns further down.
        uc = stem[len("output_"):-len("_charging-locations")]
        g = gpd.read_parquet(f)
        if g.empty:
            continue
        g = g.drop(columns=["mode"], errors="ignore")
        g["use_case"] = uc
        loc_frames.append(g)

    key_to_global_loc = {}
    ev_charging_location_rows = None
    if loc_frames:
        locs_local = pd.concat(loc_frames, ignore_index=True)
        locs_local["location_id"] = locs_local["location_id"].astype("int64")
        # location_id is only unique WITHIN one use case's own output file -
        # e.g. depot and work both number their own locations starting from
        # 0, and their ranges overlap heavily. A raw location_id alone is
        # therefore ambiguous across use cases within one Gemeinde; every
        # lookup here is keyed by (use_case, location_id) instead - matching
        # vp's own "location_use_case" column (see run_de.py's
        # _write_vehicle_profiles) - to avoid silently attributing an event
        # placed by one use case to an unrelated location from another that
        # happens to reuse the same local number.
        locs_local["_key"] = locs_local["use_case"] + "_" + locs_local["location_id"].astype(str)

        # Drop locations no event ever actually resolved to THIS year (e.g.
        # BNetzA existing-infrastructure sites the weighted reuse pass never
        # drew - see the "long tail" discussion: real demand exists, but a
        # low-weight site can go untouched by chance) before assigning
        # global ids, so ids stay gapless over just the kept locations. A
        # location carried forward from an earlier scenario year but not
        # re-drawn by this year's own demand is deliberately dropped here
        # too, not kept on the strength of its carried-forward capacity
        # alone (tried that 2026-09-08, reverted per explicit product
        # decision: a scenario year's output should only ever list locations
        # that year's own simulated demand actually used - reliability of
        # reuse belongs in the PLACEMENT step preferring existing
        # infrastructure strongly enough that it actually gets redrawn, see
        # distribute_charging_events's Phase 1 and
        # distribute_charging_events_household_capped's existing-slot
        # preference, not in retroactively keeping unused locations here).
        has_vp_loc = vp["location_id"].notna()
        used_keys = set(
            vp.loc[has_vp_loc, "location_use_case"] + "_" +
            vp.loc[has_vp_loc, "location_id"].astype("int64").astype(str)
        )
        # np.fromiter over a plain Python generator, not .isin(used_keys) or
        # a bare "_key".map(used_keys.__contains__) - confirmed via py-spy
        # that .isin() against a large Python set of STRING keys dispatches
        # into numpy's generic object-dtype isin path (contains_object=True,
        # use_table_method=False - no fast hash/table path for object
        # arrays); a first attempt to fix that with Series.map() instead
        # hit the exact same problem from the other direction - "_key" is
        # built via string concatenation, which pandas 3.x defaults to its
        # Arrow-backed "str" dtype, and .map() on THAT routes through the
        # same slow Arrow array path. .to_numpy(dtype=object) first, then
        # fromiter over a generator calling the set's own __contains__,
        # never touches pandas or Arrow for this check at all. For a
        # megacity's vp (tens of millions of rows -> used_keys) this filter
        # was confirmed to cost 50+ minutes before either fix.
        key_np = locs_local["_key"].to_numpy(dtype=object)
        mask = np.fromiter((k in used_keys for k in key_np), dtype=bool, count=key_np.size)
        locs_local = locs_local[mask].reset_index(drop=True)

        if "candidate_uid" in locs_local.columns:
            # Keyed by (use_case, candidate_uid), not candidate_uid alone -
            # public() also draws candidates from the home_apartment layer
            # as public street-charging sites, so the exact same raw
            # candidate_uid can legitimately appear under two DIFFERENT
            # use cases (home_apartment charging AND public street charging
            # at/near the same building) - two distinct real deployments
            # that must get two distinct location_ids, not collapse onto
            # one shared registry entry (confirmed: they did, before this).
            registry_keys = locs_local["use_case"] + "::" + locs_local["candidate_uid"].astype(str)
            global_ids = registry.assign_ids(registry_keys)
        else:
            # Defensive fallback for output that predates candidate_uid -
            # ids then aren't consistent across scenario years for this data.
            n_locs = len(locs_local)
            global_ids = np.arange(n_locs, dtype=np.int64) + offsets["location"]
        # .to_numpy(dtype=object) before zip(), not a bare Series - the same
        # slow Arrow-backed __iter__ path (pandas 3.x default string dtype)
        # already fixed elsewhere in this function, just missed here. For a
        # big city's locs_local (thousands of locations) this dict-build via
        # plain Series iteration was confirmed via py-spy as the dominant
        # cost for an otherwise-ordinary-looking Gemeinde.
        key_to_global_loc = dict(zip(locs_local["_key"].to_numpy(dtype=object), global_ids))
        # Translated to the spec's charging_use_case vocabulary (see
        # USE_CASE_SPEC_NAMES) here and for locs_local["use_case"] just
        # below - both are the exposed, final output value at this point
        # (registry_keys above already used the untranslated names and is
        # done with them), so it's safe to rename now without touching
        # cross-scenario-year registry continuity.
        # Plain list comprehension over .to_numpy(object), not Series.map() -
        # same Arrow-map slowness as elsewhere in this function, though at
        # per-Gemeinde location count (not vp/event count) this one was
        # never the dominant cost - fixed anyway for consistency.
        spec_use_case = np.array(
            [USE_CASE_SPEC_NAMES.get(u, u) for u in locs_local["use_case"].to_numpy(dtype=object)]
        )
        key_to_use_case = dict(zip(locs_local["_key"].to_numpy(dtype=object), spec_use_case))
        locs_local["use_case"] = spec_use_case
        locs_local["location_id"] = global_ids
        locs_local = locs_local.drop(columns=["_key"])
        if "candidate_uid" not in locs_local.columns:
            locs_local["candidate_uid"] = pd.NA
        # candidate_uid: kept in the output (beyond the target format's own
        # location_id/charging_points/average_charging_capacity/geometry) so
        # a later scenario year in the same chain can look a location back
        # up - see location_registry.py / prepare_candidates().
        # is_synthetic_location: explicit, not just inferrable from
        # candidate_uid's naming convention - a location isn't a real
        # geocoded candidate (highway HPC site, retail parking, etc.) but a
        # stand-in placed at its Gemeinde's own centroid. Two prefixes exist
        # (see run_de.py): HPC_HIGHWAY_CENTER_PREFIX for the original
        # hpc_highway_use_gemeinde_center fallback, and the newer, broader
        # SYNTHETIC_CENTER_PREFIX used by every other use case's empty-
        # candidate-pool fallback (work, hpc_urban, retail, public,
        # public_home_street, depot, home). Checking only the first one
        # (as this line originally did) silently flagged is_synthetic_location
        # False for every location created by the second - confirmed: 18,574
        # synthetic_center_* locations mis-flagged in the 2024 delivery.
        candidate_uid_str = locs_local["candidate_uid"].astype(str)
        locs_local["is_synthetic_location"] = (
            candidate_uid_str.str.startswith(run_de.HPC_HIGHWAY_CENTER_PREFIX)
            | candidate_uid_str.str.startswith(run_de.SYNTHETIC_CENTER_PREFIX)
        )
        # int32 for both counts: charging_points (max 926 nationwide in 2024)
        # and average_charging_capacity (a kW rating, physically bounded by
        # real charger hardware, max 400 in 2024) - this whole table is only
        # ~43MB nationwide, so the saving is small in absolute terms, but
        # int32 still gives comfortable headroom over int16 in case either
        # grows across the scenario chain (existing locations can accumulate
        # more points/capacity release over release).
        locs_local["charging_points"] = locs_local["charging_points"].astype(np.int32)
        # round(), not a bare astype(int32) - a real BNetzA "Normalladeeinrichtung"
        # can legitimately average under 1 kW per point (e.g. 3.7 kW total
        # across 4 points), and int32's truncate-toward-zero silently turned
        # that into a nonsensical "0 kW" location with real charging_points -
        # confirmed for 3 locations in the 2024 delivery. Floor of 1 for any
        # location that has a point at all: use_case_helpers.py's Phase 1/2
        # reuse now bumps this value up whenever a higher-power event
        # actually gets matched there (see its own comment), so this is only
        # a display-rounding safety net for locations that never do.
        rounded_capacity = locs_local["average_charging_capacity"].round()
        locs_local["average_charging_capacity"] = np.where(
            locs_local["charging_points"] > 0, rounded_capacity.clip(lower=1), rounded_capacity
        ).astype(np.int32)
        ev_charging_location_rows = gpd.GeoDataFrame(
            locs_local[["location_id", "charging_points", "average_charging_capacity",
                        "use_case", "candidate_uid", "is_synthetic_location", "geometry"]],
            geometry="geometry", crs=locs_local.crs,
        )

    # --- ev_mapping_event_location: event_id -> location_id (+ use_case) ---
    # pos_in_profile (run_de.py's vehicle_input._load_raw_profile) is each
    # row's 0-based position within its OWN profile's original source CSV,
    # captured before any per-instance filtering - it's exactly that row's
    # offset into its profile's event_id range. This can no longer be
    # recovered via vp.groupby("vehicle_id").cumcount(): a vehicle instance
    # that isn't the first to draw its profile only carries its charging
    # rows in vp (see build_charging_events_for_gemeinde), so cumcount over
    # just that filtered subset would number them 0,1,2,... instead of their
    # true original positions, silently attaching the wrong event_id to
    # every non-representative instance's charging events.
    # Plain dict.get() over a numpy object array, not Series.map(dict) - the
    # same slow Arrow array map path already fixed twice above in this
    # function (pandas 3.x's default Arrow-backed string dtype has no fast
    # path for Series.map, dict or callable) - this one runs for EVERY
    # Gemeinde's full vp (not just ones with new source files), so it's
    # likely the single biggest of the three for a megacity's vp (millions
    # of rows).
    # Looks up directly against profile_registry itself, not a freshly-built
    # {sf: event_id_start} copy of it - that copy used to be rebuilt from
    # scratch (a full pass over EVERY registered profile nationwide so far,
    # tens of thousands and growing monotonically) on EVERY Gemeinde call,
    # even the tiniest rural village's. Confirmed via py-spy this became a
    # real, growing per-Gemeinde cost as the run progressed (profile_registry
    # only grows, never shrinks) - a plain nested .get() below does the same
    # per-row lookup with no such fixed per-call cost.
    _missing = {"event_id_start": np.nan}
    event_id_start_per_row = np.array(
        [profile_registry.get(sf, _missing)["event_id_start"] for sf in vp["source_file"].to_numpy(dtype=object)]
    )
    # int32: event_id here references the same value space as ev_event's own
    # event_id, which is bounded by the SimBEV pool's fixed size (see the
    # ev_ids comment above) - NOT by this table's own (much larger, demand-
    # scaled) row count. offsets["event"] is the actual running ceiling
    # across the whole run so far; assert instead of silently overflowing if
    # that bound assumption is ever wrong for some future pool.
    assert offsets["event"] <= np.iinfo(np.int32).max, (
        f"event_id ({offsets['event']}) no longer fits int32 - the SimBEV pool "
        f"grew far beyond its usual ~7,200-profiles-per-RegioStaR7-type size, widen the dtype")
    # Guard before the int32 cast, not after: a NaN here (source_file missing
    # from profile_registry, e.g. left over from an earlier Gemeinde's failed
    # registration) casts to int32 as silent garbage rather than raising, and
    # that garbage id has gone on to segfault the whole process downstream
    # when used as a row/array index - raise here instead so this one
    # Gemeinde is cleanly skipped by the caller's per-Gemeinde try/except.
    missing = pd.isna(event_id_start_per_row)
    if missing.any():
        bad_files = sorted(set(vp.loc[missing, "source_file"]))
        raise ValueError(
            f"{missing.sum()} row(s) have no registered event_id_start (source_file not "
            f"found in profile_registry): {bad_files[:5]}")
    event_id_per_row = (event_id_start_per_row + vp["pos_in_profile"].to_numpy()).astype(np.int32)

    # Same initial-SoC-artifact condition as _derive_target_event_columns
    # (event_start == 0 and soc_start == soc_end - see that function's own
    # comment): run_de.py's own placement input only filters on
    # station_charging_capacity != 0, which these rows pass (that's part of
    # how they were identified as an artifact in the first place), so vp can
    # carry a real location_id/location_use_case for one even though its
    # ev_event row now correctly has no charging_use_case and 0 demand.
    # Excluded here too, or the mapping table ends up with placement rows
    # for events the event table itself says never charged - confirmed
    # nationwide-scale-proportional counts before this exclusion (e.g. 4,039
    # of 3.8M rows in a 100-Gemeinde sample).
    is_artifact = (vp["event_start"].to_numpy() == 0) & (vp["soc_start"].to_numpy() == vp["soc_end"].to_numpy())
    has_loc = (vp["location_id"].notna() & vp["location_use_case"].notna()).to_numpy() & ~is_artifact
    if has_loc.any() and key_to_global_loc:
        row_keys_np = (
            vp.loc[has_loc, "location_use_case"].to_numpy(dtype=object) + "_" +
            vp.loc[has_loc, "location_id"].astype("int64").astype(str).to_numpy(dtype=object)
        )
        # dict.get() over the plain object array above, not Series.map(dict)
        # - same Arrow-map slowness as event_id_start_per_row above, and
        # potentially the largest of the three here since has_loc can cover
        # most of a megacity's vp (every successfully-placed event).
        mapped = np.array([key_to_global_loc.get(k, np.nan) for k in row_keys_np])
        mapped_use_case = np.array([key_to_use_case.get(k, np.nan) for k in row_keys_np], dtype=object)
        valid = ~pd.isna(mapped)
        n_mapped = int(valid.sum())
        ev_mapping_event_loc_rows = pd.DataFrame({
            "id": np.arange(n_mapped, dtype=np.int64) + offsets["mapping_event_loc"],
            "event_id": event_id_per_row[has_loc][valid],
            # int32: total location count grows with reused/new charging
            # sites, not with vehicle demand - stayed under 2M even for
            # 2024's full nationwide run, nowhere near int32's ~2.1 billion.
            "location_id": mapped[valid].astype(np.int32),
            "use_case": mapped_use_case[valid],
        })
    else:
        ev_mapping_event_loc_rows = pd.DataFrame(
            {"id": pd.array([], dtype="int64"), "event_id": pd.array([], dtype="int32"),
             "location_id": pd.array([], dtype="int32"), "use_case": pd.array([], dtype="object")}
        )

    offsets["mapping_muni"] += n_vehicles
    offsets["mapping_event_loc"] += len(ev_mapping_event_loc_rows)
    if ev_charging_location_rows is not None:
        offsets["location"] += len(ev_charging_location_rows)

    return ev_pool_rows, ev_mapping_muni_rows, ev_charging_location_rows, ev_event_rows, ev_mapping_event_loc_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--vehicle_input_file", default="scenario/input_vehicles_2024.xlsx")
    parser.add_argument("--out_dir", required=True)
    parser.add_argument("--limit", type=int, default=None, help="only process the first N Gemeinden (testing)")
    parser.add_argument("--ags", nargs="+", default=None, help="only process these specific AGS (testing)")
    parser.add_argument("--config_file", default="scenario/config_DE.cfg",
                         help="checked for [analysis] run_evaluation to decide whether to build evaluation_report.xlsx afterwards")
    parser.add_argument("--location_registry_path", default=None,
                         help="multi-year scenario chain (see location_registry.py) - same path across all years "
                              "in one chain, so location_id stays consistent; defaults to <out_dir>/../location_registry.parquet")
    parser.add_argument("--drop_candidate_uid", action="store_true",
                         help="omit candidate_uid from the final ev_charging_location.parquet - only safe for the "
                              "LAST scenario year in a chain (e.g. 2045 in 2024->2037->2045): every earlier year's "
                              "own candidate_uid is still read back by the NEXT year's run_de.py (see "
                              "location_registry.py's merge_previous_scenario_into_candidates) to carry existing "
                              "locations forward, so dropping it from an earlier year's output breaks that for "
                              "whichever later year would have needed it.")
    args = parser.parse_args()

    # Read early (rather than alongside the metadata-only config read further
    # down) since eta_cp feeds every Gemeinde's _derive_target_event_columns
    # call in the main loop below, not just the end-of-run metadata.
    _early_cfg = cp.ConfigParser()
    if pathlib.Path(args.config_file).is_file():
        _early_cfg.read(args.config_file)
    eta_cp = _early_cfg.getfloat("basic", "eta_cp", fallback=0.9) if _early_cfg.has_section("basic") else 0.9

    result_dir = pathlib.Path(args.result_dir)
    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    registry_path = pathlib.Path(args.location_registry_path) if args.location_registry_path else out_dir.parent / "location_registry.parquet"
    registry = lr.LocationRegistry(registry_path)

    demand_df = vi.load_municipality_demand(args.vehicle_input_file)
    ags_to_rs7 = dict(zip(demand_df["AGS"], demand_df["RegioStaR7"].map(RS7_CODE)))

    gemeinden_dir = result_dir / "gemeinden"
    vp_dir = result_dir / "vehicle_profiles"
    ags_list = sorted(p.name for p in gemeinden_dir.iterdir() if p.is_dir())
    if args.ags:
        ags_list = [a.zfill(8) for a in args.ags]
    elif args.limit:
        ags_list = ags_list[: args.limit]

    offsets = {"ev": 0, "event": 0, "mapping_muni": 0, "mapping_event_loc": 0, "location": 0}
    profile_registry = {}  # source_file -> {"ev_id", "event_id_start"} - see process_gemeinde()

    # ROW_GROUP_SIZE/EV_EVENT_ENCODING/MAPPING_EVENT_LOC_ENCODING/FLUSH_ROWS
    # are module-level constants (see their definitions above, right after
    # EV_EVENT_DTYPES) - hoisted out of this function so
    # restructure_output_chunked.py can reuse the exact same encoding/dtype
    # decisions without duplicating them. Purely a definition-site move, no
    # behavior change here.

    class Writer:
        def __init__(self, path, encoding):
            self.path, self.encoding, self.handle = path, encoding, None

        def flush(self, frames):
            if not frames:
                return
            table = pa.Table.from_pandas(pd.concat(frames, ignore_index=True), preserve_index=False)
            # pyarrow auto-promotes a string column to large_string once a
            # batch's own total string byte volume crosses its internal
            # threshold - a batch containing a megacity (Hamburg, München)
            # tips over that threshold while a later, smaller batch doesn't,
            # so leaving the type to per-batch inference makes the file's
            # fixed schema (set by whichever batch happens to be written
            # first) mismatch a later batch's inferred schema. Confirmed:
            # "Table schema does not match schema used to create file" the
            # first time a large batch was immediately followed by a small
            # one. Force large_string uniformly so every batch's schema is
            # identical regardless of which Gemeinden land in it.
            table = table.cast(pa.schema([
                f.with_type(pa.large_string()) if pa.types.is_string(f.type) else f
                for f in table.schema
            ]))
            if self.handle is None:
                self.handle = pq.ParquetWriter(self.path, table.schema, **self.encoding)
            self.handle.write_table(table, row_group_size=ROW_GROUP_SIZE)
            frames.clear()

        def close(self):
            if self.handle is not None:
                self.handle.close()

    ev_pool_batches, mapping_muni_batches, location_batches = [], [], []
    event_buffer, mapping_event_loc_buffer = [], []
    event_buffer_rows = mapping_event_loc_buffer_rows = 0
    event_writer = Writer(out_dir / "ev_event.parquet", EV_EVENT_ENCODING)
    mapping_event_loc_writer = Writer(out_dir / "ev_mapping_event_location.parquet", MAPPING_EVENT_LOC_ENCODING)

    print(f"--- consolidating {len(ags_list)} Gemeinden from {result_dir} ---")
    for i, ags in enumerate(ags_list, start=1):
        vp_path = vp_dir / f"{ags}.parquet"
        if not vp_path.is_file():
            continue
        rs7_code = ags_to_rs7.get(ags)
        if rs7_code is None:
            print(f"WARNING: no RegioStaR7 code for AGS {ags}, skipping")
            continue

        try:
            (ev_pool_rows, mapping_muni_rows, location_rows,
             ev_event_rows, mapping_event_loc_rows) = process_gemeinde(
                ags, gemeinden_dir / ags, vp_path, rs7_code, offsets, registry, profile_registry, eta_cp
            )
        except Exception:
            # offsets is only mutated at the very end of process_gemeinde(),
            # after all rows are built - so a failure here never leaves the
            # global id bookkeeping in a half-updated state, and one
            # Gemeinde's bad data can't take down the whole ~75-minute
            # nationwide consolidation.
            warnings.warn(f"Gemeinde {ags}: consolidation failed, skipping:\n{traceback.format_exc()}")
            continue

        ev_pool_batches.append(ev_pool_rows)
        mapping_muni_batches.append(mapping_muni_rows)
        if location_rows is not None:
            location_batches.append(location_rows)

        event_buffer.append(ev_event_rows)
        event_buffer_rows += len(ev_event_rows)
        if event_buffer_rows >= FLUSH_ROWS:
            event_writer.flush(event_buffer)
            event_buffer_rows = 0

        mapping_event_loc_buffer.append(mapping_event_loc_rows)
        mapping_event_loc_buffer_rows += len(mapping_event_loc_rows)
        if mapping_event_loc_buffer_rows >= FLUSH_ROWS:
            mapping_event_loc_writer.flush(mapping_event_loc_buffer)
            mapping_event_loc_buffer_rows = 0

        if i % 1000 == 0 or i == len(ags_list):
            ws_mb, priv_mb = _self_memory_mb()
            mem_str = f", mem_ws={ws_mb:.0f}MB, mem_priv={priv_mb:.0f}MB" if ws_mb is not None else ""
            print(f"\r--- {i}/{len(ags_list)} Gemeinden consolidated "
                  f"(distinct EVs={offsets['ev']}, events={offsets['event']}, "
                  f"vehicle instances={offsets['mapping_muni']}, locations={offsets['location']}{mem_str}) ---",
                  end="", flush=True)
            # Periodic, not just final: this run can take well over an hour,
            # and this session has seen worker processes die unexplained
            # mid-run - losing every newly-registered location_id with it
            # would silently break the next scenario year's id continuity.
            registry.save()

    print()
    event_writer.flush(event_buffer)
    mapping_event_loc_writer.flush(mapping_event_loc_buffer)
    event_writer.close()
    mapping_event_loc_writer.close()
    registry.save()

    ev_pool_all = pd.concat(ev_pool_batches, ignore_index=True)
    pq.write_table(pa.Table.from_pandas(ev_pool_all, preserve_index=False), out_dir / "ev_pool.parquet",
                    use_dictionary=["rs7_id", "type"],
                    column_encoding={"ev_id": "DELTA_BINARY_PACKED"}, version="2.6")

    mapping_muni_all = pd.concat(mapping_muni_batches, ignore_index=True)
    pq.write_table(pa.Table.from_pandas(mapping_muni_all, preserve_index=False), out_dir / "ev_mapping_ev_municipality.parquet",
                    use_dictionary=["ags", "ev_id"], column_encoding={"id": "DELTA_BINARY_PACKED"},
                    version="2.6")

    final_n_locations = None
    if location_batches:
        all_locations = gpd.GeoDataFrame(pd.concat(location_batches, ignore_index=True),
                                          geometry="geometry", crs=location_batches[0].crs)
        # Two Gemeinden's own candidate-preparation can legitimately both
        # match the same real-world candidate (e.g. a home_apartment
        # building whose nearby-street use gets picked up by two adjacent
        # Gemeinden's own spatial join, or a Gemeinde-local synthetic-center
        # candidate that a retried/duplicated per-Gemeinde output produced
        # twice) - the registry correctly assigns both occurrences the same
        # location_id (candidate_uid is the real-world identity, not the
        # Gemeinde), but each occurrence's own row still lands in
        # location_batches separately, since every Gemeinde is appended
        # independently. Confirmed nationwide: 91 duplicated location_id
        # values in the 2037 delivery, all real (candidate_uid, use_case)
        # collisions, not a registry bug. Merge rather than drop: each
        # occurrence represents its own share of real demand at that shared
        # site, so charging_points is summed; the rest describes the same
        # physical location and any occurrence's value is as good as
        # another's, except average_charging_capacity, which takes the max
        # to match the "bump up on a higher-power match" logic below.
        if all_locations["location_id"].duplicated().any():
            merged = all_locations.groupby("location_id", as_index=False).agg({
                "charging_points": "sum",
                "average_charging_capacity": "max",
                "use_case": "first",
                "candidate_uid": "first",
                "is_synthetic_location": "first",
                "geometry": "first",
            })
            all_locations = gpd.GeoDataFrame(merged, geometry="geometry", crs=location_batches[0].crs)
        loc_dictionary_cols = ["charging_points", "average_charging_capacity", "use_case"]
        if args.drop_candidate_uid:
            # See --drop_candidate_uid's own help text: only safe for the
            # last year in a scenario chain, since every earlier year's
            # candidate_uid is what lets the NEXT year's run carry its real
            # locations forward.
            all_locations = all_locations.drop(columns=["candidate_uid"])
        else:
            loc_dictionary_cols.append("candidate_uid")
        all_locations.to_parquet(
            out_dir / "ev_charging_location.parquet",
            use_dictionary=loc_dictionary_cols,
            column_encoding={"location_id": "DELTA_BINARY_PACKED"}, version="2.6")
        final_n_locations = len(all_locations)

    # ev_count_municipality: number of sampled vehicles per Gemeinde per car
    # type (our own type vocabulary - bev_commercial/phev_commercial/
    # bev_light_duty_vehicle kept separate rather than collapsed into the
    # target format's single "lgv" category) + rs7_id. Derived from the
    # already-built ev_pool/ev_mapping_ev_municipality rather than the raw
    # demand input, so it reflects what was actually sampled (post-rounding),
    # not the theoretical input demand.
    joined = mapping_muni_all.merge(ev_pool_all[["ev_id", "rs7_id", "type"]], on="ev_id")
    counts = joined.pivot_table(index="ags", columns="type", values="id", aggfunc="count", fill_value=0)
    rs7_per_ags = joined.groupby("ags")["rs7_id"].first()
    car_type_order = [c for c in vi.CAR_TYPE_COLUMNS.values() if c in counts.columns]
    ev_count_municipality = counts[car_type_order].reset_index()
    # int32 for the per-car-type counts: a single Gemeinde's count for one
    # car type stayed under 30,000 nationwide in 2024, but this is genuinely
    # demand-scale sensitive (2037 is ~12.8x 2024's total vehicle count) -
    # int16/uint16 (max 32,767/65,535) is too close to that growth for
    # comfort for a megacity's dominant car type, so int32 (~2.1 billion) is
    # the safe choice here, unlike the other columns above that are bounded
    # by something other than demand scale.
    ev_count_municipality[car_type_order] = ev_count_municipality[car_type_order].astype(np.int32)
    # RS7_CODE is a fixed BBSR vocabulary (71-77) that will never grow -
    # same reasoning as ev_pool_rows["rs7_id"] above.
    ev_count_municipality["rs7_id"] = ev_count_municipality["ags"].map(rs7_per_ags).astype(np.int8)
    ev_count_municipality.to_parquet(out_dir / "ev_count_municipality.parquet", engine="pyarrow", index=False)

    print(f"--- done: {offsets['mapping_muni']} sampled vehicle instances ({offsets['ev']} distinct EVs/pool profiles, "
          f"{offsets['event']} event rows - dedup ratio {offsets['mapping_muni'] / max(1, offsets['ev']):.1f}x), "
          f"{offsets['location']} locations, {offsets['mapping_event_loc']} event-location mappings ---")

    # metadata_simbev_run.json / metadata_geolis_run.json - see the spec
    # Excel's "Zielformat" sheet. The simbev one is just carried forward from
    # the pool run that produced this scenario's vehicle profiles (it already
    # holds simbev's own run config incl. vehicle tech params); the geolis
    # one is new - the spec author wasn't sure it existed yet ("Gibt es
    # sowas?"), so this documents our own run's parameters.
    config_path = pathlib.Path(args.config_file)
    if not config_path.is_file():
        raise FileNotFoundError(
            f"--config_file {config_path} not found (cwd={pathlib.Path.cwd()}) - refusing to write "
            f"metadata_geolis_run.json with scenario_name/random_seed/simbev_pool_dir/previous_scenario_dir "
            f"silently left null and metadata_simbev_run.json not copied"
        )
    cfg = cp.ConfigParser()
    cfg.read(config_path)
    simbev_pool_dir = cfg.get("data", "simbev_pool_dir", fallback=None) if cfg.has_section("data") else None
    if simbev_pool_dir:
        simbev_metadata_src = pathlib.Path(simbev_pool_dir) / "metadata_simbev_run.json"
        if simbev_metadata_src.is_file():
            shutil.copyfile(simbev_metadata_src, out_dir / "metadata_simbev_run.json")
        else:
            warnings.warn(f"metadata_simbev_run.json not found at {simbev_metadata_src}, skipping copy")
    geolis_metadata = {
        "scenario_name": cfg.get("data", "scenario_name", fallback=None) if cfg.has_section("data") else None,
        "vehicle_input_file": str(args.vehicle_input_file),
        "simbev_pool_dir": simbev_pool_dir,
        "previous_scenario_dir": cfg.get("data", "previous_scenario_dir", fallback=None) if cfg.has_section("data") else None,
        "location_registry_path": str(registry_path),
        "random_seed": cfg.getint("basic", "random_seed", fallback=None) if cfg.has_section("basic") else None,
        "result_dir": str(result_dir),
        "consolidated_at": datetime.datetime.now().isoformat(),
        "n_vehicle_instances": int(offsets["mapping_muni"]),
        "n_distinct_evs": int(offsets["ev"]),
        "n_event_rows": int(offsets["event"]),
        "n_locations": int(final_n_locations) if final_n_locations is not None else int(offsets["location"]),
    }
    with open(out_dir / "metadata_geolis_run.json", "w") as f:
        json.dump(geolis_metadata, f, indent=2, default=str)

    # Master switch over all three reports (evaluation_report.xlsx,
    # data_quality_report.html, dataset_report.html) - added 2026-09-14 after
    # this session found every full-scale crash so far landing in this exact
    # phase (build_evaluation_report.write_report()'s original Kennzahlen
    # OOM, then several unexplained segfaults right around here too). The
    # underlying full-table reads across all three report scripts are now
    # streamed/chunked (see each module's own comments), which should have
    # fixed the actual cause - but this flag exists so a run can still get
    # the core consolidated tables (the part that matters for the next
    # scenario year's own location-registry chain) without depending on that
    # fix, or on this whole phase at all, by setting build_reports = false.
    # Defaults to true so a config that predates this flag keeps its current
    # behavior unchanged.
    build_reports = cfg.getboolean("analysis", "build_reports", fallback=True) if cfg.has_section("analysis") else True
    if not build_reports:
        print("--- [analysis] build_reports = false - skipping all three reports ---")
        return

    run_evaluation = cfg.getboolean("analysis", "run_evaluation", fallback=False) if cfg.has_section("analysis") else False
    if run_evaluation:
        import build_evaluation_report as ber
        ber.write_report(result_dir, out_dir, args.vehicle_input_file, out_dir / "evaluation_report.xlsx")

    # Always, independent of run_evaluation - a referential-integrity check
    # (does every charging event have a location?) is cheap relative to the
    # consolidation that just ran, and catches regressions in the very run
    # that introduced them rather than needing another manual investigation
    # (see build_data_quality_report.py's own module docstring for the
    # investigation that led to this).
    import build_data_quality_report as bdq
    bdq.write_report(out_dir, out_dir / "data_quality_report.html")

    # A second, complementary report - not a gap-check but a full structural
    # overview of the six output tables (schemas, use-case/geo breakdowns,
    # weekly load profile), styled after the reGon team's own
    # V1.1_data_structure.html reference report.
    import build_dataset_report as bdr
    bdr.write_report(out_dir, out_dir / "dataset_report.html")


if __name__ == "__main__":
    main()
