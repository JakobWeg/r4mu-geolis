"""DE-wide entry point: samples charging events per Gemeinde from the SimBEV
driving-profile pool, restricts data_DE candidate locations to that Gemeinde,
and runs the existing use_case.py placement logic in parallel across all
Gemeinden (multiprocessing.Pool).

Does not touch/replace __main__.py, which remains the entry point for the
single-region, pre-simulated-timeseries scenarios (config.cfg,
config_Berlin.cfg, config_stralsund.cfg, config_office.cfg).

Usage:
    python run_de.py --config_file config_DE.cfg
    python run_de.py --config_file config_DE.cfg --limit 20      # quick test
    python run_de.py --config_file config_DE.cfg --ags 08111000 08115001
"""
import argparse
import configparser as cp
import glob
import json
import multiprocessing as mp
import os
import pathlib
import shutil
import time
import traceback
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import geopandas as gpd

import data_de_candidates as dc
import existing_infrastructure as ei
import municipality_boundaries as mb
import use_case as uc
import use_case_helpers as uc_helpers
import vehicle_input as vi
import utility

# candidate_uid prefix for the synthetic hpc_highway-at-Gemeinde-centroid
# candidate (see _synthetic_center_candidate) - restructure_output.py imports
# this same constant to flag these locations as synthetic in the final
# output, so the two stay in sync rather than duplicating the string. Kept as
# its own distinct constant (not folded into SYNTHETIC_CENTER_PREFIX below)
# because it's already baked into every earlier scenario year's own
# candidate_uid values on disk (2024's hpc_highway synthetic locations use
# exactly this string) - changing its format would silently break that
# year's already-registered locations from being recognized by a later
# year's own carry-forward lookup (see location_registry.py).
HPC_HIGHWAY_CENTER_PREFIX = "hpc_highway_center_"
# Generic prefix for every OTHER use case's synthetic-centroid fallback (see
# _synthetic_center_candidate) - new with this fix, so no prior scenario
# year's output already used a different format that needs preserving.
SYNTHETIC_CENTER_PREFIX = "synthetic_center_"


def _synthetic_center_prefix(use_case: str) -> str:
    return HPC_HIGHWAY_CENTER_PREFIX if use_case == "hpc_highway" else f"{SYNTHETIC_CENTER_PREFIX}{use_case}_"


# Gemeinden above this nationwide vehicle count get throttled to at most
# MEGACITY_CONCURRENCY concurrent workers (see process_gemeinde) - confirmed
# via a single Hamburg-scale Gemeinde (629k vehicles, just above this
# threshold) alone needing ~94GB of committed memory for its full per-vehicle
# profile/event DataFrames, independent of the availability-matrix fix
# (use_case_helpers' memmap change) or n_workers. ~26 Gemeinden nationwide
# exceed this for the 2037 scenario; 2024's demand never reaches it (its own
# largest Gemeinde is far smaller), so this throttle is simply inactive there.
MEGACITY_VEHICLE_THRESHOLD = 100_000

# Flat cap on how many megacity-scale Gemeinden may be processed at once,
# regardless of each one's own size - a Gemeinde just over
# MEGACITY_VEHICLE_THRESHOLD is throttled the same as a Berlin/Munich-scale
# one. Calibrated against the single confirmed data point above (629k
# vehicles -> ~94GB): two such Gemeinden landing on workers simultaneously
# (2 x ~94GB) still fits this machine's 256GB with headroom for the other
# n_workers-2 workers' own (much smaller) Gemeinden plus Windows' own
# commit-charge overhead; three would not.
MEGACITY_CONCURRENCY = 2


# Every use case the synthetic-centroid fallback can cover - every one here
# is a genuine fallback, only reached when that use case's real candidate
# layer came back completely empty for a given Gemeinde (see each call
# site's own "if candidates.empty:" check in main()'s per-Gemeinde loop) -
# see _synthetic_center_candidate's own docstring for why.
SYNTHETIC_CENTER_USE_CASES = [
    "hpc_highway", "hpc_urban", "work", "retail", "public", "public_home_street",
    "home_apartment", "home_detached", "depot",
]

# Every use_case.py function expects its OWN weight column name (and, for
# retail, a whole fixed extra schema it selects columns by) - checked
# against each function's own source directly, not assumed uniform:
#   hpc(): weight_column="weight" (run_de.py's own hpc_urban/hpc_highway
#     calls) - no other special columns.
#   home(): weight_column="households_total", used as-is with no renaming/
#     filtering.
#   work(): weight_column=config["work_weight_column"] ("area" in every
#     config so far), used as-is with no renaming/filtering.
#   public(): the internal "not_home_street" candidate side always ends up
#     selected down to just ["Weight", "geometry", ...extra_cols] (capital
#     W) regardless of config - see use_case.py's public(). Its "home_street"
#     side is a SEPARATE candidate source (the home_apartment layer, reused)
#     that public() itself renames households_total->Weight - given its own
#     synthetic-center key ("public_home_street", not "home_apartment") so
#     its candidate_uid never collides with home_apartment()'s own,
#     independent fallback for the same Gemeinde (both would otherwise
#     generate the identical "synthetic_center_home_apartment_<ags>" uid for
#     what are really two logically distinct synthetic locations).
#   retail(): selects a FIXED real-world OSM/ALKIS schema (id_0, osm_way_id,
#     amenity, other_tags, id, area, category, geometry) and additionally
#     requires area > 100 OR existing_points_column > 0 to survive its own
#     area_filter - values here are chosen to clear that filter unconditionally.
#   depot(): needs "area" > 1 - same value as retail's works here too, no
#     need for a separate lower one.
SYNTHETIC_CENTER_EXTRA_COLUMNS = {
    "hpc_urban": {"weight": 1.0},
    "hpc_highway": {"weight": 1.0},
    "home_apartment": {"households_total": 1.0},
    "home_detached": {"households_total": 1.0},
    "work": {"area": 150.0},
    "public": {"Weight": 1.0},
    "public_home_street": {"households_total": 1.0},
    "retail": {
        "id_0": 0, "osm_way_id": 0, "amenity": "synthetic_center", "other_tags": "synthetic_center",
        "id": 0, "area": 150.0, "category": "synthetic_center",
    },
    "depot": {"area": 150.0},
}

COLUMNS_OUTPUT_LOCATIONS = ["location_id", "charging_points", "average_charging_capacity", "candidate_uid", "geometry"]
COLUMNS_OUTPUT_CHARGINGEVENTS = ["event_id", "charging_use_case", "car_type", "event_start", "event_time",
                                  "energy", "soc_start", "soc_end", "station_charging_capacity",
                                  "location_id", "geometry"]

# which prepared candidate layers each use case needs (see data_de_candidates.LAYER_CONFIG)
USE_CASE_LAYERS = {
    "home": ["home_apartment", "home_detached"],
    "work": ["work"],
    "hpc": ["hpc_urban", "hpc_rural"],
    "retail": ["retail"],
    "public": ["public_poi", "home_apartment"],
    "depot": ["depot"],
}


def parse_config(config_path) -> dict:
    parser = cp.ConfigParser()
    config_path = pathlib.Path(config_path)
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file {config_path} not found.")
    parser.read(config_path)

    n_workers = parser.getint("basic", "n_workers")
    if n_workers <= 0:
        # Not cpu_count()-1: process_gemeinde is I/O-heavy (each Gemeinde
        # reads hundreds to thousands of individual SimBEV pool CSVs), and
        # very high worker counts were observed to make some tasks hang
        # indefinitely under disk I/O contention (confirmed via a Gemeinde
        # that took 20s standalone but never completed under 63 workers).
        n_workers = min(16, max(1, (os.cpu_count() or 2) - 1))

    # Which of vehicle_input.CAR_TYPE_COLUMNS' car types to actually sample -
    # defaults to every class (bev_heavy_duty_vehicle, the genuinely
    # N2/N3-parameterised heavy-truck pool, isn't in CAR_TYPE_COLUMNS at all
    # and so is never sampled regardless). Set explicitly in a config to
    # scope a run to specific vehicle classes for testing.
    default_vehicle_classes = list(vi.CAR_TYPE_COLUMNS.values())
    vehicle_classes = [c.strip() for c in parser.get(
        "data", "vehicle_classes", fallback=", ".join(default_vehicle_classes)).split(",")]
    unknown_classes = set(vehicle_classes) - set(vi.CAR_TYPE_COLUMNS.values())
    if unknown_classes:
        raise ValueError(f"Unknown vehicle_classes in config: {unknown_classes} "
                          f"(valid: {sorted(vi.CAR_TYPE_COLUMNS.values())})")

    return {
        "data_dir": parser.get("data", "data_dir"),
        "vehicle_input_file": parser.get("data", "vehicle_input_file"),
        "vehicle_classes": vehicle_classes,
        "simbev_pool_dir": parser.get("data", "simbev_pool_dir"),
        "municipality_boundaries": parser.get("data", "municipality_boundaries"),
        "random_seed": parser.getint("basic", "random_seed"),
        "n_workers": n_workers,
        "multi_use_concept": parser.getboolean("basic", "multi_use_concept"),
        "multi_use_group": [g.strip() for g in parser.get("basic", "multi_use_group").split(",")],
        "use_case_multi_use": parser.get("basic", "use_case_multi_use"),
        "flexibility_multi_use": parser.getint("basic", "flexibility_multi_use"),
        "share_office_parking": parser.getfloat("basic", "share_office_parking"),
        "run_hpc": parser.getboolean("use_cases", "hpc"),
        "run_public": parser.getboolean("use_cases", "public"),
        "run_home": parser.getboolean("use_cases", "home"),
        "run_work": parser.getboolean("use_cases", "work"),
        "run_retail": parser.getboolean("use_cases", "retail"),
        "run_depot": parser.getboolean("use_cases", "depot"),
        "charging_time_limit": parser.getboolean("uc_params", "charging_time_limit"),
        "charging_time_limit_duration": parser.getint("uc_params", "charging_time_limit_duration"),
        "charging_time_limit_start": parser.getint("uc_params", "charging_time_limit_start"),
        "charging_time_limit_end": parser.getint("uc_params", "charging_time_limit_end"),
        "work_weight_column": parser.get("uc_params", "work_weight_column"),
        "use_existing_infrastructure": parser.getboolean("data", "use_existing_infrastructure", fallback=False),
        "existing_infrastructure_xlsx": parser.get("data", "existing_infrastructure_xlsx", fallback=None),
        "existing_infrastructure_gpkg": parser.get("data", "existing_infrastructure_gpkg", fallback=None),
        # Multi-year scenario chain (e.g. 2024 -> 2037 -> 2045): path to an
        # earlier scenario's results/normalized_.../ directory, whose own
        # already-placed locations get filled first (see
        # location_registry.py) instead of/on top of BNetzA, and whose
        # location_registry.parquet this run continues (same registry_path
        # for every year in one chain). Leave both unset for a scenario
        # chain's first year.
        "previous_scenario_dir": parser.get("data", "previous_scenario_dir", fallback=None),
        "location_registry_path": parser.get("data", "location_registry_path", fallback=None),
        # Distinguishes this year's prepared candidates from other scenario
        # years' - existing_points/existing_capacity_kw seeding (BNetzA vs.
        # a prior scenario's own output) differs per year, so they can't
        # share one _prepared/ cache even though the underlying geographic
        # candidates (data_dir) are identical.
        "scenario_name": parser.get("data", "scenario_name", fallback="default"),
    }


def active_layers(config: dict) -> list:
    layers = set()
    for uc_name, needed in USE_CASE_LAYERS.items():
        if config[f"run_{uc_name}"]:
            layers.update(needed)
    return sorted(layers)


_WORKER = {}


def _init_worker(config, demand_df, pool_index, prepared_dir, result_dir, synthetic_center,
                  megacity_ags=frozenset(), megacity_semaphore=None):
    _WORKER.update(config=config, demand_df=demand_df, pool_index=pool_index,
                    prepared_dir=prepared_dir, result_dir=result_dir, synthetic_center=synthetic_center,
                    megacity_ags=megacity_ags, megacity_semaphore=megacity_semaphore)
    # One scratch dir per worker process (own PID - workers are recycled via
    # maxtasksperchild, so a fresh worker gets a fresh, empty dir; an old
    # one's dir is simply abandoned on disk, harmless and tiny since
    # process_gemeinde already clears it after every Gemeinde regardless).
    # See use_case_helpers.set_availability_scratch_dir's own docstring for
    # why this exists: the (n_locations x simulation_steps) availability
    # matrices are memmap-backed here instead of plain in-memory arrays, to
    # stay under Windows' commit-charge ceiling at DE-wide demand scale.
    scratch_dir = pathlib.Path(result_dir, "_availability_scratch", str(os.getpid()))
    uc_helpers.set_availability_scratch_dir(scratch_dir)
    _WORKER["availability_scratch_dir"] = scratch_dir


def _synthetic_center_candidate(ags: str, use_case: str) -> gpd.GeoDataFrame:
    """One-row candidate GeoDataFrame at this Gemeinde's own centroid.

    A genuine FALLBACK for every use case that can call this (including
    hpc_highway, see below) - only reached when that use case's real
    candidate layer came back completely empty for this Gemeinde (see each
    call site's own "if candidates.empty:" check). Added after a 2024
    nationwide referential-integrity check found 8.1%/6.4%/4.4%/1.3% of
    retail/urban_fast(hpc_urban)/work/street charging events had no location
    at all - traced to Gemeinden with zero real candidates of that kind (e.g.
    a village with no OSM-tagged retail parking).

    hpc_highway used to bypass its real hpc_rural candidate layer
    UNCONDITIONALLY instead, via a since-removed "hpc_highway_use_gemeinde_
    center" config flag - confirmed 2026-09-08 to have made 100% of
    highway_fast locations nationwide synthetic, discarding real BAST
    candidates (and any BNetzA existing-infrastructure already matched onto
    them) even in Gemeinden that had them. Fixed to use the same
    empty-only-fallback pattern as every other use case here.

    Empty (no row) if this Gemeinde's centroid wasn't precomputed in main() -
    same effect as an empty real candidate layer (that use case just doesn't
    place anything there, same as always).

    candidate_uid is deterministic per (use_case, Gemeinde) - not per-file-
    position like data_de_candidates.load_layer()'s - so it stays stable
    across scenario years the same way real candidates' ids do (see
    location_registry.py).
    """
    center = _WORKER["synthetic_center"]
    extra_cols = dict(SYNTHETIC_CENTER_EXTRA_COLUMNS.get(use_case, {}))
    centroid = center["centroids"].get(ags)
    if centroid is None:
        return gpd.GeoDataFrame(
            columns=["candidate_uid", "existing_points", "existing_capacity_kw", *extra_cols],
            geometry=[], crs=center["crs"],
        )
    existing_points, existing_capacity_kw = center["existing"].get((use_case, ags), (0, 0.0))
    data = {
        "candidate_uid": [f"{_synthetic_center_prefix(use_case)}{ags}"],
        "existing_points": [existing_points],
        "existing_capacity_kw": [existing_capacity_kw],
        **{col: [val] for col, val in extra_cols.items()},
    }
    return gpd.GeoDataFrame(data, geometry=[centroid], crs=center["crs"])


def _base_uc_dict(config: dict, ags: str, charging_events: pd.DataFrame, gemeinde_result_dir: pathlib.Path) -> dict:
    # The SimBEV pool covers a much longer period than the ~1-week window the
    # old Berlin pipeline hardcoded (simulation_steps=2000); size the
    # availability arrays to whatever this Gemeinde's events actually span.
    simulation_steps = int((charging_events["event_start"] + charging_events["event_time"]).max()) + 1

    return {
        "charging_event": charging_events,
        "result_dir": gemeinde_result_dir,
        "random_seed": np.random.default_rng((config["random_seed"], int(ags))),
        "seed": config["random_seed"],
        "simulation_steps": simulation_steps,
        "columns_output_locations": COLUMNS_OUTPUT_LOCATIONS,
        "columns_output_chargingevents": COLUMNS_OUTPUT_CHARGINGEVENTS,
        "multi_use_concept": config["multi_use_concept"],
        "multi_use_group": config["multi_use_group"],
        "use_case_multi_use": config["use_case_multi_use"],
        "flexibility_multi_use": config["flexibility_multi_use"],
        "share_office_parking": config["share_office_parking"],
        "additional_public_input": False,
        "save_csv": False,
        "save_gpkg": False,
    }


def _run_use_case(ags: str, uc_name: str, fn, *args, **kwargs) -> dict:
    """Run one use_case.py function, logging (not raising) on failure so one
    broken use case doesn't lose a whole Gemeinde's other results."""
    try:
        return fn(*args, **kwargs)
    except Exception:
        warnings.warn(f"Gemeinde {ags}: use case '{uc_name}' failed:\n{traceback.format_exc()}")
        return None


def _write_vehicle_profiles(ags: str, charging_events: pd.DataFrame, full_profiles: pd.DataFrame,
                             gemeinde_result_dir: pathlib.Path, result_dir: pathlib.Path) -> None:
    """Enrich full_profiles (vi.build_charging_events_for_gemeinde's vp - the
    first instance to draw a given pool profile keeps every row, every other
    instance drawing that same profile only keeps its own charging rows,
    see that function's docstring) with a location_id column (only filled on
    rows an actual charging event got placed on) and write ONE Parquet file
    per Gemeinde (all vehicles, keyed by vehicle_id) instead of one CSV per
    vehicle - same information, far fewer files and much smaller on disk
    (columnar + compression) at nationwide scale.
    """
    event_files = glob.glob(str(gemeinde_result_dir / "output_*_charging-events.parquet"))
    lookup_frames = []
    for f in event_files:
        # location_id is only unique WITHIN one use case's own output file
        # (e.g. depot and work both number their own locations from 0, and
        # their ranges overlap) - tag which use case's file each row came
        # from so downstream consumers (restructure_output.py) can
        # disambiguate instead of conflating, say, depot location 70 with
        # work location 70. A given event_id itself is unambiguous (each
        # simulated event is placed by exactly one use_case.py function), so
        # this tag doesn't change which rows match below - it only carries
        # forward which file the match came from.
        stem = pathlib.Path(f).stem
        uc = stem[len("output_"):-len("_charging-events")]
        # Plain pd.read_parquet, not gpd.read_parquet - geometry isn't needed
        # here at all (only event_id/location_id), so there's no reason to
        # pay for decoding it.
        df = pd.read_parquet(f, columns=["event_id", "location_id"])
        df["location_use_case"] = uc
        lookup_frames.append(df)

    full_profiles = full_profiles.copy()
    full_profiles["location_id"] = pd.NA
    full_profiles["location_use_case"] = pd.NA

    if lookup_frames:
        location_lookup = pd.concat(lookup_frames, ignore_index=True).drop_duplicates("event_id")
        # Written back by direct row position (profile_row_index, captured in
        # vehicle_input.build_charging_events_for_gemeinde before charging_events
        # got its own reset_index) - not by re-matching on (vehicle_id,
        # event_start, event_time). That key-based match silently missed any
        # event whose event_time use_case_helpers.park_time_limitation()
        # shortened for placement (config charging_time_limit, "street"
        # events specifically): full_profiles keeps the ORIGINAL event_time,
        # so the two no longer agreed and the row's real, successfully-placed
        # location_id was dropped - confirmed responsible for ~18% of all
        # "street" charging events ending up unmapped despite being placed.
        enriched = charging_events[["event_id", "profile_row_index"]].merge(
            location_lookup, on="event_id", how="inner")
        full_profiles.loc[enriched["profile_row_index"], "location_id"] = enriched["location_id"].to_numpy()
        # .astype(object) before .to_numpy(), not a bare .to_numpy() - on
        # pandas 3.x's default Arrow-backed string dtype (location_use_case
        # is a plain Python string assigned above, e.g. "public"),
        # ChunkedArray.to_numpy() has been observed to raise a spurious
        # ArrowException("Unknown error: Wrapping") on an otherwise-healthy
        # Gemeinde (confirmed: Gemeinde 05119000's 2037 run) - same
        # underlying Arrow-memory-pool fragmentation already worked around
        # in restructure_output.py's process_gemeinde(). .astype(object)
        # first forces a plain numpy object array, sidestepping the fragile
        # Arrow compute kernel entirely.
        full_profiles.loc[enriched["profile_row_index"], "location_use_case"] = (
            enriched["location_use_case"].astype(object).to_numpy())

    vehicle_profile_dir = pathlib.Path(result_dir, "vehicle_profiles")
    vehicle_profile_dir.mkdir(parents=True, exist_ok=True)
    # row_group_size bounds pyarrow's per-row-group dictionary/string buffer -
    # writing a megacity's full_profiles (potentially tens of millions of
    # rows) as one single row group hit pyarrow's internal "cannot store
    # strings with size 2GB or more" limit (confirmed on München, AGS
    # 09162000, during the first full nationwide 2024 run). 1_000_000 was
    # NOT enough for Berlin/Duisburg during the 2037 rerun (2026-09-21,
    # same ArrowInvalid error) - lowered further since even the biggest
    # German city's per-row-group string buffer must fit under 2GB at this
    # smaller size (empirically confirmed working for Berlin below).
    full_profiles.to_parquet(vehicle_profile_dir / f"{ags}.parquet", engine="pyarrow", index=False,
                              row_group_size=200_000)


def process_gemeinde(ags: str):
    """Thin wrapper: one Gemeinde with bad/corrupted upstream data (e.g. a
    malformed string in a SimBEV pool CSV) must not kill the whole
    nationwide multiprocessing.Pool run - log and skip instead.

    Also throttles genuinely huge Gemeinden (see MEGACITY_VEHICLE_THRESHOLD)
    to at most MEGACITY_CONCURRENCY concurrent workers via a shared
    Semaphore: a single Hamburg-scale Gemeinde (629k vehicles) was confirmed
    to need ~94GB alone (full per-vehicle profile/event DataFrames, not the
    availability matrices - those are memmap-backed separately, see
    use_case_helpers), so even a handful of these landing on workers
    simultaneously blows Windows' commit-charge ceiling regardless of
    n_workers or the large-Gemeinde queue spacing above (both only reduce
    how OFTEN that happens, not the per-Gemeinde cost itself). The
    Semaphore caps how MANY of these specific Gemeinden run at once,
    independent of the rest of the pool - a worker that draws one while
    MEGACITY_CONCURRENCY are already in flight blocks until one finishes,
    which does cost that worker's own throughput meanwhile, but only ~26
    Gemeinden nationwide are ever affected.
    """
    is_megacity = ags in _WORKER.get("megacity_ags", ())
    semaphore = _WORKER.get("megacity_semaphore") if is_megacity else None
    if semaphore is not None:
        semaphore.acquire()
    try:
        return _process_gemeinde(ags)
    except Exception:
        warnings.warn(f"Gemeinde {ags}: process_gemeinde failed entirely, skipping:\n{traceback.format_exc()}")
        return ags, {}
    finally:
        if semaphore is not None:
            semaphore.release()
        # Clears this worker's memmap scratch files (see use_case_helpers.
        # set_availability_scratch_dir) - every availability array created
        # while processing this Gemeinde is done being used by the time it
        # returns (or raises), including work()'s return_mask=True path,
        # whose availability_mask is captured but never used again once
        # unpacked (verified against every call site). Clearing per-Gemeinde
        # rather than only at worker recycle keeps disk usage bounded to
        # whatever one Gemeinde's own candidates need, not the whole
        # maxtasksperchild batch's worth.
        scratch_dir = _WORKER.get("availability_scratch_dir")
        if scratch_dir is not None and scratch_dir.is_dir():
            for f in scratch_dir.iterdir():
                try:
                    f.unlink()
                except OSError:
                    pass


def _process_gemeinde(ags: str):
    config = _WORKER["config"]
    demand_df = _WORKER["demand_df"]
    pool_index = _WORKER["pool_index"]
    prepared_dir = _WORKER["prepared_dir"]
    result_dir = _WORKER["result_dir"]

    gemeinde_demand = demand_df.loc[demand_df["AGS"] == ags]
    if gemeinde_demand.empty:
        return ags, {}
    regiostar7 = gemeinde_demand["RegioStaR7"].iloc[0]

    vehicles = vi.sample_vehicles(ags, regiostar7, demand_df, pool_index, config["random_seed"])
    if not vehicles:
        return ags, {}

    charging_events, full_profiles = vi.build_charging_events_for_gemeinde(ags, vehicles)
    if charging_events.empty:
        return ags, {}

    if config["charging_time_limit"]:
        charging_events = uc_helpers.park_time_limitation(charging_events, config, "street")

    gemeinde_result_dir = pathlib.Path(result_dir, "gemeinden", ags)
    gemeinde_result_dir.mkdir(parents=True, exist_ok=True)

    uc_dict = _base_uc_dict(config, ags, charging_events, gemeinde_result_dir)
    # energy_grid_kWh: GRID-side energy (charging_events["energy"], SimBEV's
    # own "energy_grid" column - see vehicle_input.py's load_and_tag_profile),
    # i.e. what's drawn at the plug INCLUDING charging losses. This is a
    # different physical quantity than the consolidated ev_event table's
    # chargingdemand_battery_kWh (BATTERY-side, from energy_battery - see
    # restructure_output.py's _derive_target_event_columns) - the two are
    # expected to differ by roughly the charging efficiency (~1/0.9 nationwide
    # in 2024), not a bug. Named explicitly to avoid the two being silently
    # compared as if the same quantity (confirmed: summarize_by_rs7.py's
    # workbook energy figures, sourced from here, came out ~11% above the
    # tables' battery-side totals with no indication anywhere that they were
    # different things).
    results_summary = {}
    charging_events_public_after_multi_use = None

    # existing_points/existing_capacity_kw get written into the prepared
    # candidate parquet by either BNetzA matching (retail/public/hpc only)
    # or a prior scenario year's own output (all 8 use cases - see
    # location_registry.py) - either way, pass the column names through so
    # distribute_charging_events*() seeds/prioritizes from them.
    existing_cols = {}
    if config["use_existing_infrastructure"] or config["previous_scenario_dir"]:
        existing_cols = {"existing_points_column": "existing_points", "existing_capacity_column": "existing_capacity_kw"}

    if config["run_home"]:
        for mode, layer in [("apartment", "home_apartment"), ("detached", "home_detached")]:
            candidates = dc.load_candidates(prepared_dir, layer, ags)
            if candidates.empty:
                # Genuine fallback (see _synthetic_center_candidate) - a
                # Gemeinde with no real home_apartment/home_detached
                # candidates at all would otherwise silently drop 100% of
                # its own home charging demand.
                candidates = _synthetic_center_candidate(ags, layer)
            if candidates.empty:
                continue
            result = _run_use_case(ags, f"home_{mode}", uc.home, candidates, uc_dict, mode=mode,
                                    simulation_steps=uc_dict["simulation_steps"], vehicle_column="vehicle_id",
                                    label=ags, **existing_cols)
            if result:
                points, energy, power = result
                results_summary[f"home_{mode}"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

    if config["run_work"]:
        candidates = dc.load_candidates(prepared_dir, "work", ags)
        if candidates.empty:
            candidates = _synthetic_center_candidate(ags, "work")
        if not candidates.empty:
            result = _run_use_case(ags, "work", uc.work, candidates, uc_dict, weight_column=config["work_weight_column"],
                                    simulation_steps=uc_dict["simulation_steps"], vehicle_column="vehicle_id",
                                    label=ags, **existing_cols)
            if result:
                points, energy, power = result
                results_summary["work"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

    if config["run_hpc"]:
        urban = dc.load_candidates(prepared_dir, "hpc_urban", ags)
        if urban.empty:
            urban = _synthetic_center_candidate(ags, "hpc_urban")
        if not urban.empty:
            result = _run_use_case(ags, "hpc_urban", uc.hpc, urban, uc_dict, uc_id="hpc_urban",
                                    weight_column="weight", charging_use_case="urban_fast", exclude_shopping=True,
                                    simulation_steps=uc_dict["simulation_steps"], **existing_cols)
            if result:
                points, energy, power = result
                results_summary["hpc_urban"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

        rural = dc.load_candidates(prepared_dir, "hpc_rural", ags)
        if rural.empty:
            rural = _synthetic_center_candidate(ags, "hpc_highway")
        if not rural.empty:
            result = _run_use_case(ags, "hpc_highway", uc.hpc, rural, uc_dict, uc_id="hpc_highway",
                                    weight_column="weight", charging_use_case="highway_fast", exclude_shopping=False,
                                    simulation_steps=uc_dict["simulation_steps"], **existing_cols)
            if result:
                points, energy, power = result
                results_summary["hpc_highway"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

    if config["run_retail"]:
        candidates = dc.load_candidates(prepared_dir, "retail", ags)
        # retail() itself drops any candidate with area<=100 (unless it's
        # real existing infrastructure - see its own area_filter) BEFORE
        # placing anything - a Gemeinde can have real candidates here (so
        # this check alone would see it as non-empty and skip the fallback)
        # while every single one of them is too small to survive that
        # filter, silently leaving the whole Gemeinde's retail demand
        # unplaced with no location and no warning. Replicate retail()'s own
        # filter here so the fallback triggers on the same emptiness retail()
        # itself will actually see, not just on the raw candidate count.
        existing_points_column = existing_cols.get("existing_points_column")
        nothing_survives_area_filter = False
        if not candidates.empty:
            usable = candidates["area"] > 100
            if existing_points_column and existing_points_column in candidates.columns:
                usable |= candidates[existing_points_column].fillna(0) > 0
            nothing_survives_area_filter = not usable.any()
        if candidates.empty or nothing_survives_area_filter:
            candidates = _synthetic_center_candidate(ags, "retail")
        if not candidates.empty:
            result = _run_use_case(ags, "retail", uc.retail, candidates, uc_dict,
                                    simulation_steps=uc_dict["simulation_steps"], **existing_cols)
            if result:
                if config["multi_use_concept"] and config["use_case_multi_use"] == "retail":
                    points, energy, power, charging_events_public_after_multi_use = result
                else:
                    points, energy, power = result
                results_summary["retail"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

    if config["run_public"]:
        poi = dc.load_candidates(prepared_dir, "public_poi", ags)
        home_street = dc.load_candidates(prepared_dir, "home_apartment", ags)
        # public() places two INDEPENDENT event populations (not_home_street
        # onto poi, home_street onto home_street) - each needs its own
        # fallback check. A Gemeinde with real poi but zero home_apartment
        # buildings (or vice versa) previously fell through this as "not
        # empty overall" and silently left the empty side's entire demand
        # unplaced (confirmed: public/street had the largest residual
        # placement gap of any use case, ~1.36% nationally).
        if poi.empty:
            poi = _synthetic_center_candidate(ags, "public")
        if home_street.empty:
            home_street = _synthetic_center_candidate(ags, "public_home_street")
        if not poi.empty or not home_street.empty:
            if config["multi_use_concept"]:
                result = _run_use_case(ags, "public", uc.public, poi, home_street, uc_dict,
                                        charging_locations_public_after_multi_use=charging_events_public_after_multi_use,
                                        simulation_steps=uc_dict["simulation_steps"], **existing_cols)
            else:
                result = _run_use_case(ags, "public", uc.public, poi, home_street, uc_dict,
                                        simulation_steps=uc_dict["simulation_steps"], **existing_cols)
            if result:
                points, energy, power = result
                results_summary["public"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

    if config["run_depot"]:
        candidates = dc.load_candidates(prepared_dir, "depot", ags)
        if candidates.empty:
            candidates = _synthetic_center_candidate(ags, "depot")
        if not candidates.empty:
            result = _run_use_case(ags, "depot", uc.depot, candidates, uc_dict,
                                    simulation_steps=uc_dict["simulation_steps"], **existing_cols)
            if result:
                points, energy, power = result
                results_summary["depot"] = {"charging_points": points, "energy_grid_kWh": energy, "installed_power": power}

    try:
        _write_vehicle_profiles(ags, charging_events, full_profiles, gemeinde_result_dir, result_dir)
    except Exception:
        warnings.warn(f"Gemeinde {ags}: writing vehicle profiles failed:\n{traceback.format_exc()}")

    return ags, results_summary


def main():
    parser = argparse.ArgumentParser(description="DE-wide charging infrastructure allocation")
    parser.add_argument("--config_file", default="config_DE.cfg")
    parser.add_argument("--limit", type=int, default=None, help="only process the first N Gemeinden (testing)")
    parser.add_argument("--ags", nargs="+", default=None, help="only process these specific AGS (testing)")
    parser.add_argument("--ags_file", default=None,
                         help="only process the whitespace-separated AGS listed in this file - for resuming a run "
                              "with more AGS than fit on a command line (Windows caps argv length well under what "
                              "a full nationwide minus-completed list needs)")
    parser.add_argument("--result_dir", default=None,
                         help="write into this existing results/_DE_<timestamp> dir instead of creating a fresh "
                              "one - for isolated re-runs of specific AGS/use-cases that should merge into an "
                              "already-in-progress or completed run's own output. _write_vehicle_profiles() re-scans "
                              "gemeinde_result_dir's own output_*_charging-events.parquet files from disk every "
                              "call regardless of which invocation wrote them, so this is safe: e.g. re-running "
                              "with only [use_cases] public=true (others false) for one AGS, targeting that AGS's "
                              "existing result_dir, correctly regenerates its vehicle_profiles with both the "
                              "already-present use cases' events AND the freshly-written public ones, without "
                              "recomputing anything already correct on disk.")
    args = parser.parse_args()
    if args.ags_file:
        args.ags = pathlib.Path(args.ags_file).read_text().split()

    config = parse_config(pathlib.Path("scenario", args.config_file))

    boundaries_path = pathlib.Path(config["municipality_boundaries"])
    mb.download_vg250(boundaries_path)
    municipalities = mb.load_municipalities(boundaries_path)

    demand_df = vi.load_municipality_demand(config["vehicle_input_file"], vehicle_classes=config["vehicle_classes"])
    demand_per_ags = demand_df.groupby("AGS")["n_vehicles"].sum()
    megacity_ags = frozenset(demand_per_ags[demand_per_ags > MEGACITY_VEHICLE_THRESHOLD].index)
    if megacity_ags:
        utility.safe_print(
            f"--- {len(megacity_ags)} Gemeinden exceed {MEGACITY_VEHICLE_THRESHOLD:,} vehicles - "
            f"throttled to at most {MEGACITY_CONCURRENCY} concurrent ---")
    pool_index = vi.build_pool_index(config["simbev_pool_dir"])
    # One-time nationwide preprocessing (rename/coerce/pos_in_profile baked
    # into a Parquet cache per pool file) instead of every one of the
    # 10,747 Gemeinden redoing that same work from raw CSV each time it
    # happens to draw an already-seen profile - see prepare_pool_cache().
    pool_index = vi.prepare_pool_cache(config["simbev_pool_dir"], pool_index, n_workers=config["n_workers"])

    existing_infrastructure_path = None
    if config["use_existing_infrastructure"]:
        existing_infrastructure_path = ei.convert_register_to_gpkg(
            config["existing_infrastructure_xlsx"], config["existing_infrastructure_gpkg"]
        )

    prepared_dir = pathlib.Path(config["data_dir"], f"_prepared_{config['scenario_name']}")
    dc.prepare_candidates(config["data_dir"], prepared_dir, municipalities, active_layers(config),
                           existing_infrastructure_path=existing_infrastructure_path,
                           previous_scenario_dir=config["previous_scenario_dir"])

    # Precompute once here (not per-worker) rather than making every worker
    # re-derive it from data_de_candidates.py internals - centroids and any
    # prior-scenario capacity for these synthetic per-Gemeinde candidates are
    # both small (~10,747 Gemeinden x up to 7 use cases), so cheap to build
    # upfront and hand to every worker via _init_worker. Centroids are always
    # computed - every use case can fall back to one now, not just
    # hpc_highway (see SYNTHETIC_CENTER_USE_CASES / _synthetic_center_candidate).
    synthetic_center = {
        "centroids": dict(zip(municipalities["AGS"], municipalities.geometry.centroid)),
        "existing": {},
        "crs": municipalities.crs,
    }
    if config["previous_scenario_dir"]:
        prev_locations = pd.read_parquet(
            pathlib.Path(config["previous_scenario_dir"]) / "ev_charging_location.parquet",
            columns=["candidate_uid", "charging_points", "average_charging_capacity"],
        )
        candidate_uid_str = prev_locations["candidate_uid"].astype(str)
        existing = {}
        for uc in SYNTHETIC_CENTER_USE_CASES:
            prefix = _synthetic_center_prefix(uc)
            prev_synthetic = prev_locations[candidate_uid_str.str.startswith(prefix)]
            for _, row in prev_synthetic.iterrows():
                ags = row["candidate_uid"][len(prefix):]
                existing[(uc, ags)] = (int(row["charging_points"]), float(row["average_charging_capacity"]))
        synthetic_center["existing"] = existing

    if args.result_dir:
        result_dir = pathlib.Path(args.result_dir)
        result_dir.mkdir(parents=True, exist_ok=True)
        # Deliberately does NOT overwrite metadata.json here - that file
        # documents the ORIGINAL full run's config, and an isolated
        # single-use-case re-run's own config (e.g. only public=true) would
        # be misleading written over it.
    else:
        timestamp = datetime.now().strftime("%y-%m-%d_%H%M%S")
        result_dir = pathlib.Path("results", f"_DE_{timestamp}")
        result_dir.mkdir(parents=True, exist_ok=True)
        with open(result_dir / "metadata.json", "w") as f:
            json.dump(config, f, default=str)

    ags_list = sorted(demand_df["AGS"].unique())
    if args.ags:
        ags_list = [a.zfill(8) for a in args.ags]
    elif args.limit:
        ags_list = ags_list[: args.limit]
    else:
        # Longest-processing-time-first, SPREAD OUT rather than bunched:
        # pool.imap_unordered below dispatches in list order (chunksize 1),
        # and AGS's alphabetical order has no relation to a Gemeinde's actual
        # size - a handful of megacities can end up scattered late in that
        # order, so most workers finish their (small) share and sit idle
        # while just 1-4 workers grind through the last big cities alone,
        # adding 30-90+ min of near-idle tail to the whole run (confirmed:
        # Berlin/Munich-scale Gemeinden).
        #
        # A first fix (largest-first, no spreading) traded that for something
        # worse: with a queue front-loaded by pure descending demand, ALL
        # n_workers workers grab a megacity SIMULTANEOUSLY at the very start
        # - confirmed to overwhelm available memory even after the per-array
        # uint8 fix (this time as a pandas CSV-parser "C error: out of
        # memory" from tens of thousands of SimBEV pool files being read at
        # once across several concurrent megacities - each one alone samples
        # as many individual pool CSVs as it has vehicles, tens of thousands
        # for a city like Berlin).
        #
        # Fix: schedule the largest Gemeinden early (so they're not an
        # idle-tail liability) but SPACED OUT through the whole queue - at
        # most one still lands in any window of n_workers consecutive
        # dispatches, so at most a handful are ever in flight together,
        # while smaller Gemeinden keep every other worker busy in between.
        # Order has no effect on correctness/ids (restructure_output.py
        # re-sorts by AGS on its own regardless). demand_per_ags computed
        # once, above, alongside megacity_ags.
        by_demand_desc = sorted(ags_list, key=lambda a: demand_per_ags.get(a, 0), reverse=True)
        # Fixed, NOT scaled by n_workers (a previous version of this used
        # n_workers*6, which for n_workers=8 evaluated to 48 - clamped right
        # back down to the same 50 floor this was meant to raise, so it
        # silently had zero effect). What actually determines how many
        # Gemeinden need spacing is the NATIONWIDE DEMAND DISTRIBUTION, not
        # the worker count - confirmed via 2037's own numbers: Berlin alone
        # is ~951k vehicles, and 676 Gemeinden exceed 10k (vs a 2024-tuned
        # top-50 cutoff). 1000 covers deep into that long tail so the
        # "rest" random shuffle isn't left to cluster several still-
        # substantial (10-50k vehicle) cities together purely by chance
        # (confirmed cause of a 2037 run stalling for hours: several
        # such cities landed on workers simultaneously despite the old
        # top-50-only spacing).
        large_count = 1000
        large, rest = by_demand_desc[:large_count], by_demand_desc[large_count:]
        # rest must NOT stay demand-sorted: it's used as filler between the
        # spaced-out large cities below, and a still-descending rest means
        # the filler positions immediately surrounding each inserted large
        # city are themselves the next-largest Gemeinden (rank ~51-200),
        # reproducing the exact same concurrent-megacity clustering one tier
        # down - confirmed via a second OOM crash on AGS 08421000 (48,308
        # vehicles, rank ~60, not one of the intentionally-spaced large
        # ones). Sorting rest back to natural AGS order is NOT enough
        # either: AGS is assigned by administrative region, so e.g. the
        # entire Ruhrgebiet ends up as a run of adjacent, similarly
        # mid-to-large Gemeinden - confirmed via a verification script
        # finding a 16-worker window with 8 top-200-demand Gemeinden and
        # ~448k combined vehicles just from natural AGS order. A seeded
        # random shuffle decorrelates position from both size and geography.
        rest = list(rest)
        np.random.default_rng(config["random_seed"]).shuffle(rest)
        ags_list = list(rest)
        if large:
            step = max(1, len(rest) // len(large))
            for i, ags in enumerate(large):
                ags_list.insert(min(i * step, len(ags_list)), ags)

    t0 = time.time()
    all_results = {}
    # maxtasksperchild: retire and respawn a worker after this many Gemeinden
    # instead of letting it live for the whole run. Each Gemeinde allocates
    # differently-sized numpy arrays (availability matrices scale with that
    # Gemeinde's own candidate count and event span), and a worker that's
    # processed thousands of them back-to-back can fragment its own address
    # space badly enough that a later, genuinely large allocation (a big
    # city's home_apartment/home_detached candidates) fails even though the
    # system overall has plenty of free RAM - confirmed via a 12.2GB
    # allocation failing on a 256GB machine. A fresh process has a clean,
    # unfragmented heap. Lowered from 100 to 50 for the 2037/2045 scenario
    # years - their nationwide vehicle demand is ~7x 2024's, so individual
    # Gemeinden that aren't among the explicitly-spaced-out largest ones
    # (see large_count below) are now big enough to fragment a worker's heap
    # well before it reaches 100 tasks (confirmed: MemoryError on a 2.27 MiB
    # allocation, i.e. fragmentation, not genuine system-wide exhaustion).
    # Not lower than 50: Windows' spawn-based multiprocessing (no fork/
    # copy-on-write) re-transfers and re-unpickles the full initargs payload
    # into a fresh interpreter on every recycle, so recycling more often than
    # this trades RAM safety for real, non-trivial wall-clock overhead.
    # Shared across all workers (see process_gemeinde/MEGACITY_VEHICLE_
    # THRESHOLD/MEGACITY_CONCURRENCY) - a plain mp.Semaphore is inherited
    # correctly by Pool's worker processes via initargs.
    megacity_semaphore = mp.Semaphore(MEGACITY_CONCURRENCY)
    with mp.Pool(config["n_workers"], initializer=_init_worker, maxtasksperchild=50,
                 initargs=(config, demand_df, pool_index, prepared_dir, result_dir, synthetic_center,
                           megacity_ags, megacity_semaphore)) as pool:
        for i, (ags, summary) in enumerate(pool.imap_unordered(process_gemeinde, ags_list), start=1):
            all_results[ags] = summary
            elapsed = time.time() - t0
            utility.safe_print(f"--- {i}/{len(ags_list)} Gemeinden done ({ags}, {elapsed:.0f}s) ---")

    # Each worker's own scratch dir is already emptied after every Gemeinde
    # (see process_gemeinde) - this just removes the now-empty per-PID
    # subdirectories themselves, tidiness only (also happens implicitly if
    # result_dir gets deleted wholesale after consolidation, as the chain
    # scripts do).
    #
    # Skipped when --result_dir was explicitly given: that flag means this
    # invocation is targeting a result_dir it doesn't own alone (an isolated
    # single-Gemeinde/single-use-case re-run merging into an already-in-
    # progress or already-completed run's own output - see --result_dir's
    # own help text). Confirmed the hard way (2026-09-21): pointing an
    # isolated Hamburg-only re-run at the main run's own still-active
    # result_dir, this rmtree fired at ITS OWN completion and deleted the
    # MAIN run's still-in-use per-PID scratch subdirectories out from under
    # it - every one of its 8 workers then failed every use case needing
    # _new_availability_array() (all but home_detached) for the next ~55
    # Gemeinden, until the missing directories were manually recreated.
    if not args.result_dir:
        shutil.rmtree(pathlib.Path(result_dir, "_availability_scratch"), ignore_errors=True)

    rows = []
    for ags, summary in all_results.items():
        for uc_name, values in summary.items():
            rows.append({"AGS": ags, "use_case": uc_name, **values})
    result_summary = pd.DataFrame(rows)
    result_summary.to_csv(result_dir / "result_summary_DE.csv", index=False)

    # merge_results()/results/merged/ (one nationwide GPKG per use case) is
    # superseded by restructure_output.py's leaner, crash-resumable Parquet
    # consolidation (see that module's docstring) - not called here anymore,
    # since it duplicated the whole nationwide dataset in memory in one shot
    # and nothing downstream reads results/merged/. gemeinden/ is therefore
    # also NOT deleted here: restructure_output.py still needs to read each
    # Gemeinde's raw output_*_charging-{locations,events}.parquet files
    # directly. Delete gemeinden/ yourself once restructure_output.py has
    # consolidated this result_dir, if you want to reclaim the disk space.


if __name__ == "__main__":
    main()
