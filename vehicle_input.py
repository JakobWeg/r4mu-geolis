"""Municipality vehicle demand (input_vehicles_2024.xlsx) and the SimBEV
driving-profile pool (base_..._simbev_run/<RegioStaR7-Typ>/) that DE-wide
runs (run_de.py) sample from.
"""
import multiprocessing as mp
import os
import pathlib
import warnings

import numpy as np
import pandas as pd

import utility

# Maps the long RegioStaR7-Typ labels used in input_vehicles_2024.xlsx to the
# short codes used as pool folder names / in simbev's regions.csv.
REGIOSTAR7_LABELS = {
    "Stadtregion - Metropole": "SR_Metro",
    "Stadtregion - Regiopole und Großstadt": "SR_Gross",
    "Stadtregion - Mittelstadt, städtischer Raum": "SR_Mitte",
    "Stadtregion - Kleinstädtischer, dörflicher Raum": "SR_Klein",
    "Ländliche Region - Zentrale Stadt": "LR_Zentr",
    "Ländliche Region - Städtischer Raum": "LR_Mitte",
    "Ländliche Region - Kleinstädtischer, dörflicher Raum": "LR_Klein",
}

# Maps the vehicle-count columns in input_vehicles_2024.xlsx to the pool file
# prefixes used in base_..._simbev_run/<RegioStaR7>/<prefix>_NNNNN_*.csv
CAR_TYPE_COLUMNS = {
    "Pkw privat Kleinwagen BEV": "bev_mini",
    "Pkw privat Kleinwagen PHEV": "phev_mini",
    "Pkw privat Mittelklasse BEV": "bev_medium",
    "Pkw privat Mittelklasse PHEV": "phev_medium",
    "Pkw privat Oberklasse BEV": "bev_luxury",
    "Pkw privat Oberklasse PHEV": "phev_luxury",
    "Pkw gewerblich BEV": "bev_commercial",
    "Pkw gewerblich PHEV": "phev_commercial",
    "N1-Fahrzeuge BEV": "bev_light_duty_vehicle",
}

# car_type prefixes considered "Commercial" (rest is "Private"), mirroring
# the Type column of the old Berlin pipeline (see __main__.py:parse_car_data)
COMMERCIAL_CAR_TYPES = {"bev_commercial", "phev_commercial", "bev_light_duty_vehicle"}

AGS_COLUMN = "AGS-Schlüssel"
REGIOSTAR7_COLUMN = "RegioStaR7-Typ"
GEMEINDE_COLUMN = "Gemeindename"


def load_municipality_demand(xlsx_path: pathlib.Path, vehicle_classes: list = None) -> pd.DataFrame:
    """Read input_vehicles_2024.xlsx into a tidy long-format table with
    columns: AGS, Gemeindename, RegioStaR7, car_type, n_vehicles.

    vehicle_classes: optional allowlist of car_type values (CAR_TYPE_COLUMNS'
    values, e.g. "bev_mini") to include - every other input column is
    dropped entirely (not sampled, not counted, not present in any output
    table) rather than just zeroed out. None (default) keeps every class in
    CAR_TYPE_COLUMNS. See run_de.py's parse_config "vehicle_classes".

    Note: "N1-Fahrzeuge BEV" maps to the SimBEV pool's bev_light_duty_vehicle
    (~95 kWh battery, <=350 kW charging, has "depot" charging events) - NOT
    bev_heavy_duty_vehicle, which is a separate pool parameterised as genuine
    N2/N3 heavy trucks (>450 kWh battery swings, up to 1000 kW MCS charging)
    and has no corresponding column in the input Excel at all.
    """
    df = pd.read_excel(xlsx_path)

    # Drop a trailing "Summe"/totals row some spreadsheet edits leave at the
    # bottom of the table (no AGS/Gemeindename/RegioStaR7-Typ, just numeric
    # column totals) - not a real municipality, would otherwise trip the
    # RegioStaR7-Typ validation right below or get sampled as Gemeinde #10748.
    df = df.dropna(subset=[AGS_COLUMN, GEMEINDE_COLUMN, REGIOSTAR7_COLUMN], how="all")

    unknown_labels = set(df[REGIOSTAR7_COLUMN].unique()) - set(REGIOSTAR7_LABELS)
    if unknown_labels:
        raise ValueError(f"Unknown RegioStaR7-Typ labels in {xlsx_path}: {unknown_labels}")

    df["AGS"] = df[AGS_COLUMN].astype(int).astype(str).str.zfill(8)
    df["RegioStaR7"] = df[REGIOSTAR7_COLUMN].map(REGIOSTAR7_LABELS)

    active_car_type_columns = {
        excel_col: car_type for excel_col, car_type in CAR_TYPE_COLUMNS.items()
        if vehicle_classes is None or car_type in vehicle_classes
    }
    long_df = df.melt(
        id_vars=["AGS", GEMEINDE_COLUMN, "RegioStaR7"],
        value_vars=list(active_car_type_columns),
        var_name="input_column",
        value_name="n_vehicles",
    )
    long_df["car_type"] = long_df["input_column"].map(active_car_type_columns)
    long_df["n_vehicles"] = long_df["n_vehicles"].fillna(0).round().astype(int)
    long_df = long_df.rename(columns={GEMEINDE_COLUMN: "Gemeindename"})

    return long_df[["AGS", "Gemeindename", "RegioStaR7", "car_type", "n_vehicles"]]


def build_pool_index(pool_dir: pathlib.Path) -> dict:
    """Group the SimBEV pool files by RegioStaR7 type and car_type prefix.

    Returns {regiostar7: {car_type: [Path, ...]}}. Missing RegioStaR7 folders
    are logged and left out (skip+warn, per run_de.py's robustness contract).
    """
    pool_dir = pathlib.Path(pool_dir)
    index = {}
    for regiostar7 in REGIOSTAR7_LABELS.values():
        region_dir = pool_dir / regiostar7
        if not region_dir.is_dir():
            warnings.warn(f"SimBEV pool folder missing for RegioStaR7 '{regiostar7}': {region_dir}")
            index[regiostar7] = {}
            continue
        by_car_type = {}
        for car_type in CAR_TYPE_COLUMNS.values():
            files = sorted(region_dir.glob(f"{car_type}_*_events.csv"))
            if not files:
                warnings.warn(f"No pool files for car_type '{car_type}' in {region_dir}")
            by_car_type[car_type] = files
        index[regiostar7] = by_car_type
    return index


def sample_vehicles(ags: str, regiostar7: str, demand_df: pd.DataFrame, pool_index: dict, seed: int) -> list:
    """Draw (with replacement) the required number of pool profiles for one
    Gemeinde, seeded reproducibly from (seed, AGS).

    Returns a list of dicts: {vehicle_id, car_type, source_file}.
    """
    rng = np.random.default_rng((seed, int(ags)))
    car_pools = pool_index.get(regiostar7, {})

    rows = demand_df.loc[(demand_df["AGS"] == ags) & (demand_df["n_vehicles"] > 0)]

    vehicles = []
    for _, row in rows.iterrows():
        car_type = row["car_type"]
        n_vehicles = int(row["n_vehicles"])
        files = car_pools.get(car_type, [])
        if not files:
            warnings.warn(f"Gemeinde {ags}: no pool files for car_type '{car_type}' (RegioStaR7 {regiostar7}), skipping {n_vehicles} vehicles")
            continue

        chosen_idx = rng.integers(0, len(files), size=n_vehicles)
        for i, file_idx in enumerate(chosen_idx):
            vehicles.append({
                "vehicle_id": f"{ags}_{car_type}_{i:05d}",
                "car_type": car_type,
                "source_file": files[file_idx],
            })

    return vehicles


def _pool_cache_dir(pool_dir) -> pathlib.Path:
    pool_dir = pathlib.Path(pool_dir)
    return pool_dir.parent / (pool_dir.name + "_parquet_cache")


def _convert_pool_file(paths: tuple) -> None:
    """prepare_pool_cache()'s mp.Pool worker: converts one raw SimBEV pool
    CSV into its pre-normalized Parquet cache file. Module-level (not
    nested/lambda) - Windows multiprocessing (spawn) must be able to pickle
    and re-import it in each worker. Idempotent: skips if already converted,
    so re-running a scenario year (or resuming after a crash) doesn't
    reconvert files a previous run already cached.
    """
    csv_path, cache_path = paths
    if cache_path.exists():
        return
    profile = pd.read_csv(csv_path)
    # 0-based position of each row within THIS profile's own original file,
    # captured before any filtering ever happens to it. restructure_output.py
    # reconstructs each row's event_id as event_id_start + pos_in_profile;
    # that only stays correct if pos_in_profile survives even for vehicle
    # instances that end up keeping just a filtered subset of rows (see
    # build_charging_events_for_gemeinde's non-representative-instance path).
    profile["pos_in_profile"] = np.arange(len(profile), dtype=np.int32)
    profile = profile.rename(columns={"energy_grid": "energy"})
    for numeric_col in ("energy_battery", "average_charging_power"):
        if numeric_col in profile.columns:
            # A minority of pool CSVs store some numeric columns as quoted
            # strings instead of plain floats; concatenating those files with
            # normal ones later yields an object-dtype column pyarrow can't
            # write to Parquet ("Could not convert '0.0' ... to double").
            # Coerce defensively.
            profile[numeric_col] = pd.to_numeric(profile[numeric_col], errors="coerce")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    profile.to_parquet(cache_path, index=False)


def prepare_pool_cache(pool_dir, index: dict, n_workers: int = None) -> dict:
    """One-time, whole-pool preprocessing pass: converts every SimBEV pool
    CSV referenced in `index` into an already-normalized Parquet cache file
    (see _convert_pool_file), then returns a copy of `index` pointing at
    those cache files instead of the raw CSVs.

    Without this, sampling with replacement means the same handful-thousand
    distinct profiles nationwide (far fewer than the number of sampled
    vehicle instances) each got re-read AND re-processed (CSV parse, column
    rename, numeric coercion) from scratch by EVERY Gemeinde that happened to
    draw them - build_charging_events_for_gemeinde's cache only helps within
    one Gemeinde's own worker process, not across the 10,747 Gemeinden or the
    16 worker processes that each independently draw from the same shared
    pool. This preprocessing is pure, deterministic, per-file work that does
    not depend on which Gemeinde or vehicle instance ends up drawing a
    profile, so doing it once nationwide instead of once per draw removes
    redundant work without changing any result. Parquet is also simply
    faster to read back than CSV, for every one of the many repeat reads.
    """
    pool_dir = pathlib.Path(pool_dir)
    cache_dir = _pool_cache_dir(pool_dir)
    jobs = []
    new_index = {}
    for regiostar7, by_car_type in index.items():
        new_by_car_type = {}
        for car_type, files in by_car_type.items():
            new_files = []
            for csv_path in files:
                cache_path = cache_dir / csv_path.relative_to(pool_dir).with_suffix(".parquet")
                jobs.append((csv_path, cache_path))
                new_files.append(cache_path)
            new_by_car_type[car_type] = new_files
        new_index[regiostar7] = new_by_car_type

    to_convert = [j for j in jobs if not j[1].exists()]
    if to_convert:
        utility.safe_print(f"--- pool cache: converting {len(to_convert)}/{len(jobs)} SimBEV profiles to Parquet (one-time) ---")
        with mp.Pool(n_workers or max(1, (os.cpu_count() or 2) - 1)) as pool:
            for i, _ in enumerate(pool.imap_unordered(_convert_pool_file, to_convert, chunksize=50), 1):
                if i % 5000 == 0 or i == len(to_convert):
                    utility.safe_print(f"\r--- pool cache: {i}/{len(to_convert)} converted ---", end="", flush=True)
        utility.safe_print("")
    return new_index


def _load_raw_profile(source_file) -> pd.DataFrame:
    """Read one already-cached pool profile (see prepare_pool_cache/
    _convert_pool_file - rename, numeric coercion and pos_in_profile are
    baked in at conversion time, not redone here) - WITHOUT vehicle-instance
    tagging (car_type, vehicle_id, Type). Kept separate from
    load_and_tag_profile() so a distinct source_file can be read from disk
    exactly once and its content reused for every sampled vehicle instance
    that draws it: sampling is with replacement from a pool far smaller than
    the number of instances (~7,200 files per RegioStaR7 type), so one
    megacity alone can draw the same file 100+ times - see
    build_charging_events_for_gemeinde().
    """
    return pd.read_parquet(source_file)


def _tag_profile(profile: pd.DataFrame, vehicle: dict) -> pd.DataFrame:
    """Stamp one already-loaded raw profile with one sampled vehicle
    instance's identity. Always copies - the raw profile may be the shared
    cache entry reused across many instances of the same source_file."""
    profile = profile.copy()
    profile["car_type"] = vehicle["car_type"]
    profile["vehicle_id"] = vehicle["vehicle_id"]
    profile["Type"] = "Commercial" if vehicle["car_type"] in COMMERCIAL_CAR_TYPES else "Private"
    # Stable identity of the underlying pool profile (not the sampled
    # vehicle instance) - sampling draws with replacement, both within one
    # Gemeinde and across all of them sharing the same RegioStaR7 pool, so
    # the same profile file gets reused ~65x on average nationwide. This is
    # what lets restructure_output.py store each distinct profile's events
    # once instead of once per vehicle that happened to draw it.
    profile["source_file"] = str(vehicle["source_file"])
    return profile


def load_and_tag_profile(vehicle: dict) -> pd.DataFrame:
    """Load one sampled pool CSV and tag it with vehicle/type metadata,
    harmonizing columns onto the schema the existing use_case.py pipeline
    expects (energy, Type, car_type, vehicle_id).
    """
    return _tag_profile(_load_raw_profile(vehicle["source_file"]), vehicle)


def build_charging_events_for_gemeinde(ags: str, vehicles: list) -> tuple:
    """Load and concatenate all sampled vehicle profiles for one Gemeinde.

    Returns (charging_events, vp):
    - charging_events: only rows that represent an actual charging event
      (mirrors __main__.py:parse_car_data's output shape), tagged with a
      Gemeinde-unique event_id.
    - vp: the table written out as vehicle_profiles/<ags>.parquet. For the
      FIRST vehicle instance to draw a given source_file (the "representative"
      of that profile) this holds every row (driving included), since
      restructure_output.py needs one full profile's worth of rows to write
      ev_event the first time it sees that profile anywhere in the whole
      nationwide run. Every OTHER instance drawing the same profile only
      contributes its own charging rows (or, on the rare profile with zero
      charging events all year, one fallback row) - its driving legs are
      byte-identical to the representative's and dropping them is exactly
      what keeps this scaling with the number of DISTINCT profiles drawn
      instead of the number of vehicle INSTANCES. Sampling with replacement
      means a single megacity can draw ~7,200 distinct profiles hundreds of
      thousands of times each - holding every instance's full driving-leg
      history (not just its ~7% of rows that are charging events) blew up to
      ~440GB for Berlin's 2037 demand alone (849,190 instances), confirmed
      via an out-of-memory crash reading the pool CSVs.
    """
    if not vehicles:
        empty = pd.DataFrame()
        return empty, empty

    # Grouped by source_file (dict preserves first-appearance order, Python
    # 3.7+) instead of tagging one instance at a time: car_type/Type/
    # source_file are properties of the FILE, identical for every instance
    # that draws it - only vehicle_id differs. With sampling drawing the same
    # handful of files hundreds of thousands of times for a megacity, tagging
    # instance-by-instance meant hundreds of thousands of separate small
    # pandas DataFrame constructions (.copy() + 4 column assignments each) -
    # confirmed to dominate this function's cost (measured: 784 vehicles/sec,
    # ~1083s projected for Berlin's 849,190 2037 instances). Tiling all of a
    # file's non-representative instances into ONE block per file (repeating
    # its charging-only rows and a properly-repeated vehicle_id array,
    # instead of building each instance's block separately) turns hundreds of
    # thousands of small DataFrame constructions into a few thousand -
    # measured ~14.8x faster (11,600 vehicles/sec) at the same test scale.
    vehicles_by_file = {}
    for vehicle in vehicles:
        vehicles_by_file.setdefault(vehicle["source_file"], []).append(vehicle)

    group_blocks = []
    for sf, vlist in vehicles_by_file.items():
        raw = _load_raw_profile(sf)
        car_type = vlist[0]["car_type"]
        vehicle_type = "Commercial" if car_type in COMMERCIAL_CAR_TYPES else "Private"
        # str(sf), not the raw Path: pool_index (see prepare_pool_cache)
        # stores source_file as a pathlib.Path, and assigning a Path object
        # into a DataFrame column (instead of converting it to plain text
        # first) produces an object-dtype column pyarrow can't serialize -
        # confirmed via "Could not convert WindowsPath(...) ... did not
        # recognize Python value type when inferring an Arrow data type"
        # when _write_vehicle_profiles() tried to write it out.
        sf_str = str(sf)

        # Representative (first instance in the original vehicles order to
        # draw this file - see the module-level docstring above on why this
        # must match restructure_output.py's own "first occurrence" pick):
        # gets every row, tagged with its own vehicle_id.
        representative = vlist[0]
        rep_block = raw.copy()
        rep_block["car_type"] = car_type
        rep_block["vehicle_id"] = representative["vehicle_id"]
        rep_block["Type"] = vehicle_type
        rep_block["source_file"] = sf_str
        group_blocks.append(rep_block)

        others = vlist[1:]
        if not others:
            continue
        part = raw.loc[raw["station_charging_capacity"] != 0]
        if part.empty:
            # This profile had zero charging events all year - still need
            # exactly one row per instance so restructure_output.py's
            # drop_duplicates("vehicle_id") counts it as a sampled vehicle
            # instance (ev_mapping_ev_municipality).
            part = raw.iloc[[0]]
        k = len(part)
        # np.tile (not pd.concat([part] * len(others))) for the shared
        # charging-row content: repeating the underlying numpy/arrow buffers
        # directly is far cheaper than re-concatenating len(others) copies
        # of the same small DataFrame, especially since len(others) is
        # exactly where this function used to spend nearly all its time.
        tiled = part.iloc[np.tile(np.arange(k), len(others))].reset_index(drop=True)
        tiled["car_type"] = car_type
        tiled["vehicle_id"] = np.repeat([o["vehicle_id"] for o in others], k)
        tiled["Type"] = vehicle_type
        tiled["source_file"] = sf_str
        group_blocks.append(tiled)

    vp = pd.concat(group_blocks, ignore_index=True, sort=False)
    del group_blocks

    charging_events = vp.loc[vp["station_charging_capacity"] != 0].copy()
    # Stable positional link back into vp, captured BEFORE the reset_index()
    # below discards it - lets _write_vehicle_profiles() write each event's
    # resolved location_id back by direct position instead of re-matching on
    # (vehicle_id, event_start, event_time). That match was fragile:
    # use_case_helpers.park_time_limitation() (config charging_time_limit)
    # shortens event_time for a fraction of "street" events for placement
    # purposes only, so vp keeps the ORIGINAL event_time while the
    # placed/saved event carries the SHORTENED one - the two no longer
    # agree, so the old key-based merge silently failed to find these rows
    # and their location got dropped even though placement succeeded
    # internally (confirmed: ~16% of one Gemeinde's "street" events had
    # event_time changed, matching the ~18% of "street" charging events
    # found unmapped in the final output).
    charging_events["profile_row_index"] = charging_events.index
    charging_events["charging_use_case"] = charging_events["charging_use_case"].where(
        charging_events["charging_use_case"].notna(), charging_events["use_case"]
    )
    # A plain, Gemeinde-local sequential int - not exposed anywhere outside
    # this one run_de.py process (the actual target-format event_id is a
    # separate, globally-assigned scheme built later in restructure_output.py
    # - see its module docstring). Only ever used as a merge/isin key within
    # this Gemeinde's own processing (run_de.py's _write_vehicle_profiles),
    # so uniqueness is all it needs, not a human-readable AGS-prefixed
    # string. A Python list comprehension doing per-row string formatting
    # here was a genuine bottleneck for a megacity - Berlin 2037 alone has on
    # the order of 100M+ charging-event rows (849,190 vehicles x ~170
    # events/vehicle on average, per the 2024 nationwide run's ratio).
    charging_events["event_id"] = np.arange(len(charging_events), dtype=np.int64)
    charging_events = charging_events.reset_index(drop=True)

    return charging_events, vp
