"""Chunked/checkpointed alternative to restructure_output.py's DE-wide
consolidation - same target-format output, reuses restructure_output.py's
own process_gemeinde() (domain logic untouched), but a fundamentally
different PROCESS architecture.

Why this exists: restructure_output.py's full nationwide 2037 consolidation
has crashed 4 times (2026-09-17 to 2026-09-19), each at a different,
non-reproducible Gemeinde, with no OS-level explanation (no reboot, no
resource-exhaustion event, no WER report for 3 of the 4 crashes). The one
crash that DID leave a trace was a genuine STATUS_ACCESS_VIOLATION inside
python313.dll. That process runs ALL 10,747 Gemeinden through ONE long-lived
interpreter, with one Arrow-backed ParquetWriter and one ever-growing
profile_registry dict held for the entire multi-hour run.

restructure_output.py's OWN process_gemeinde() docstring already documents a
related, previously-diagnosed failure mode: pandas' Arrow-backed string
dtype raising spurious ArrowMemoryError/ArrowException "partway through a
long consolidation run... looks like Arrow memory-pool fragmentation after
hundreds of prior Gemeinden in the same process, not anything about the
failing Gemeinde's own data - a standalone rerun of the same file always
succeeds." That is an already-confirmed precedent for a LONG-RUNNING-PROCESS-
DEPENDENT corruption mechanism (pyarrow's C++ memory pool), not hardware -
this script is built on the working assumption that the harder, unexplained
crashes are the same class of problem, just manifesting differently (as a
hard access violation instead of a catchable ArrowException).

Architecture: Gemeinden are processed in small, fixed-size batches, each one
in its OWN short-lived subprocess (fresh interpreter, fresh Arrow memory
pool, fresh everything - torn down completely when the batch finishes). No
single process ever accumulates more than one batch's worth of pandas/Arrow
activity before being replaced. A crash in one batch only loses that one
batch's work (retried a bounded number of times before giving up loudly),
never the whole multi-hour run. The three small pieces of state that must
survive across batches (profile_registry, running id offsets, the location
registry) are checkpointed to disk between batches instead of kept as
in-process globals for the whole run.

Usage (same required args as restructure_output.py, plus --batch_size):
    python restructure_output_chunked.py --result_dir results/_DE_2037_merged \\
        --vehicle_input_file scenario/input_vehicles_2037.xlsx \\
        --out_dir results/normalized_DE_2037_v4 \\
        --location_registry_path data_DE/location_registry.parquet \\
        --config_file scenario/config_DE_2037.cfg --batch_size 200
"""
import argparse
import configparser as cp
import datetime
import json
import pathlib
import subprocess
import sys
import time
import traceback
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import location_registry as lr
import restructure_output as ro
import vehicle_input as vi

MAX_BATCH_RETRIES = 6


# --------------------------------------------------------------------------
# Small on-disk checkpoints - all tiny (profile_registry is bounded by the
# SimBEV pool's fixed size, ~7,200 profiles x 7 RegioStaR7 types x ~9 car
# type prefixes, so at most a few hundred thousand rows nationwide - a
# sub-second parquet write/read regardless of how many Gemeinden have been
# processed so far, unlike the vehicle/event tables which scale with demand).
# --------------------------------------------------------------------------

def _load_state(checkpoint_dir: pathlib.Path):
    offsets_path = checkpoint_dir / "offsets.json"
    registry_path = checkpoint_dir / "profile_registry.parquet"
    if offsets_path.is_file():
        offsets = json.loads(offsets_path.read_text())
    else:
        offsets = {"ev": 0, "event": 0, "mapping_muni": 0, "mapping_event_loc": 0, "location": 0}
    if registry_path.is_file():
        df = pd.read_parquet(registry_path)
        profile_registry = {
            row.source_file: {"ev_id": int(row.ev_id), "event_id_start": int(row.event_id_start)}
            for row in df.itertuples()
        }
    else:
        profile_registry = {}
    return offsets, profile_registry


def _save_state(checkpoint_dir: pathlib.Path, offsets: dict, profile_registry: dict):
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    (checkpoint_dir / "offsets.json").write_text(json.dumps(offsets))
    if profile_registry:
        df = pd.DataFrame(
            [{"source_file": sf, "ev_id": v["ev_id"], "event_id_start": v["event_id_start"]}
             for sf, v in profile_registry.items()]
        )
        # Atomic-ish: write to a temp file then replace, so a crash mid-write
        # can never leave a half-written profile_registry.parquet that the
        # NEXT batch would silently load as truncated/corrupt.
        tmp = checkpoint_dir / "profile_registry.parquet.tmp"
        df.to_parquet(tmp, engine="pyarrow", index=False)
        tmp.replace(checkpoint_dir / "profile_registry.parquet")


def _batch_done_marker(checkpoint_dir: pathlib.Path, batch_index: int) -> pathlib.Path:
    return checkpoint_dir / f"batch_{batch_index:05d}.done"


def _batch_shard_paths(checkpoint_dir: pathlib.Path, batch_index: int) -> dict:
    stem = f"batch_{batch_index:05d}"
    return {
        "ev_pool": checkpoint_dir / f"{stem}_ev_pool.parquet",
        "mapping_muni": checkpoint_dir / f"{stem}_mapping_muni.parquet",
        "location": checkpoint_dir / f"{stem}_location.parquet",
        "event": checkpoint_dir / f"{stem}_event.parquet",
        "mapping_event_loc": checkpoint_dir / f"{stem}_mapping_event_loc.parquet",
    }


# --------------------------------------------------------------------------
# Worker: processes exactly one batch of Gemeinden, in its own subprocess,
# then exits. Never called with more than --batch_size Gemeinden.
# --------------------------------------------------------------------------

def run_worker(args):
    checkpoint_dir = pathlib.Path(args.checkpoint_dir)
    batches = json.loads((checkpoint_dir / "batches.json").read_text())
    ags_list = batches[args.batch_index]

    offsets, profile_registry = _load_state(checkpoint_dir)
    registry = lr.LocationRegistry(pathlib.Path(args.location_registry_path))

    demand_df = vi.load_municipality_demand(args.vehicle_input_file)
    ags_to_rs7 = dict(zip(demand_df["AGS"], demand_df["RegioStaR7"].map(ro.RS7_CODE)))

    result_dir = pathlib.Path(args.result_dir)
    gemeinden_dir = result_dir / "gemeinden"
    vp_dir = result_dir / "vehicle_profiles"

    ev_pool_batches, mapping_muni_batches, location_batches = [], [], []
    event_frames, mapping_event_loc_frames = [], []

    for ags in ags_list:
        vp_path = vp_dir / f"{ags}.parquet"
        if not vp_path.is_file():
            continue
        rs7_code = ags_to_rs7.get(ags)
        if rs7_code is None:
            print(f"WARNING: no RegioStaR7 code for AGS {ags}, skipping", flush=True)
            continue
        try:
            (ev_pool_rows, mapping_muni_rows, location_rows,
             ev_event_rows, mapping_event_loc_rows) = ro.process_gemeinde(
                ags, gemeinden_dir / ags, vp_path, rs7_code, offsets, registry, profile_registry, args.eta_cp
            )
        except Exception:
            warnings.warn(f"Gemeinde {ags}: consolidation failed, skipping:\n{traceback.format_exc()}")
            continue
        ev_pool_batches.append(ev_pool_rows)
        mapping_muni_batches.append(mapping_muni_rows)
        if location_rows is not None:
            location_batches.append(location_rows)
        event_frames.append(ev_event_rows)
        mapping_event_loc_frames.append(mapping_event_loc_rows)

    shard_paths = _batch_shard_paths(checkpoint_dir, args.batch_index)

    ev_pool_all = pd.concat(ev_pool_batches, ignore_index=True) if ev_pool_batches else pd.DataFrame(
        {"ev_id": pd.array([], dtype="int32"), "rs7_id": pd.array([], dtype="int8"), "type": pd.array([], dtype="object")})
    pq.write_table(pa.Table.from_pandas(ev_pool_all, preserve_index=False), shard_paths["ev_pool"])

    mapping_muni_all = pd.concat(mapping_muni_batches, ignore_index=True) if mapping_muni_batches else pd.DataFrame(
        {"id": pd.array([], dtype="int64"), "ev_id": pd.array([], dtype="int32"), "ags": pd.array([], dtype="int32")})
    pq.write_table(pa.Table.from_pandas(mapping_muni_all, preserve_index=False), shard_paths["mapping_muni"])

    if location_batches:
        all_locations = gpd.GeoDataFrame(pd.concat(location_batches, ignore_index=True),
                                          geometry="geometry", crs=location_batches[0].crs)
        all_locations.to_parquet(shard_paths["location"])

    event_all = (pd.concat(event_frames, ignore_index=True) if event_frames
                 else pd.DataFrame({c: pd.array([], dtype=dt) for c, dt in ro.EV_EVENT_DTYPES.items()}))
    event_table = pa.Table.from_pandas(event_all, preserve_index=False)
    event_table = event_table.cast(pa.schema([
        f.with_type(pa.large_string()) if pa.types.is_string(f.type) else f for f in event_table.schema
    ]))
    pq.write_table(event_table, shard_paths["event"], row_group_size=1_000_000, **ro.EV_EVENT_ENCODING)

    mapping_event_loc_all = (pd.concat(mapping_event_loc_frames, ignore_index=True) if mapping_event_loc_frames
                              else pd.DataFrame({"id": pd.array([], dtype="int64"), "event_id": pd.array([], dtype="int32"),
                                                  "location_id": pd.array([], dtype="int32"), "use_case": pd.array([], dtype="object")}))
    mapping_table = pa.Table.from_pandas(mapping_event_loc_all, preserve_index=False)
    mapping_table = mapping_table.cast(pa.schema([
        f.with_type(pa.large_string()) if pa.types.is_string(f.type) else f for f in mapping_table.schema
    ]))
    pq.write_table(mapping_table, shard_paths["mapping_event_loc"], row_group_size=1_000_000, **ro.MAPPING_EVENT_LOC_ENCODING)

    registry.save()
    _save_state(checkpoint_dir, offsets, profile_registry)
    _batch_done_marker(checkpoint_dir, args.batch_index).write_text(datetime.datetime.now().isoformat())
    print(f"--- batch {args.batch_index} done ({len(ags_list)} Gemeinden, "
          f"offsets={offsets}) ---", flush=True)


# --------------------------------------------------------------------------
# Orchestrator: splits ags_list into batches, launches one fresh subprocess
# per batch (sequentially - id assignment must stay in strict AGS order),
# retries a crashed batch a bounded number of times, then merges all shards.
# --------------------------------------------------------------------------

def _stream_merge_parquet(shard_files: list, out_path: pathlib.Path, encoding: dict = None):
    """Copies each shard's row groups straight through to one output file,
    never materializing more than one row group in memory at a time - unlike
    a pd.concat of everything, this stays cheap regardless of how many
    shards or how large the final file is."""
    encoding = encoding or {}
    writer = None
    try:
        for shard in shard_files:
            pf = pq.ParquetFile(shard)
            if pf.metadata.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(out_path, pf.schema_arrow, **encoding)
            for batch in pf.iter_batches():
                writer.write_table(pa.Table.from_batches([batch]))
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        # No shard had any rows - still produce an empty, correctly-schema'd
        # file rather than none at all, using the first shard's (empty) schema.
        pf = pq.ParquetFile(shard_files[0])
        pq.write_table(pf.read(), out_path, **encoding)


def run_orchestrator(args):
    result_dir = pathlib.Path(args.result_dir)
    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = pathlib.Path(args.checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    gemeinden_dir = result_dir / "gemeinden"
    ags_list = sorted(p.name for p in gemeinden_dir.iterdir() if p.is_dir())
    if args.limit:
        ags_list = ags_list[: args.limit]

    batches = [ags_list[i:i + args.batch_size] for i in range(0, len(ags_list), args.batch_size)]
    batches_path = checkpoint_dir / "batches.json"
    if not batches_path.is_file():
        batches_path.write_text(json.dumps(batches))
    else:
        # Resuming a previous run of this orchestrator - trust the ALREADY
        # persisted batch split (which any already-.done batches' shards are
        # keyed against) rather than silently re-splitting with possibly
        # different boundaries if the raw Gemeinde count changed underneath.
        batches = json.loads(batches_path.read_text())

    print(f"--- {len(ags_list)} Gemeinden in {len(batches)} batches of up to {args.batch_size} ---", flush=True)

    for batch_index in range(len(batches)):
        if _batch_done_marker(checkpoint_dir, batch_index).is_file():
            continue
        for attempt in range(1, MAX_BATCH_RETRIES + 1):
            print(f"--- launching batch {batch_index}/{len(batches) - 1} (attempt {attempt}) ---", flush=True)
            t0 = time.monotonic()
            proc = subprocess.run(
                [sys.executable, "-u", __file__,
                 "--worker",
                 "--batch_index", str(batch_index),
                 "--checkpoint_dir", str(checkpoint_dir),
                 "--result_dir", str(args.result_dir),
                 "--out_dir", str(args.out_dir),
                 "--vehicle_input_file", str(args.vehicle_input_file),
                 "--location_registry_path", str(args.location_registry_path),
                 "--eta_cp", str(args.eta_cp)],
            )
            elapsed = time.monotonic() - t0
            if proc.returncode == 0 and _batch_done_marker(checkpoint_dir, batch_index).is_file():
                print(f"--- batch {batch_index} succeeded in {elapsed:.0f}s ---", flush=True)
                break
            print(f"--- batch {batch_index} FAILED (exit code {proc.returncode}, {elapsed:.0f}s) ---", flush=True)
            if attempt == MAX_BATCH_RETRIES:
                raise RuntimeError(
                    f"batch {batch_index} (AGS {batches[batch_index][0]}..{batches[batch_index][-1]}) "
                    f"failed {MAX_BATCH_RETRIES} times in a row - stopping rather than silently retrying "
                    f"forever. Fix the underlying issue (see this batch's own stderr above) and re-run "
                    f"this same command - already-completed batches are skipped automatically.")

    print("--- all batches done, merging shards into final output ---", flush=True)
    shard_lists = {key: [] for key in ["ev_pool", "mapping_muni", "location", "event", "mapping_event_loc"]}
    for batch_index in range(len(batches)):
        paths = _batch_shard_paths(checkpoint_dir, batch_index)
        for key, path in paths.items():
            if path.is_file():
                shard_lists[key].append(path)

    _stream_merge_parquet(shard_lists["event"], out_dir / "ev_event.parquet", ro.EV_EVENT_ENCODING)
    _stream_merge_parquet(shard_lists["mapping_event_loc"], out_dir / "ev_mapping_event_location.parquet", ro.MAPPING_EVENT_LOC_ENCODING)

    ev_pool_all = pd.concat([pd.read_parquet(p) for p in shard_lists["ev_pool"]], ignore_index=True)
    # A profile can appear as a "new" ev_pool row in more than one batch's
    # shard only if something upstream is already broken (profile_registry
    # is checkpointed between EVERY batch specifically to prevent this) -
    # deduplicated defensively rather than trusted blindly, since silently
    # writing the same ev_id twice into ev_pool would be a correctness bug,
    # not just a cosmetic one.
    ev_pool_all = ev_pool_all.drop_duplicates("ev_id", keep="first").reset_index(drop=True)
    pq.write_table(pa.Table.from_pandas(ev_pool_all, preserve_index=False), out_dir / "ev_pool.parquet",
                    use_dictionary=["rs7_id", "type"], column_encoding={"ev_id": "DELTA_BINARY_PACKED"}, version="2.6")

    mapping_muni_all = pd.concat([pd.read_parquet(p) for p in shard_lists["mapping_muni"]], ignore_index=True)
    pq.write_table(pa.Table.from_pandas(mapping_muni_all, preserve_index=False), out_dir / "ev_mapping_ev_municipality.parquet",
                    use_dictionary=["ags", "ev_id"], column_encoding={"id": "DELTA_BINARY_PACKED"}, version="2.6")

    final_n_locations = None
    if shard_lists["location"]:
        loc_frames = [gpd.read_parquet(p) for p in shard_lists["location"]]
        all_locations = gpd.GeoDataFrame(pd.concat(loc_frames, ignore_index=True), geometry="geometry", crs=loc_frames[0].crs)
        if all_locations["location_id"].duplicated().any():
            merged = all_locations.groupby("location_id", as_index=False).agg({
                "charging_points": "sum", "average_charging_capacity": "max", "use_case": "first",
                "candidate_uid": "first", "is_synthetic_location": "first", "geometry": "first",
            })
            all_locations = gpd.GeoDataFrame(merged, geometry="geometry", crs=loc_frames[0].crs)
        loc_dictionary_cols = ["charging_points", "average_charging_capacity", "use_case"]
        if args.drop_candidate_uid:
            all_locations = all_locations.drop(columns=["candidate_uid"])
        else:
            loc_dictionary_cols.append("candidate_uid")
        all_locations.to_parquet(out_dir / "ev_charging_location.parquet", use_dictionary=loc_dictionary_cols,
                                  column_encoding={"location_id": "DELTA_BINARY_PACKED"}, version="2.6")
        final_n_locations = len(all_locations)

    joined = mapping_muni_all.merge(ev_pool_all[["ev_id", "rs7_id", "type"]], on="ev_id")
    counts = joined.pivot_table(index="ags", columns="type", values="id", aggfunc="count", fill_value=0)
    rs7_per_ags = joined.groupby("ags")["rs7_id"].first()
    car_type_order = [c for c in vi.CAR_TYPE_COLUMNS.values() if c in counts.columns]
    ev_count_municipality = counts[car_type_order].reset_index()
    ev_count_municipality[car_type_order] = ev_count_municipality[car_type_order].astype(np.int32)
    ev_count_municipality["rs7_id"] = ev_count_municipality["ags"].map(rs7_per_ags).astype(np.int8)
    ev_count_municipality.to_parquet(out_dir / "ev_count_municipality.parquet", engine="pyarrow", index=False)

    offsets, _ = _load_state(checkpoint_dir)
    print(f"--- done: {offsets['mapping_muni']} sampled vehicle instances ({offsets['ev']} distinct EVs/pool profiles, "
          f"{offsets['event']} event rows), {offsets['location']} locations, "
          f"{offsets['mapping_event_loc']} event-location mappings ---", flush=True)

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
            import shutil
            shutil.copyfile(simbev_metadata_src, out_dir / "metadata_simbev_run.json")
        else:
            warnings.warn(f"metadata_simbev_run.json not found at {simbev_metadata_src} - "
                           f"not copied into {out_dir}")
    geolis_metadata = {
        "scenario_name": cfg.get("data", "scenario_name", fallback=None) if cfg.has_section("data") else None,
        "vehicle_input_file": str(args.vehicle_input_file),
        "simbev_pool_dir": simbev_pool_dir,
        "previous_scenario_dir": cfg.get("data", "previous_scenario_dir", fallback=None) if cfg.has_section("data") else None,
        "location_registry_path": str(args.location_registry_path),
        "random_seed": cfg.getint("basic", "random_seed", fallback=None) if cfg.has_section("basic") else None,
        "result_dir": str(result_dir),
        "consolidated_at": datetime.datetime.now().isoformat(),
        "n_vehicle_instances": int(offsets["mapping_muni"]),
        "n_distinct_evs": int(offsets["ev"]),
        "n_event_rows": int(offsets["event"]),
        "n_locations": int(final_n_locations) if final_n_locations is not None else int(offsets["location"]),
        "consolidation_method": "restructure_output_chunked.py (batched/checkpointed)",
    }
    with open(out_dir / "metadata_geolis_run.json", "w") as f:
        json.dump(geolis_metadata, f, indent=2, default=str)
    print("--- consolidation complete ---", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--vehicle_input_file", default="scenario/input_vehicles_2024.xlsx")
    parser.add_argument("--out_dir", required=True)
    parser.add_argument("--config_file", default="scenario/config_DE.cfg")
    parser.add_argument("--location_registry_path", required=True)
    parser.add_argument("--drop_candidate_uid", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch_size", type=int, default=200,
                         help="Gemeinden per subprocess batch - small enough that even a megacity landing "
                              "in one batch doesn't dominate it, large enough to keep subprocess-launch "
                              "overhead negligible (~54 batches for the full 10,747-Gemeinde nationwide run)")
    parser.add_argument("--checkpoint_dir", default=None,
                         help="defaults to <out_dir>_chunks - holds per-batch shards and resume state, "
                              "safe to delete only after a full successful run")
    # --worker/--batch_index/--eta_cp: internal, used by the orchestrator to
    # relaunch this same script as a one-batch worker subprocess - not meant
    # to be passed by hand.
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--batch_index", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--eta_cp", type=float, default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.checkpoint_dir is None:
        args.checkpoint_dir = str(pathlib.Path(args.out_dir).parent / (pathlib.Path(args.out_dir).name + "_chunks"))

    if args.worker:
        run_worker(args)
    else:
        _early_cfg = cp.ConfigParser()
        if pathlib.Path(args.config_file).is_file():
            _early_cfg.read(args.config_file)
        args.eta_cp = _early_cfg.getfloat("basic", "eta_cp", fallback=0.9) if _early_cfg.has_section("basic") else 0.9
        run_orchestrator(args)


if __name__ == "__main__":
    main()
