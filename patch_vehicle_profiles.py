"""Adds the 'average_charging_power' column back to an already-completed
run_de.py run's vehicle_profiles/<ags>.parquet files.

That column used to be dropped in vehicle_input.py:load_and_tag_profile() to
save space; it's now kept for new runs, but the existing nationwide result
was generated before that change. Re-running the whole simulation just for
one extra pass-through column would cost hours - this only re-does the cheap
part (re-sampling + reading the raw SimBEV CSVs, which is deterministic
given the same seed/AGS) in parallel, then merges the one new column into
the existing per-Gemeinde files by position.

Row-order safety: vehicle_profiles/<ags>.parquet was built as
pd.concat([load_and_tag_profile(v) for v in sample_vehicles(...)]), then
left-merged with location_id on (vehicle_id, event_start, event_time) - a
left merge with a unique join key preserves row order/count exactly. So
re-doing sample_vehicles + load_and_tag_profile with the same seed
reproduces the identical row sequence; event_start is compared per Gemeinde
as a cheap alignment check before trusting the position-based assignment.

Usage:
    python patch_vehicle_profiles.py --result_dir results/_DE_26-08-25_122428 \
        --config_file scenario/config_DE.cfg
"""
import argparse
import multiprocessing as mp
import pathlib
import traceback
import warnings

import numpy as np
import pandas as pd

import run_de
import vehicle_input as vi

_WORKER = {}


def _init_worker(demand_df, pool_index, seed, vp_dir):
    _WORKER.update(demand_df=demand_df, pool_index=pool_index, seed=seed, vp_dir=vp_dir)


def patch_gemeinde(ags: str):
    try:
        return _patch_gemeinde(ags)
    except Exception:
        warnings.warn(f"Gemeinde {ags}: patch failed, skipping:\n{traceback.format_exc()}")
        return ags, False


def _patch_gemeinde(ags: str):
    demand_df = _WORKER["demand_df"]
    pool_index = _WORKER["pool_index"]
    seed = _WORKER["seed"]
    vp_path = _WORKER["vp_dir"] / f"{ags}.parquet"
    if not vp_path.is_file():
        return ags, False

    vp = pd.read_parquet(vp_path)
    if "average_charging_power" in vp.columns:
        return ags, False  # already patched

    gemeinde_demand = demand_df.loc[demand_df["AGS"] == ags]
    if gemeinde_demand.empty:
        return ags, False
    regiostar7 = gemeinde_demand["RegioStaR7"].iloc[0]

    vehicles = vi.sample_vehicles(ags, regiostar7, demand_df, pool_index, seed)
    if not vehicles:
        return ags, False
    profiles = [vi.load_and_tag_profile(v) for v in vehicles]
    fresh = pd.concat(profiles, ignore_index=True, sort=False)

    if len(fresh) != len(vp) or not (fresh["event_start"].to_numpy() == vp["event_start"].to_numpy()).all():
        warnings.warn(f"Gemeinde {ags}: re-sampled profile doesn't align with existing vehicle_profiles - skipping")
        return ags, False

    vp["average_charging_power"] = fresh["average_charging_power"].to_numpy()
    vp.to_parquet(vp_path, engine="pyarrow", index=False)
    return ags, True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--config_file", default="scenario/config_DE.cfg")
    args = parser.parse_args()

    config = run_de.parse_config(pathlib.Path(args.config_file))
    demand_df = vi.load_municipality_demand(config["vehicle_input_file"])
    pool_index = vi.build_pool_index(config["simbev_pool_dir"])

    vp_dir = pathlib.Path(args.result_dir) / "vehicle_profiles"
    ags_list = sorted(p.stem for p in vp_dir.glob("*.parquet"))
    print(f"--- patching {len(ags_list)} vehicle_profiles files with {config['n_workers']} workers ---")

    patched = 0
    with mp.Pool(config["n_workers"], initializer=_init_worker,
                 initargs=(demand_df, pool_index, config["random_seed"], vp_dir)) as pool:
        for i, (ags, did_patch) in enumerate(pool.imap_unordered(patch_gemeinde, ags_list), start=1):
            patched += did_patch
            print(f"--- {i}/{len(ags_list)} done ({ags}, patched={patched}) ---")

    print(f"--- done: patched {patched}/{len(ags_list)} files ---")


if __name__ == "__main__":
    main()
