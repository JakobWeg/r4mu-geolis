"""Retroactively detect and/or fix is_synthetic_location in an already-
consolidated ev_charging_location.parquet, without re-running run_de.py or
restructure_output.py - the flag is a pure function of candidate_uid, so a
past delivery can be patched in place.

Usage:
    # just report how many rows are mis-flagged, change nothing
    python fix_is_synthetic_location.py --normalized_dir results/normalized_DE_2024

    # detect AND overwrite the file with the corrected flag
    python fix_is_synthetic_location.py --normalized_dir results/normalized_DE_2024 --fix
"""
import argparse
import pathlib

import geopandas as gpd

import run_de


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--normalized_dir", required=True)
    parser.add_argument("--fix", action="store_true", help="overwrite the file with the corrected flag (default: report only)")
    args = parser.parse_args()

    path = pathlib.Path(args.normalized_dir) / "ev_charging_location.parquet"
    gdf = gpd.read_parquet(path)

    uid = gdf["candidate_uid"].astype(str)
    expected = (
        uid.str.startswith(run_de.HPC_HIGHWAY_CENTER_PREFIX)
        | uid.str.startswith(run_de.SYNTHETIC_CENTER_PREFIX)
    )
    mismatch = gdf["is_synthetic_location"].astype(bool) != expected
    n_mismatch = int(mismatch.sum())
    print(f"--- {path}: {n_mismatch:,} of {len(gdf):,} rows mis-flagged ---")

    if n_mismatch and args.fix:
        gdf["is_synthetic_location"] = expected
        gdf.to_parquet(path, index=False)
        print(f"--- fixed and overwrote {path} ---")
    elif n_mismatch:
        print("--- pass --fix to overwrite the file with the corrected flag ---")
    else:
        print("--- nothing to fix ---")


if __name__ == "__main__":
    main()
