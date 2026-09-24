"""Multi-year scenario chains (e.g. 2024 -> 2037 -> 2045): keeps charging
location identity consistent across separate run_de.py invocations.

Two pieces:
- merge_previous_scenario_into_candidates(): pre-seeds a candidate layer's
  existing_points/existing_capacity from a prior scenario's own output,
  matched by exact candidate_uid (data_de_candidates.py's stable per-
  candidate id) - not a spatial nearest-neighbor match like
  existing_infrastructure.py's BNetzA handling, since a prior scenario's
  locations are literally our own candidate points, so identity is exact.
  Reuses the same "fill existing first, weighted by point count" logic in
  distribute_charging_events() - no separate placement logic needed here.
- LocationRegistry: a small persistent id ledger (candidate_uid ->
  location_id) that grows across scenario runs, so a location already
  placed in an earlier scenario keeps the exact same global location_id
  when it reappears (grown or not) in a later one; only genuinely new
  locations get new ids.
"""
import pathlib

import geopandas as gpd
import numpy as np
import pandas as pd


def merge_previous_scenario_into_candidates(candidates: gpd.GeoDataFrame,
                                             previous_locations: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """previous_locations: the prior scenario's ev_charging_location rows for
    this one layer (columns: candidate_uid, charging_points,
    average_charging_capacity, geometry). Locations may also grow beyond
    these seeded values in this run if demand exceeds them - existing_points/
    existing_capacity_column only set the starting point.

    A previous-scenario candidate_uid with NO matching row in `candidates`
    at all is re-appended as its own new row (using its saved geometry),
    not just seeded onto an existing one - this is real infrastructure that
    data_de_candidates.py's regular layers never had a matching candidate
    point for in the first place, so it only exists as a synthetic row
    appended during whichever earlier scenario year actually applied
    existing_infrastructure_path (see existing_infrastructure.py's
    merge_existing_into_candidates() - same "unmatched -> append as new
    candidate row" pattern, just one scenario year later). Confirmed this
    was previously being silently dropped for EVERY such location (923/923
    in one test run) as soon as the NEXT scenario year stopped applying
    BNetzA directly and switched to previous_scenario_dir instead - this
    function used to only ever seed columns onto rows already present, never
    add rows for candidate_uids it doesn't recognize.
    """
    if previous_locations.empty:
        return candidates

    candidates = candidates.reset_index(drop=True).copy()
    lookup = previous_locations.drop_duplicates("candidate_uid").set_index("candidate_uid")
    matched_points = candidates["candidate_uid"].map(lookup["charging_points"])
    matched_capacity = candidates["candidate_uid"].map(lookup["average_charging_capacity"])

    candidates["existing_points"] = matched_points.fillna(0).astype(int)
    candidates["existing_capacity_kw"] = matched_capacity.fillna(0.0)

    orphaned = lookup.loc[~lookup.index.isin(candidates["candidate_uid"])]
    if not orphaned.empty:
        new_rows = gpd.GeoDataFrame(
            {"candidate_uid": orphaned.index.to_numpy(),
             "existing_points": orphaned["charging_points"].to_numpy(),
             "existing_capacity_kw": orphaned["average_charging_capacity"].to_numpy()},
            geometry=orphaned.geometry.to_numpy(), crs=candidates.crs,
        )
        for col in candidates.columns:
            if col not in new_rows.columns:
                new_rows[col] = np.nan
        candidates = pd.concat([candidates, new_rows[candidates.columns]], ignore_index=True)

    return candidates


class LocationRegistry:
    """Persistent candidate_uid -> location_id ledger, one file shared by an
    entire scenario chain (not per-run) - load it once per run_de.py
    invocation, call assign_ids() as locations are finalized, save() once at
    the end.
    """

    def __init__(self, path):
        self.path = pathlib.Path(path)
        if self.path.is_file():
            self._table = pd.read_parquet(self.path)
        else:
            self._table = pd.DataFrame({"candidate_uid": pd.array([], dtype="string"),
                                         "location_id": pd.array([], dtype="int64")})
        self._lookup = dict(zip(self._table["candidate_uid"], self._table["location_id"]))
        self._next_id = int(self._table["location_id"].max()) + 1 if len(self._table) else 0
        self._new_rows = []

    def assign_ids(self, candidate_uids: pd.Series) -> np.ndarray:
        """One location_id per input candidate_uid - existing ones reuse
        their registered id, new ones get a fresh id and get registered."""
        # A missing candidate_uid is never a legitimate identity - confirmed
        # to silently collapse every such row onto one shared dict key (and
        # therefore one shared location_id) instead of erroring, which is
        # far worse than failing loudly here (see existing_infrastructure.py
        # merge_existing_into_candidates()'s unmatched-BNetzA-facility fix).
        assert not candidate_uids.isna().any(), "candidate_uid must never be NaN/missing"
        out = np.empty(len(candidate_uids), dtype=np.int64)
        uids = candidate_uids.to_numpy()
        for i, uid in enumerate(uids):
            loc_id = self._lookup.get(uid)
            if loc_id is None:
                loc_id = self._next_id
                self._next_id += 1
                self._lookup[uid] = loc_id
                self._new_rows.append((uid, loc_id))
            out[i] = loc_id
        return out

    def save(self) -> None:
        if not self._new_rows:
            return
        new_df = pd.DataFrame(self._new_rows, columns=["candidate_uid", "location_id"]).astype(
            {"candidate_uid": "string", "location_id": "int64"})
        self._table = pd.concat([self._table, new_df], ignore_index=True)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._table.to_parquet(self.path, engine="pyarrow", index=False)
        self._new_rows = []
