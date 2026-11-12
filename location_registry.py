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
import os
import pathlib
import re

import geopandas as gpd
import numpy as np
import pandas as pd

import utility


# Registry keys are "<use_case>::<candidate_uid>". Both the internal use case
# names (public, hpc_urban, hpc_highway - what the registry actually holds) and
# the output spec names are accepted. candidate_uids only ever consist of
# letters, digits and underscores, so any other character is a corrupted key.
KEY_USE_CASES = ("home_detached", "home_apartment", "work", "retail", "depot", "public", "hpc_urban",
                 "hpc_highway", "street", "urban_fast", "highway_fast")
_KEY_PATTERN = re.compile(r"^(?:" + "|".join(KEY_USE_CASES) + r")::[A-Za-z0-9_]+$")


def validate_registry_keys(keys, where: str) -> None:
    """Fail loudly on a malformed registry key instead of silently carrying it
    on. A single-bit flip in memory ('home_apartme~t::...') once got written to
    disk and then made the real key look unknown in the next scenario year, so
    that location silently received a second location_id."""
    keys = pd.Series(keys, dtype="string")
    bad = keys[~keys.str.match(_KEY_PATTERN, na=False)]
    if not bad.empty:
        raise ValueError(f"{len(bad)} invalid location registry key(s) in {where}, e.g. {bad.head(5).tolist()}")


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


def seed_home_street_columns(candidates: gpd.GeoDataFrame,
                              previous_home_street: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Adds street_existing_points / street_existing_capacity_kw to the
    home_apartment layer from the previous scenario's *home_street* locations
    (use_case "street", candidate_uid "home_apartment_<N>": residential
    buildings that street-parking residents charged at).

    Why separate columns instead of existing_points/existing_capacity_kw: that
    pair on the home_apartment layer is the apartment-wallbox seed of
    home_apartment() itself. public()'s home_street pass places a different,
    independent event population onto the same buildings, so it needs its own
    seed - mixing the two would double-count. Matching is by exact
    candidate_uid. Previously these street locations were instead fed back into
    the public_poi (not-home) pool as orphaned candidates, so non-home events
    (work, shopping, ...) ended up on residential buildings, and the
    home_street pass itself restarted from zero every scenario year.
    """
    candidates = candidates.reset_index(drop=True).copy()
    if previous_home_street is None or previous_home_street.empty:
        candidates["street_existing_points"] = 0
        candidates["street_existing_capacity_kw"] = 0.0
        return candidates
    lookup = previous_home_street.drop_duplicates("candidate_uid").set_index("candidate_uid")
    candidates["street_existing_points"] = (
        candidates["candidate_uid"].map(lookup["charging_points"]).fillna(0).astype(int))
    candidates["street_existing_capacity_kw"] = (
        candidates["candidate_uid"].map(lookup["average_charging_capacity"]).fillna(0.0))
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
        if len(self._table):
            validate_registry_keys(self._table["candidate_uid"], f"{self.path} (on load)")
            assert self._table["location_id"].is_unique, f"duplicate location_id in {self.path}"
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
        validate_registry_keys(new_df["candidate_uid"], "new registry rows (before save)")
        table = pd.concat([self._table, new_df], ignore_index=True)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Atomic: write a temp file next to the registry, then replace in one
        # step - an interrupted/failed write can no longer leave a truncated
        # or half-updated registry behind.
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        table.to_parquet(tmp_path, engine="pyarrow", index=False)
        utility.replace_with_retry(tmp_path, self.path)
        self._table = table
        self._new_rows = []
