"""One-time preprocessing of the nationwide data_DE candidate location layers
for run_de.py: load each layer once, reduce polygons to a representative
point, spatially join against the VG250 Gemeinde polygons, and write the
result as a Parquet dataset partitioned by AGS.

Workers then read just their own Gemeinde's partition (a tiny file) instead
of doing a spatial join per task. This also sidesteps Windows' multiprocessing
"spawn" behaviour, which would otherwise re-pickle the full in-memory,
nationwide candidate set into every worker process.
"""
import pathlib
import shutil

import geopandas as gpd
import pandas as pd

import existing_infrastructure as ei
import location_registry as lr
import utility

# Public use cases the BNetzA existing-infrastructure register can pre-seed
# (home/work are private charging, not covered by the public register), and
# the weight_column each one uses (see LAYER_CONFIG below).
EXISTING_INFRASTRUCTURE_LAYERS = {"retail", "public_poi", "hpc_urban", "hpc_rural"}
EXISTING_INFRASTRUCTURE_WEIGHT_COLUMN = {
    "retail": "weight",
    "public_poi": "weight",
    "hpc_urban": "weight",
    "hpc_rural": "weight",
}

# data_de_candidates layer key -> the use_case tag a previous scenario's
# ev_charging_location output tags that layer's locations with (used to look
# a layer's own prior-scenario locations back up - see LAYER_CONFIG below).
# This must be the SPEC/output name (what restructure_output.py's own
# USE_CASE_SPEC_NAMES already translated hpc_urban/hpc_highway/public to
# before writing ev_charging_location.parquet), NOT run_de.py's internal
# name - most layers don't need translation (name unchanged), but these
# three do. Confirmed bug (2026-09-08): this used to list the INTERNAL
# names ("hpc_urban", "hpc_highway", "public") here, so
# prepare_candidates()'s `prev_locations["use_case"] == use_case` filter
# matched ZERO rows for these three layers - previous_locations_by_layer
# came back empty, merge_previous_scenario_into_candidates() no-op'd on its
# own empty-input check, and existing_points/existing_capacity_kw were
# never seeded at all. Measured impact on the real 2024->2037 run: 45% of
# urban_fast's and 30% of street's previously-used real locations vanished
# because they got no Phase-1 reuse preference and had to compete as if
# brand new.
LAYER_KEY_TO_USE_CASE = {
    "home_apartment": "home_apartment",
    "home_detached": "home_detached",
    "work": "work",
    "retail": "retail",
    "depot": "depot",
    "public_poi": "street",
    "hpc_urban": "urban_fast",
    "hpc_rural": "highway_fast",
}

# Per use case: which data_dir file to load. Every file holds only point
# geometry and ONE weight column named "weight" (publication format
# {n}_{use_case}.gpkg, weights scaled to a max of 1, see geodata_minimal) -
# use_case.py reads that single column for every use case. A column named
# "Weight" is renamed on load.
LAYER_CONFIG = {
    "home_detached": {"file": "1_home_detached.gpkg"},
    "home_apartment": {"file": "2_home_apartment.gpkg"},
    "work": {"file": "3_work.gpkg"},
    "hpc_urban": {"file": "4a_urban_fast.gpkg"},
    "hpc_rural": {"file": "4b_highway_fast.gpkg"},
    "retail": {"file": "5_retail.gpkg"},
    "public_poi": {"file": "6_street.gpkg"},
    "depot": {"file": "7_depot.gpkg"},
}

TARGET_CRS = 3035

# candidate_uid prefix of the residential buildings that public()'s home_street
# pass places street-parking residents on (they come from the home_apartment layer).
HOME_STREET_UID_PREFIX = "home_apartment_"


def _to_representative_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    is_polygon = gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    if is_polygon.any():
        gdf = gdf.copy()
        gdf.loc[is_polygon, "geometry"] = gdf.loc[is_polygon, "geometry"].centroid
    return gdf


def load_layer(data_dir, layer_key: str) -> gpd.GeoDataFrame:
    path = pathlib.Path(data_dir) / LAYER_CONFIG[layer_key]["file"]
    gdf = gpd.read_file(path).rename(columns={"Weight": "weight"})
    if "weight" not in gdf.columns:
        raise ValueError(f"{path} has no 'weight' column (found: {list(gdf.columns)})")
    gdf = gdf[["weight", "geometry"]]
    gdf = _to_representative_points(gdf)
    # Deterministic per-candidate identity (layer + row position in the
    # source file), used to recognize "this is the same real-world
    # candidate" across separate scenario-year runs (see
    # location_registry.py) - a location placed here in a 2024 run and
    # reused in 2037 keeps the same global location_id. Only valid as long
    # as data_dir's *.gpkg files themselves don't change between scenario
    # years (same row order); if they do, this identity scheme needs
    # replacing with a spatial match like existing_infrastructure.py's.
    gdf = gdf.reset_index(drop=True)
    gdf["candidate_uid"] = [f"{layer_key}_{i}" for i in range(len(gdf))]
    return gdf


def assign_ags(gdf: gpd.GeoDataFrame, municipalities: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    joined = gpd.sjoin(gdf, municipalities[["AGS", "geometry"]], how="inner", predicate="within")
    return joined.drop(columns=["index_right"])


def prepare_candidates(data_dir, prepared_dir, municipalities: gpd.GeoDataFrame, layer_keys,
                        existing_infrastructure_path=None, previous_scenario_dir=None) -> None:
    """Load, spatially join and write each requested candidate layer as a
    Parquet dataset partitioned by AGS under prepared_dir/<layer_key>/.
    Skips layers that already have a prepared dataset on disk.

    If existing_infrastructure_path is given, retail/public_poi/hpc_urban/
    hpc_rural (the public use cases) are pre-seeded with existing capacity
    from the BNetzA Ladesaeulenregister before being written, so
    distribute_charging_events fills existing infrastructure first (see
    existing_infrastructure.py).

    If previous_scenario_dir is given (path to an earlier scenario year's
    results/normalized_.../ directory - see location_registry.py), every
    requested layer is instead (not also - see the assertion below) pre-
    seeded from that scenario's own already-placed locations for it, matched
    by exact candidate_uid. This is what keeps a multi-year scenario chain's
    charging locations consistent: a 2037 run built with
    previous_scenario_dir=.../normalized_DE_2024 fills 2024's locations
    first (which already include anything BNetzA contributed there) before
    proposing anything new, for ALL 8 use cases (not just the 4 BNetzA
    covers) - home/work locations need to persist across years too.
    """
    assert not (existing_infrastructure_path and previous_scenario_dir), (
        "existing_infrastructure_path and previous_scenario_dir would both "
        "write existing_points/existing_capacity_kw - pass only one (a later "
        "scenario's previous_scenario_dir already carries BNetzA forward "
        "from whichever earlier run applied it)"
    )
    prepared_dir = pathlib.Path(prepared_dir)

    existing_matches = {}
    base_cache = {}
    needs_existing = existing_infrastructure_path and (EXISTING_INFRASTRUCTURE_LAYERS & set(layer_keys))
    already_prepared = {lk for lk in layer_keys if (prepared_dir / lk).is_dir() and any((prepared_dir / lk).iterdir())}
    if needs_existing and (EXISTING_INFRASTRUCTURE_LAYERS & set(layer_keys)) - already_prepared:
        utility.safe_print("--- loading existing charging infrastructure (BNetzA Ladesaeulenregister) ---")
        existing_gdf = ei.load_existing_infrastructure(existing_infrastructure_path)
        base_cache = {lk: load_layer(data_dir, lk) for lk in EXISTING_INFRASTRUCTURE_LAYERS}
        existing_matches = ei.categorize_and_match(
            existing_gdf, base_cache["retail"], base_cache["public_poi"],
            base_cache["hpc_urban"], base_cache["hpc_rural"],
        )

    previous_locations_by_layer = {}
    previous_home_street = None
    if previous_scenario_dir and (set(layer_keys) - already_prepared):
        utility.safe_print(f"--- loading previous scenario's locations from {previous_scenario_dir} ---")
        # geometry is needed (not just the seeded columns) so a candidate_uid
        # orphaned in the current year's raw layers - real infrastructure
        # with no matching data_DE candidate point - can be re-appended as
        # its own row by merge_previous_scenario_into_candidates() instead of
        # silently dropped; gpd.read_parquet (not pd.read_parquet) to get it
        # back as real geometry rather than raw WKB bytes.
        prev_locations = gpd.read_parquet(
            pathlib.Path(previous_scenario_dir) / "ev_charging_location.parquet",
            columns=["candidate_uid", "charging_points", "average_charging_capacity", "use_case", "geometry"],
        )
        for layer_key, use_case in LAYER_KEY_TO_USE_CASE.items():
            previous_locations_by_layer[layer_key] = prev_locations[prev_locations["use_case"] == use_case]
        # use_case "street" holds two kinds of locations: real street POIs
        # (public_poi_*, bnetza_*, synthetic centres) and the home_street
        # locations of street-parking residents, which sit on residential
        # buildings of the home_apartment layer (candidate_uid
        # "home_apartment_<N>"). The latter must NOT be re-appended to the
        # public_poi pool as orphaned candidates (non-home events would be
        # placed on residential buildings, and the same building would turn up
        # in both populations); they seed public()'s home_street pass via the
        # home_apartment layer instead (see location_registry.
        # seed_home_street_columns).
        street_prev = previous_locations_by_layer.get("public_poi")
        if street_prev is not None:
            is_home_street = street_prev["candidate_uid"].astype(str).str.startswith(HOME_STREET_UID_PREFIX)
            previous_locations_by_layer["public_poi"] = street_prev[~is_home_street]
            previous_home_street = street_prev[is_home_street]

    for layer_key in layer_keys:
        layer_dir = prepared_dir / layer_key
        if layer_dir.is_dir() and any(layer_dir.iterdir()):
            utility.safe_print(f"--- candidate layer '{layer_key}' already prepared, skipping ---")
            continue

        utility.safe_print(f"--- preparing candidate layer '{layer_key}' ---")
        gdf = base_cache.get(layer_key) if layer_key in base_cache else load_layer(data_dir, layer_key)

        if layer_key in existing_matches:
            gdf = ei.merge_existing_into_candidates(
                gdf, existing_matches[layer_key], weight_column=EXISTING_INFRASTRUCTURE_WEIGHT_COLUMN[layer_key]
            )
        if layer_key in previous_locations_by_layer:
            gdf = lr.merge_previous_scenario_into_candidates(gdf, previous_locations_by_layer[layer_key])
        if layer_key == "home_apartment" and previous_scenario_dir:
            gdf = lr.seed_home_street_columns(gdf, previous_home_street)

        joined = assign_ags(gdf, municipalities)

        df = pd.DataFrame(joined.drop(columns=["geometry"]))
        df["x"] = joined.geometry.x.values
        df["y"] = joined.geometry.y.values
        # Sorted by Gemeinde (stable: keeps the source row order inside each
        # one): the writer opens a new file per partition per written batch,
        # so a spatially unsorted source file (rows of many Gemeinden mixed)
        # would produce hundreds of thousands of tiny files and hours of
        # write time instead of about one file per Gemeinde.
        df = df.sort_values("AGS", kind="stable").reset_index(drop=True)

        # Written to a temp directory and renamed at the end: a crash midway
        # used to leave a half-written layer dir that the "already prepared"
        # check above (dir exists and is non-empty) accepted on the next run.
        tmp_dir = prepared_dir / f"{layer_key}.__writing__"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(tmp_dir, engine="pyarrow", partition_cols=["AGS"], index=False, max_partitions=12000)
        if layer_dir.exists():
            shutil.rmtree(layer_dir)  # only an empty dir can be here (see check above)
        utility.replace_with_retry(tmp_dir, layer_dir)
        utility.safe_print(f"--- '{layer_key}': {len(df)}/{len(gdf)} candidates matched, written to {layer_dir} ---")


def load_candidates(prepared_dir, layer_key: str, ags: str) -> gpd.GeoDataFrame:
    """Read one Gemeinde's candidates for one layer. Empty GeoDataFrame with
    the right columns if this Gemeinde has none for this layer.
    """
    attr_columns = ["weight"]
    partition_dir = pathlib.Path(prepared_dir) / layer_key / f"AGS={ags}"

    if not partition_dir.is_dir():
        return gpd.GeoDataFrame(columns=attr_columns + ["geometry"], geometry="geometry", crs=TARGET_CRS)

    df = pd.read_parquet(partition_dir, engine="pyarrow")
    geometry = gpd.points_from_xy(df["x"], df["y"])
    gdf = gpd.GeoDataFrame(df.drop(columns=["x", "y"]), geometry=geometry, crs=TARGET_CRS)

    return gdf.reset_index(drop=True)
