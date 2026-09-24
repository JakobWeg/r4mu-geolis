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
    "retail": "area",
    "public_poi": "Weight",
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

# Per use case: which data_DE file/layer to load, which columns to keep
# (weight/attribute columns the corresponding use_case.py function needs,
# geometry is always kept), and how to rename them onto the schema the
# existing use_case.py functions expect.
LAYER_CONFIG = {
    "home_apartment": {
        "file": "infas_home_apartment_de.gpkg",
        "keep_columns": ["Haushalte"],
        "rename": {"Haushalte": "households_total"},
    },
    "home_detached": {
        "file": "infas_home_detached_de.gpkg",
        "keep_columns": ["Haushalte"],
        "rename": {"Haushalte": "households_total"},
    },
    "work": {
        # Merged OSM+INFAS candidates (replaced the old INFAS-only file) -
        # has a real "area" column directly, so work() can use its normal
        # default weight_column="area" instead of a company-count stand-in.
        "file": "osm_work_candidates_de.gpkg",
        "keep_columns": ["category", "office", "area", "source"],
        "rename": {},
    },
    "retail": {
        "file": "osm_retail_parking_de.gpkg",
        "keep_columns": ["parking_id", "area", "n_shops", "categories"],
        "rename": {"categories": "category"},
    },
    "depot": {
        "file": "osm_depot_candidates_de.gpkg",
        "keep_columns": ["category", "area"],
        "rename": {},
    },
    "public_poi": {
        "file": "osm_street_parking_de.gpkg",
        "keep_columns": ["parking_id", "area", "n_pois", "total_weight", "categories"],
        "rename": {"total_weight": "Weight"},
    },
    "hpc_urban": {
        "file": "osm_hpc_urban_candidates_de.gpkg",
        "keep_columns": ["category", "area", "weight"],
        "rename": {},
    },
    "hpc_rural": {
        "file": "osm_bast_hpc_rural_candidates_de.gpkg",
        "keep_columns": ["highway", "Str_Klasse_kurz", "DTV_Kfz", "weight"],
        "rename": {},
    },
}

TARGET_CRS = 3035


def _to_representative_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    is_polygon = gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"])
    if is_polygon.any():
        gdf = gdf.copy()
        gdf.loc[is_polygon, "geometry"] = gdf.loc[is_polygon, "geometry"].centroid
    return gdf


def _finalize_retail_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Synthesize the exact column set use_case.py:retail() expects, so its
    internal rename-fallback branch (built for the ALKIS/Berlin schema) is
    skipped entirely for the data_DE schema.
    """
    gdf = gdf.copy()
    gdf["id_0"] = range(len(gdf))
    gdf["osm_way_id"] = gdf["id_0"]
    gdf["amenity"] = gdf.get("source", "")
    gdf["other_tags"] = ""
    gdf["id"] = gdf["id_0"]
    return gdf


def load_layer(data_dir, layer_key: str) -> gpd.GeoDataFrame:
    spec = LAYER_CONFIG[layer_key]
    path = pathlib.Path(data_dir) / spec["file"]
    gdf = gpd.read_file(path, layer=spec.get("layer"), columns=spec["keep_columns"])
    if spec["rename"]:
        gdf = gdf.rename(columns=spec["rename"])
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

        joined = assign_ags(gdf, municipalities)

        df = pd.DataFrame(joined.drop(columns=["geometry"]))
        df["x"] = joined.geometry.x.values
        df["y"] = joined.geometry.y.values

        layer_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(layer_dir, engine="pyarrow", partition_cols=["AGS"], index=False, max_partitions=12000)
        utility.safe_print(f"--- '{layer_key}': {len(df)}/{len(gdf)} candidates matched, written to {layer_dir} ---")


def load_candidates(prepared_dir, layer_key: str, ags: str) -> gpd.GeoDataFrame:
    """Read one Gemeinde's candidates for one layer. Empty GeoDataFrame with
    the right columns if this Gemeinde has none for this layer.
    """
    spec = LAYER_CONFIG[layer_key]
    attr_columns = [spec["rename"].get(c, c) for c in spec["keep_columns"]]
    partition_dir = pathlib.Path(prepared_dir) / layer_key / f"AGS={ags}"

    if not partition_dir.is_dir():
        return gpd.GeoDataFrame(columns=attr_columns + ["geometry"], geometry="geometry", crs=TARGET_CRS)

    df = pd.read_parquet(partition_dir, engine="pyarrow")
    geometry = gpd.points_from_xy(df["x"], df["y"])
    gdf = gpd.GeoDataFrame(df.drop(columns=["x", "y"]), geometry=geometry, crs=TARGET_CRS)

    if layer_key == "retail":
        gdf = _finalize_retail_columns(gdf)

    return gdf.reset_index(drop=True)
