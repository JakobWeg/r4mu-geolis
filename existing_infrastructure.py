"""Public charging infrastructure that already exists (BNetzA
Ladesaeulenregister), used to pre-seed capacity in the public use cases
(retail, street/public, hpc_urban, hpc_highway) so the model fills existing
infrastructure before proposing new locations. Not applicable to home/work,
which are private (not publicly registered) infrastructure.

distribute_charging_events already prefers reusing a location that has free
capacity over opening a brand-new one - so "use existing infrastructure
first" needs no new placement logic, just seeding a candidate's starting
charging_points/average_charging_capacity from the register instead of 0.
"""
import pathlib

import geopandas as gpd
import numpy as np
import pandas as pd
import utility

TARGET_CRS = 3035
MATCH_RADIUS_M = 50
RETAIL_PREFERENCE_RATIO = 2.0

REGISTER_HEADER_ROW = 10


def convert_register_to_gpkg(xlsx_path, gpkg_path) -> pathlib.Path:
    """One-time conversion of the BNetzA Excel export to a GeoPackage - much
    faster to read on every subsequent run than re-parsing the Excel file.
    No-op if gpkg_path already exists.
    """
    gpkg_path = pathlib.Path(gpkg_path)
    if gpkg_path.is_file():
        return gpkg_path

    utility.safe_print(f"--- converting {xlsx_path} to {gpkg_path} ---")
    df = pd.read_excel(xlsx_path, header=REGISTER_HEADER_ROW)
    df = df.loc[df["Status"] == "In Betrieb"].copy()

    lat = df["Breitengrad"].astype(str).str.replace(",", ".", regex=False).astype(float)
    lon = df["Längengrad"].astype(str).str.replace(",", ".", regex=False).astype(float)

    keep = df[["Ladeeinrichtungs-ID", "Anzahl Ladepunkte", "Nennleistung Ladeeinrichtung [kW]",
               "Art der Ladeeinrichtung"]].rename(columns={
        "Ladeeinrichtungs-ID": "facility_id",
        "Anzahl Ladepunkte": "existing_points",
        "Nennleistung Ladeeinrichtung [kW]": "existing_capacity_kw_total",
        "Art der Ladeeinrichtung": "kind",
    })
    gdf = gpd.GeoDataFrame(keep, geometry=gpd.points_from_xy(lon, lat), crs=4326)
    gdf = gdf.dropna(subset=["geometry"]).to_crs(TARGET_CRS)
    # "Nennleistung Ladeeinrichtung" is the facility's total rated power
    # (confirmed via sample rows where it equals the sum of per-connector
    # ratings), not per charging point - divide to get the average per-point
    # capacity that average_charging_capacity represents throughout the model.
    gdf["existing_capacity_kw"] = gdf["existing_capacity_kw_total"] / gdf["existing_points"].replace(0, 1)

    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(gpkg_path, driver="GPKG")
    utility.safe_print(f"--- converted {len(gdf)} existing charging facilities to {gpkg_path} ---")
    return gpkg_path


def convert_zenodo_register_to_gpkg(zenodo_dir, gpkg_path) -> pathlib.Path:
    """One-time conversion of the "FAIR Charging Station data package"
    (Zenodo DOI 10.5281/zenodo.14587399, BNetzA Ladesaeulenregister data as
    of 2024-12-01) to the same GeoPackage schema convert_register_to_gpkg()
    produces from the official raw BNetzA Excel export - so run_de.py's
    downstream categorize_and_match()/merge_existing_into_candidates() don't
    need to know which source a given scenario year's existing-infrastructure
    snapshot came from. Used for the 2024 scenario, since BNetzA itself only
    publishes its CURRENT snapshot (no archive of past export dates) - this
    dataset is the closest available approximation of what the register
    actually looked like in the 2024 scenario year, rather than using data
    from ~2 years later.

    zenodo_dir: directory containing the package's extracted CSVs
    (bnetza_charging_columns_01_12_2024.csv, bnetza_charging_points_01_12_2024.csv,
    bnetza_locations_01_12_2024.csv - the other files in the package,
    operators/sockets/compatibility, aren't needed here).

    No-op if gpkg_path already exists.
    """
    gpkg_path = pathlib.Path(gpkg_path)
    if gpkg_path.is_file():
        return gpkg_path

    zenodo_dir = pathlib.Path(zenodo_dir)
    utility.safe_print(f"--- converting {zenodo_dir} to {gpkg_path} ---")
    columns = pd.read_csv(zenodo_dir / "bnetza_charging_columns_01_12_2024.csv")
    points = pd.read_csv(zenodo_dir / "bnetza_charging_points_01_12_2024.csv")
    locations = pd.read_csv(zenodo_dir / "bnetza_locations_01_12_2024.csv")

    # One row per "Ladeeinrichtung" (charging column/station) - matches the
    # raw BNetzA export's own granularity. "existing_points" = how many
    # individual charging points (plugs) it has, from the normalised
    # dataset's separate points table (one row per point, linked by
    # column_id) - the raw export has this as a single "Anzahl Ladepunkte"
    # column directly, this just re-derives the same count.
    existing_points = points.groupby("column_id").size().rename("existing_points")
    columns = columns.merge(existing_points, left_on="id", right_index=True, how="left")
    columns["existing_points"] = columns["existing_points"].fillna(0).astype(int)

    columns = columns.merge(
        locations[["id", "latitude", "longitude"]].rename(columns={"id": "location_id"}),
        on="location_id", how="left",
    )

    keep = columns[["id", "existing_points", "net_capacity", "column_type", "latitude", "longitude"]].rename(
        columns={"id": "facility_id", "net_capacity": "existing_capacity_kw_total"}
    )
    # column_type has exactly two values in this dataset ("regular"/"fast") -
    # mapped onto the same "kind" vocabulary categorize_and_match() already
    # branches on for the raw-Excel source.
    keep["kind"] = keep["column_type"].map({"regular": "Normalladeeinrichtung", "fast": "Schnellladeeinrichtung"})
    keep = keep.drop(columns=["column_type"])

    gdf = gpd.GeoDataFrame(keep, geometry=gpd.points_from_xy(keep["longitude"], keep["latitude"]), crs=4326)
    gdf = gdf.drop(columns=["latitude", "longitude"]).dropna(subset=["geometry"]).to_crs(TARGET_CRS)
    # net_capacity is the station's total rated power (see convert_register_
    # to_gpkg()'s same note) - divide to get the average per-point capacity.
    gdf["existing_capacity_kw"] = gdf["existing_capacity_kw_total"] / gdf["existing_points"].replace(0, 1)

    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(gpkg_path, driver="GPKG")
    utility.safe_print(f"--- converted {len(gdf)} existing charging facilities to {gpkg_path} ---")
    return gpkg_path


def load_existing_infrastructure(gpkg_path) -> gpd.GeoDataFrame:
    return gpd.read_file(gpkg_path)


def _nearest_match(points: gpd.GeoDataFrame, candidates: gpd.GeoDataFrame):
    """For each row in `points`, the positional index into `candidates` of
    its nearest neighbour and the distance to it (-1 / inf if candidates is
    empty or points is empty).
    """
    n = len(points)
    if candidates.empty or n == 0:
        return np.full(n, -1), np.full(n, np.inf)

    cand = candidates[["geometry"]].reset_index(drop=True)
    cand["cand_idx"] = np.arange(len(cand))
    joined = gpd.sjoin_nearest(points[["geometry"]].reset_index(drop=True), cand, how="left", distance_col="dist")
    joined = joined[~joined.index.duplicated(keep="first")].sort_index()
    return joined["cand_idx"].fillna(-1).astype(int).to_numpy(), joined["dist"].fillna(np.inf).to_numpy()


def categorize_and_match(existing_gdf: gpd.GeoDataFrame, retail_candidates: gpd.GeoDataFrame,
                          street_candidates: gpd.GeoDataFrame, hpc_urban_candidates: gpd.GeoDataFrame,
                          hpc_rural_candidates: gpd.GeoDataFrame, match_radius: float = MATCH_RADIUS_M,
                          retail_preference_ratio: float = RETAIL_PREFERENCE_RATIO) -> dict:
    """
    Assigns every existing charging facility to one of the four public use
    cases, and within that use case to its nearest candidate location (or
    flags it unmatched with cand_idx=-1 if nothing is within match_radius).

    "Normalladeeinrichtung" (slow/AC) -> retail or street/public: goes to
    retail only if the nearest retail candidate is meaningfully closer
    (retail_preference_ratio) than the nearest street/public POI, so an
    ordinary curbside charger near a shop isn't misclassified as retail.
    "Schnellladeeinrichtung" (fast/DC) -> hpc_urban or hpc_highway, whichever
    candidate type is nearer.

    Returns {layer_key: GeoDataFrame[existing_points, existing_capacity_kw,
    cand_idx, geometry]}.
    """
    slow = existing_gdf[existing_gdf["kind"] == "Normalladeeinrichtung"].reset_index(drop=True)
    fast = existing_gdf[existing_gdf["kind"] == "Schnellladeeinrichtung"].reset_index(drop=True)
    # facility_id is carried through so merge_existing_into_candidates() can
    # give each UNMATCHED facility (appended as its own new candidate row) a
    # stable candidate_uid - without it they'd all share the same missing
    # value and collapse onto one shared location_id downstream.
    cols = ["facility_id", "existing_points", "existing_capacity_kw", "geometry"]

    retail_idx, retail_dist = _nearest_match(slow, retail_candidates)
    street_idx, street_dist = _nearest_match(slow, street_candidates)

    is_retail = (retail_dist <= match_radius) & (retail_dist * retail_preference_ratio <= street_dist)
    is_street = (~is_retail) & (street_dist <= match_radius)
    is_unmatched_slow = ~is_retail & ~is_street

    retail_df = slow.loc[is_retail, cols].assign(cand_idx=retail_idx[is_retail])
    street_df = pd.concat([
        slow.loc[is_street, cols].assign(cand_idx=street_idx[is_street]),
        slow.loc[is_unmatched_slow, cols].assign(cand_idx=-1),  # unmatched slow -> new street candidate
    ], ignore_index=True)

    urban_idx, urban_dist = _nearest_match(fast, hpc_urban_candidates)
    rural_idx, rural_dist = _nearest_match(fast, hpc_rural_candidates)
    is_urban = (urban_dist <= match_radius) & (urban_dist <= rural_dist)
    is_rural = (~is_urban) & (rural_dist <= match_radius)
    is_unmatched_fast = ~is_urban & ~is_rural

    urban_df = pd.concat([
        fast.loc[is_urban, cols].assign(cand_idx=urban_idx[is_urban]),
        fast.loc[is_unmatched_fast, cols].assign(cand_idx=-1),  # unmatched fast -> new urban hpc candidate
    ], ignore_index=True)
    rural_df = fast.loc[is_rural, cols].assign(cand_idx=rural_idx[is_rural]).reset_index(drop=True)

    crs = existing_gdf.crs
    return {
        "retail": gpd.GeoDataFrame(retail_df, geometry="geometry", crs=crs),
        "public_poi": gpd.GeoDataFrame(street_df, geometry="geometry", crs=crs),
        "hpc_urban": gpd.GeoDataFrame(urban_df, geometry="geometry", crs=crs),
        "hpc_rural": gpd.GeoDataFrame(rural_df, geometry="geometry", crs=crs),
    }


def merge_existing_into_candidates(candidates: gpd.GeoDataFrame, matches: gpd.GeoDataFrame,
                                    weight_column: str) -> gpd.GeoDataFrame:
    """Pre-seed matched candidates' existing_points/existing_capacity_kw (so
    distribute_charging_events fills them before opening new locations), and
    append unmatched existing facilities as their own new candidate rows so
    no real infrastructure is silently dropped from the model.
    """
    crs = candidates.crs
    candidates = candidates.reset_index(drop=True).copy()
    candidates["existing_points"] = 0
    candidates["existing_capacity_kw"] = 0.0

    matched = matches[matches["cand_idx"] >= 0]
    if not matched.empty:
        grouped = matched.groupby("cand_idx")
        existing_points = grouped["existing_points"].sum()
        existing_capacity_kw = grouped.apply(lambda g: np.average(g["existing_capacity_kw"], weights=g["existing_points"]))
        candidates.loc[existing_points.index, "existing_points"] = existing_points.astype(int)
        candidates.loc[existing_capacity_kw.index, "existing_capacity_kw"] = existing_capacity_kw

    unmatched = matches[matches["cand_idx"] == -1]
    if not unmatched.empty:
        new_rows = gpd.GeoDataFrame(
            {"existing_points": unmatched["existing_points"].to_numpy(),
             "existing_capacity_kw": unmatched["existing_capacity_kw"].to_numpy()},
            geometry=unmatched.geometry.to_numpy(), crs=crs,
        )
        if weight_column and weight_column in candidates.columns:
            # nominal weight so an unmatched existing site can still receive
            # additional new points later if demand exceeds what it already has
            new_rows[weight_column] = new_rows["existing_points"].clip(lower=1)
        if "candidate_uid" in candidates.columns:
            # Each unmatched facility is its own new candidate row - without
            # a real, stable identity here they'd all fall through to the
            # generic NaN-fill below and share one missing candidate_uid,
            # which location_registry.LocationRegistry.assign_ids() would
            # then collapse onto a single shared location_id (confirmed:
            # multiple distinct unmatched hpc_urban/public sites in one
            # Gemeinde all landing on the exact same output location_id).
            # facility_id (BNetzA's own "Ladeeinrichtungs-ID") is stable
            # across repeated preparations of the same scenario year, which
            # is all candidate_uid needs to guarantee here - use_existing_
            # infrastructure only ever applies to the scenario chain's first
            # year, so this never needs to survive across scenario years.
            new_rows["candidate_uid"] = "bnetza_" + unmatched["facility_id"].astype(str).to_numpy()
        for col in candidates.columns:
            if col not in new_rows.columns:
                new_rows[col] = np.nan
        candidates = pd.concat([candidates, new_rows[candidates.columns]], ignore_index=True)

    return gpd.GeoDataFrame(candidates, geometry="geometry", crs=crs)
