import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import numpy as np
import math

def postprocess_public_demands(charging_locations: gpd.GeoDataFrame, located_charging_events: gpd.GeoDataFrame):


    max_distance = 1000 # Meter

    # Filter street locations und home_street Events
    street_locations = charging_locations[charging_locations["mode"] == "street"].copy().reset_index(drop=True)
    home_events = located_charging_events[located_charging_events["mode"] == "home_street"]

    # Maximaler Zeitschritt (für Maskenlänge)
    max_step = max(located_charging_events["event_start"].max(), (located_charging_events["event_start"]+located_charging_events["event_time"]).max())

    # Belegungs-Maske: location_id -> np.array (Zeitschritte). int16 statt
    # Default-int: bei vollem Jahr (max_step ~35000) und vielen Standorten ist
    # das sonst der größte Speicherposten; Belegungszahlen je Zeitschritt
    # bleiben immer klein.
    location_ids_all = charging_locations["location_id"].to_numpy()
    occupancy_mask = {loc_id: np.zeros(max_step + 1, dtype=np.int16) for loc_id in location_ids_all}

    # Belegungs-Maske vektorisiert über Differenz-Array + Kumulativsumme statt
    # iterrows() über jedes Event füllen: +1 bei event_start, -1 bei
    # event_start+event_time, anschließend cumsum ergibt die Belegung je
    # Zeitschritt - exakt äquivalent zum vorherigen Event-für-Event-Aufaddieren.
    events_loc_ids = located_charging_events["location_id"].to_numpy()
    events_start = located_charging_events["event_start"].to_numpy()
    events_end = events_start + located_charging_events["event_time"].to_numpy()
    valid_loc_mask = np.isin(events_loc_ids, location_ids_all)

    diff_by_loc = {}
    for loc_id, start, end in zip(events_loc_ids[valid_loc_mask], events_start[valid_loc_mask], events_end[valid_loc_mask]):
        diff = diff_by_loc.get(loc_id)
        if diff is None:
            diff = np.zeros(max_step + 2, dtype=np.int32)
            diff_by_loc[loc_id] = diff
        diff[start] += 1
        diff[end] -= 1
    for loc_id, diff in diff_by_loc.items():
        occupancy_mask[loc_id] = np.cumsum(diff[:-1]).astype(np.int16)

    # Räumlicher Index für street locations
    street_locations_sindex = street_locations.sindex

    # Kandidatensuche/-filterung komplett in numpy statt pandas/shapely pro
    # Event: .iloc[]-Kopien und .distance()-Aufrufe auf GeoDataFrames haben pro
    # Aufruf spürbaren Overhead, der sich bei hunderttausenden home_street-
    # Events in Großstädten (z.B. Stuttgart: ~60k) zu Stunden aufsummiert.
    # street_locations wurde oben auf einen 0..n-1 RangeIndex zurückgesetzt,
    # sodass diese Arrays exakt per Position mit street_locations übereinstimmen.
    street_ids_arr = street_locations["location_id"].to_numpy()
    street_points_arr = street_locations["charging_points"].to_numpy()
    street_xy = np.column_stack([
        street_locations.geometry.x.to_numpy(),
        street_locations.geometry.y.to_numpy(),
    ])
    street_geoms = street_locations.geometry.to_numpy()
    max_distance_sq = max_distance ** 2

    umverteilte_events = 0
    zugeschlagene_punkte = 0

    # Nur home_street-Events durchgehen (statt aller Events mit anschließendem
    # Skip).
    for row in home_events.itertuples():
        event_point = row.geometry
        start = row.event_start
        end = row.event_start + row.event_time
        old_location_id = row.location_id
        ex, ey = event_point.x, event_point.y

        # Kandidaten in max_distance suchen (Bounding-Box-Query per sindex,
        # exakte Distanzfilterung danach vektorisiert in numpy statt per
        # GeoDataFrame.distance()).
        candidate_idx = street_locations_sindex.intersection(
            (ex - max_distance, ey - max_distance, ex + max_distance, ey + max_distance)
        )
        candidate_idx = np.fromiter(candidate_idx, dtype=np.int64)
        if candidate_idx.size == 0:
            continue

        dx = street_xy[candidate_idx, 0] - ex
        dy = street_xy[candidate_idx, 1] - ey
        dist_sq = dx * dx + dy * dy
        within = dist_sq <= max_distance_sq
        if not within.any():
            continue  # Kein Standort in Reichweite

        candidate_idx = candidate_idx[within]
        # Nächstgelegene Kandidaten zuerst prüfen.
        order = np.argsort(dist_sq[within])
        candidate_idx = candidate_idx[order]

        # Prüfe freie Kapazität in Maske
        winner_idx = None
        for cand_idx in candidate_idx:
            loc_id = street_ids_arr[cand_idx]
            if occupancy_mask[loc_id][start:end].max() < street_points_arr[cand_idx]:
                winner_idx = cand_idx
                break

        if winner_idx is not None:
            new_location_id = street_ids_arr[winner_idx]

            # Ladevent umverteilen
            idx = row.Index
            located_charging_events.at[idx, "location_id"] = new_location_id
            located_charging_events.at[idx, "mode"] = "street"
            located_charging_events.at[idx, "geometry"] = street_geoms[winner_idx]

            # Maske updaten
            # Abziehen der belegten Zeitschritte von der ursprünglichen home_street Location
            occupancy_mask[old_location_id][start:end] -= 1

            # Hinzufügen der belegten Zeitschritte zu der neuen location_id
            occupancy_mask[new_location_id][start:end] += 1

            umverteilte_events += 1


    # Berechnung der maximalen gleichzeitigen Belegung je Location - vektorisiert
    # über eine Lookup-Series statt einem .loc[boolean mask]-Scan (O(n)) pro
    # Location im ursprünglichen Code (das war O(n_locations^2) insgesamt).
    max_concurrent_demand = pd.Series(
        {loc_id: int(mask.max()) for loc_id, mask in occupancy_mask.items()}
    )
    charging_locations = charging_locations.set_index("location_id", drop=False)
    demand_aligned = max_concurrent_demand.reindex(charging_locations.index)
    increase_mask = demand_aligned > charging_locations["charging_points"]
    decrease_mask = demand_aligned < charging_locations["charging_points"]

    zugeschlagene_punkte = int((demand_aligned[increase_mask] - charging_locations.loc[increase_mask, "charging_points"]).sum())
    charging_locations.loc[increase_mask, "charging_points"] = demand_aligned[increase_mask].astype(int)
    charging_locations.loc[decrease_mask, "charging_points"] = demand_aligned[decrease_mask].astype(int)
    charging_locations = charging_locations.reset_index(drop=True)


    return charging_locations, located_charging_events

def park_time_limitation(charging_events, data_dict, charging_use_case):

    df = charging_events # .loc[charging_events["charging_use_case"] == charging_use_case].copy()

    # if start >= start_grenze and start < end_grenze:
    #     if (row['energy'] / row['station_charging_capacity'] * 4) > limit_schritte:
    #         return row['energy'] / row['station_charging_capacity'] * 4
    #     return min(row['event_time'], limit_schritte)

    limit_schritte = data_dict["charging_time_limit_duration"] # 4h
    tag_laenge = 96 # 24h
    start_grenze = data_dict["charging_time_limit_start"] # 9:00
    end_grenze = data_dict["charging_time_limit_end"] # 21:00

    def begrenze_event(start, dauer, lade):
        ende = start + dauer

        neuer_start = start
        neue_dauer = 0

        while neuer_start < ende:
            tag_start = (neuer_start // tag_laenge) * tag_laenge
            fenster_start = tag_start + start_grenze
            fenster_ende = tag_start + end_grenze

            teil_ende = min(ende, tag_start + tag_laenge)
            teil_dauer = teil_ende - neuer_start

            if neuer_start >= fenster_ende or teil_ende <= fenster_start:
                neue_dauer += teil_dauer
                neuer_start += teil_dauer
                continue

            overlap_start = max(neuer_start, fenster_start)
            overlap_end = min(teil_ende, fenster_ende)
            overlap = max(0, overlap_end - overlap_start)

            # Ausnahme: Beginn im letzten 4h-Fenster
            if neuer_start >= fenster_ende - limit_schritte:
                neue_dauer += teil_dauer
                neuer_start += teil_dauer
                continue

            max_ladezeit = lade if lade > limit_schritte else limit_schritte
            begrenzt_overlap = min(overlap, max_ladezeit)

            vor_fenster = max(0, fenster_start - neuer_start)
            neue_dauer += vor_fenster + begrenzt_overlap

            neuer_start = fenster_ende

        return int(min(neue_dauer, dauer))

    # Neue Spalte: Originale Dauer speichern
    df['original_event_time'] = df['event_time']

    # Vorher: df['event_time'].loc[mask] = df.loc[mask].apply(begrenze_event, axis=1)
    # - das ist chained assignment und aktualisiert unter Copy-on-Write das
    #   Original NICHT (siehe pandas ChainedAssignmentError) - die Limitierung
    #   griff also faktisch nie. Außerdem baut .apply(axis=1) pro Zeile eine
    #   eigene Series auf, was bei hunderttausenden Events (z.B. "street" in
    #   Großstädten) sehr langsam ist. Fix: mit .loc[mask, col] = ... in einem
    #   Schritt zuweisen, und begrenze_event über nackte numpy-Arrays statt
    #   Row-Objekten aufrufen.
    mask = charging_events["charging_use_case"] == charging_use_case
    if mask.any():
        starts = df.loc[mask, "event_start"].to_numpy()
        dauern = df.loc[mask, "event_time"].to_numpy()
        lade_arr = df.loc[mask, "energy"].to_numpy() / df.loc[mask, "station_charging_capacity"].to_numpy() * 4
        limited = np.fromiter(
            (begrenze_event(s, d, l) for s, d, l in zip(starts, dauern, lade_arr)),
            dtype=np.int64, count=len(starts),
        )
        df.loc[mask, "event_time"] = limited

    df['wurde_begrenzt'] = df['event_time'] < df['original_event_time']

    return df.drop(columns=['original_event_time'])

def get_id(use_case_id, location_id):

    # todo: eliminate float in ids

    use_case_map = {
        "home_detached": "1",
        "home_apartment": "2",
        "work": "3",
        "hpc": "4",
        "retail": "5",
        "public": "6",
        "depot": "7",
        "hpc_urban": "4",
        "hpc_highway": "8",
    }

    location_id = location_id.astype(int)
    uc_id = use_case_map.get(use_case_id)

    ids = location_id.astype(str).apply(lambda x: int(uc_id + x))

    return ids.values.astype(int)

def distribute_charging_events(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    fill_existing_first: bool = True,  # Old behavior
    rng: np.random.Generator = None,
    #home_street: bool = False,
    fill_existing_only: bool = False,  # New behavior
    availability_mask: np.array = None,
    flexibility_multi_use: int = 0,
    return_mask: bool = False,
    seed: int = 1,
    additional_street_input: bool = False,
    location_id_start: int = 0,
    existing_points_column: str = None,
    existing_capacity_column: str = None,
):
    """
    Distributes charging events to locations with optional random assignment.
    Tracks number of charging points and average charging capacity per location.
    If 'fill_existing_only' is True, only existing charging points are filled.

    existing_points_column/existing_capacity_column: optional column names in
    `locations` giving already-existing real-world charging_points/capacity
    (e.g. from the BNetzA Ladesaeulenregister) to seed a location with instead
    of starting at 0. Since the assignment loop below already prefers reusing
    a location with free capacity over opening a new one, this alone makes
    existing infrastructure get filled before any new location is proposed -
    no separate "existing first" pass needed.

    The reuse search below checks every currently-opened location (no cap):
    since run_de.py simulates one Gemeinde at a time, "opened" here means
    opened within one Gemeinde's one use case, not nationwide - a few
    thousand at most even for the largest city, not the ~270k a use case can
    reach nationwide. That search is one vectorized numpy call over however
    many are open, not a Python-level loop per candidate, so it stays fast
    at that scale (unlike distribute_charging_events_per_vehicle, which
    checks multiple time windows per vehicle and genuinely needs its cap).
    """
    # reset seed so that the locations are always the same
    #rng = np.random.default_rng(seed)

    if fill_existing_only:
        return distribute_charging_events_fill_existing_only(
            locations, events, weight_column, simulation_steps, flexibility_multi_use, rng, availability_mask,
            additional_street_input= additional_street_input, location_id_start=location_id_start
        )

    # if home_street:
    #     n_locations_home_street = len(locations[locations["mode"]== "home_street"])
    #     n_locations_not_home_street = len(locations[locations["mode"]== "not_home_street"])
    #
    # else:
    n_locations = len(locations)
    n_events = len(events)

    if n_locations == 0:
        # No candidate locations at all (e.g. a use case's area/weight filter
        # eliminated every candidate for this Gemeinde) - rng.choice(0, ...)
        # would raise ValueError below; nothing can be assigned instead.
        locations = locations.reset_index().copy()
        locations["charging_points"] = pd.array([], dtype="int64")
        locations["average_charging_capacity"] = pd.array([], dtype="int64")
        locations.index = locations.index + location_id_start
        events = events.copy()
        events["assigned_location"] = np.full(n_events, np.nan)
        if return_mask:
            return locations, events, np.zeros((0, simulation_steps), dtype=np.uint8)
        return locations, events

    # Normalize weights
    # NaN in the weight column (e.g. a merged candidate source with some
    # rows missing a computed area) would otherwise poison the whole
    # probabilities array and crash rng.choice - treat it as zero weight
    # (never picked) instead. If EVERY candidate ends up zero-weight (e.g. a
    # small Gemeinde where this weight column is entirely missing/zero), fall
    # back to uniform probabilities instead of dividing by a zero sum, which
    # would make every entry NaN and crash rng.choice just the same.
    # astype(float64): pd.concat with an empty, object-dtype sibling frame
    # (e.g. a use case's other candidate layer being empty for this Gemeinde)
    # silently upcasts an otherwise-numeric weight column to object dtype -
    # rng.choice then fails to cast the resulting probabilities to float64.
    weights = locations[weight_column].fillna(0).to_numpy().astype(np.float64)
    weights_sum = weights.sum()
    if weights_sum > 0:
        probabilities = weights / weights_sum
    else:
        probabilities = np.full(n_locations, 1.0 / n_locations)

    # Initial setup

    locations = locations.reset_index().copy()

    assigned_locations = np.full(n_events, np.nan)

    # Create availability matrix: rows=locations, cols=timesteps
    # uint8 instead of the platform default (int64): occupancy counts per
    # location/timestep are always small (94 charging_points was the largest
    # ever observed across a full nationwide run - comfortably under uint8's
    # 255 ceiling), and at full-year simulation_steps (~35000, vs. the
    # original single-region pipeline's hardcoded 2000 - a ~17.5x bigger time
    # axis) this matrix is the dominant memory cost for large candidate pools
    # in big cities - confirmed responsible for an out-of-memory crash
    # (single 12.2GB allocation failing despite 256GB system RAM, most
    # likely address-space fragmentation from a long-running worker process
    # cycling through many differently-sized arrays across thousands of
    # Gemeinden) on Kiel/Luebeck's home_apartment/home_detached candidates.
    # uint8 halves this vs. the previous int16 (an 8x reduction vs. the
    # platform default int64).
    availability = np.zeros((n_locations, simulation_steps), dtype=np.uint8)

    # Plain numpy arrays instead of DataFrame .at[]/.values lookups inside the
    # hot loop, and the "reuse if free" search below is restricted to
    # currently-opened locations (charging_points > 0) instead of scanning
    # all n_locations every event - locations with 0 points can never satisfy
    # in_use < required anyway (0 < 0 is False), so this is exact, not an
    # approximation, and is what makes this loop tractable for the
    # full-year-of-events volumes the DE-wide pipeline produces for large
    # cities (previously O(n_events * n_locations), now bounded by the number
    # of locations actually in use so far).
    if existing_points_column and existing_points_column in locations.columns:
        charging_points = locations[existing_points_column].fillna(0).to_numpy().astype(np.int64)
    else:
        charging_points = np.zeros(n_locations, dtype=np.int64)
    if existing_capacity_column and existing_capacity_column in locations.columns:
        average_charging_capacity = locations[existing_capacity_column].fillna(0).to_numpy().astype(np.float64)
    else:
        average_charging_capacity = np.zeros(n_locations, dtype=np.float64)

    # Real-world existing infrastructure (e.g. BNetzA Ladesaeulenregister)
    # gets a dedicated first-priority pass below, weighted by how many
    # charging points each site already has - not by weight_column (which is
    # meant for placing NEW candidates and can be very unevenly distributed,
    # e.g. by area/POI density). Captured once, before the loop, so this
    # weighting reflects the real installed capacity even if a location
    # later gains additional simulated points via the normal fallback path.
    existing_idx = np.nonzero(charging_points)[0]
    existing_weight = charging_points[existing_idx].astype(np.float64)
    # Compact, contiguous mirror of just existing_idx's own rows of
    # `availability` - see the identical fix (with the full rationale) in
    # distribute_charging_events_per_vehicle. n_locations here (every
    # candidate for this use case, e.g. every retail parking lot or public
    # street-parking spot in the Gemeinde) can still be far bigger than
    # existing_idx (only the ones with real pre-existing infrastructure), so
    # the same scattered-gather-out-of-a-huge-array cost applies to Phase 1
    # below on every one of this use case's events.
    existing_availability = availability[existing_idx, :].copy()
    existing_pos = {int(loc): i for i, loc in enumerate(existing_idx)}

    event_start_arr = events["event_start"].to_numpy()
    event_time_arr = events["event_time"].to_numpy()
    capacity_arr = events["station_charging_capacity"].to_numpy()


    progress_step = (n_events // 10000 + 1) if n_events > 10000 else None

    # Incrementally tracked instead of np.nonzero(charging_points) freshly
    # every event - that recomputation, plus fancy-indexing availability with
    # the full (potentially thousands-large) opened set, is what made this
    # loop scale with the number of opened locations instead of just the
    # event count. Seeded with whatever's already opened (e.g. pre-seeded
    # existing infrastructure) before the loop starts.
    opened_list = list(np.nonzero(charging_points)[0])

    for idx in range(n_events):
        start = event_start_arr[idx]
        duration = event_time_arr[idx]
        end = start + duration
        capacity = capacity_arr[idx]

        assigned = None

        # Phase 1: real existing infrastructure first, weighted by installed
        # charging-point count. Only fall through to the normal methodology
        # (reuse any opened location, else open a new one, both weighted by
        # weight_column) once every existing site is full for this event's
        # time window.
        if existing_idx.size:
            in_use_existing = existing_availability[:, start:end].max(axis=1)
            free_existing_mask = in_use_existing < charging_points[existing_idx]
            if free_existing_mask.any():
                free_existing = existing_idx[free_existing_mask]
                free_existing_weights = existing_weight[free_existing_mask]
                weight_sum = free_existing_weights.sum()
                if weight_sum > 0:
                    assigned = rng.choice(free_existing, p=free_existing_weights / weight_sum)
                else:
                    assigned = rng.choice(free_existing)

        if assigned is None and fill_existing_first:
            if additional_street_input:
                availability_mask = np.zeros((n_locations, 2000))
            if availability.size < 1:
                pass
            if start >= end:
                pass

            if opened_list:
                search_idx = np.array(opened_list)
                in_use = availability[search_idx, start:end].max(axis=1)
                required = charging_points[search_idx]
                free_mask = in_use < required
                if free_mask.any():
                    # Weighted pick among the free candidates (by the same
                    # weight_column used to open new locations) instead of
                    # always the first free index in search_idx. The plain
                    # "first free wins" rule is invisible when locations open
                    # one-by-one via weighted random draws, but with
                    # pre-seeded existing infrastructure (existing_points_
                    # column) dozens of locations can start "open"
                    # simultaneously in arbitrary file order - "first free
                    # wins" then piles all demand onto whichever few rows
                    # happen to come first and never saturate, leaving the
                    # rest of the real infrastructure with zero events even
                    # though it's included in the output (observed for
                    # Flensburg's hpc_urban: 40 pre-seeded locations, only 3
                    # ever got an event).
                    free_idx = search_idx[free_mask]
                    free_weights = probabilities[free_idx]
                    weight_sum = free_weights.sum()
                    if weight_sum > 0:
                        assigned = rng.choice(free_idx, p=free_weights / weight_sum)
                    else:
                        assigned = rng.choice(free_idx)

        if assigned is None:
            assigned = rng.choice(n_locations, p=probabilities)
            was_unopened = charging_points[assigned] == 0
            # Increase number of charging points
            prev_count = charging_points[assigned]
            prev_avg = average_charging_capacity[assigned]
            new_avg = (prev_avg * prev_count + capacity) / (prev_count + 1)
            charging_points[assigned] += 1
            average_charging_capacity[assigned] = new_avg
            if was_unopened:
                opened_list.append(assigned)

        availability[assigned, start:end] += 1
        pos = existing_pos.get(int(assigned))
        if pos is not None:
            # Keep the compact mirror (see above) in sync whenever the
            # chosen location happens to be one of existing_idx's own.
            existing_availability[pos, start:end] += 1
        assigned_locations[idx] = assigned

        if progress_step and idx % progress_step == 0:
            percent = (idx + 1) / n_events * 100


    locations["charging_points"] = charging_points
    locations["average_charging_capacity"] = average_charging_capacity.astype(int)

    events = events.copy()
    events["assigned_location"] = assigned_locations + location_id_start

    locations.index = locations.index + location_id_start

    if return_mask:
        return locations, events, availability
    else:
        return locations, events

def distribute_charging_events_per_vehicle(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    vehicle_column: str,
    rng: np.random.Generator = None,
    return_mask: bool = False,
    location_id_start: int = 0,
    max_reuse_candidates: int = 20_000,
    existing_points_column: str = None,
    existing_capacity_column: str = None,
    label: str = None,
):
    """
    Same idea as distribute_charging_events (fill_existing_first=True), but
    assigns ONE location per distinct value of `vehicle_column` and applies
    it to every event in that group, instead of choosing a location
    independently per event.

    Why: a real vehicle always charges at the same home location and the same
    work location (barring a house move or job change, which this pipeline
    doesn't model) - assigning each event independently would scatter the
    same car's repeated home/work charging across many different candidate
    buildings, which is physically wrong. Use this for home/work; retail,
    public street charging, hpc and depot are genuinely different-location
    visits each time and should keep using distribute_charging_events as-is.

    max_reuse_candidates caps how many already-opened locations Phase 2 below
    checks for a fit before giving up and opening a new one. Originally had
    to be small (200): Phase 2 used to be a Python-level loop over the
    opened-locations list, and for home charging in a large city - where
    almost every vehicle needs its own dedicated point anyway (households
    don't share another household's wallbox) - that check is real work that
    essentially always fails, making the whole function scale as
    O(n_vehicles^2) and making it intractable for a big city (confirmed:
    Stuttgart with ~21k vehicles took 20+ minutes on just home_apartment
    before this cap was first added). Phase 2 is now vectorized (one numpy
    check across the whole search_pool per time window, not a Python loop -
    see below), so a much larger cap costs little per check; raised to
    20,000 to check more of the (potentially very large, for a big city)
    opened-locations list for a genuine reuse before giving up and opening a
    new one, instead of just the 200 most recently opened.

    Unlike Phase 1's existing_idx (fixed size for the whole run - see that
    section's comment on why vectorizing alone was enough there), opened_list
    GROWS as this loop runs - every vehicle that opens a genuinely new
    location appends to it, so it can approach the vehicle count itself by
    the end of a big city's home charging (most vehicles need their own
    dedicated point; households don't share a wallbox). A cap therefore still
    matters here even after vectorizing: removing it entirely would make the
    per-vehicle search size grow with how far into the loop that vehicle is,
    making total work O(n_vehicles^2) again regardless of how fast each
    individual check is - the same class of blowup Phase 1 had, just via
    list growth instead of an unbounded per-candidate Python loop.
    """
    n_locations = len(locations)
    n_events = len(events)

    if n_locations == 0:
        locations = locations.reset_index().copy()
        locations["charging_points"] = pd.array([], dtype="int64")
        locations["average_charging_capacity"] = pd.array([], dtype="int64")
        locations.index = locations.index + location_id_start
        events = events.copy()
        events["assigned_location"] = np.full(n_events, np.nan)
        if return_mask:
            return locations, events, np.zeros((0, simulation_steps), dtype=np.uint8)
        return locations, events

    # NaN in the weight column (e.g. a merged candidate source with some
    # rows missing a computed area) would otherwise poison the whole
    # probabilities array and crash rng.choice - treat it as zero weight
    # (never picked) instead. If EVERY candidate ends up zero-weight (e.g. a
    # small Gemeinde where this weight column is entirely missing/zero), fall
    # back to uniform probabilities instead of dividing by a zero sum, which
    # would make every entry NaN and crash rng.choice just the same.
    # astype(float64): pd.concat with an empty, object-dtype sibling frame
    # (e.g. a use case's other candidate layer being empty for this Gemeinde)
    # silently upcasts an otherwise-numeric weight column to object dtype -
    # rng.choice then fails to cast the resulting probabilities to float64.
    weights = locations[weight_column].fillna(0).to_numpy().astype(np.float64)
    weights_sum = weights.sum()
    if weights_sum > 0:
        probabilities = weights / weights_sum
    else:
        probabilities = np.full(n_locations, 1.0 / n_locations)
    locations = locations.reset_index().copy()

    # Seed from real existing infrastructure or a prior scenario year's own
    # output (see location_registry.py) instead of starting empty - same
    # idea as distribute_charging_events(), now also available for home/work.
    if existing_points_column and existing_points_column in locations.columns:
        charging_points = locations[existing_points_column].fillna(0).to_numpy().astype(np.int64)
    else:
        charging_points = np.zeros(n_locations, dtype=np.int64)
    if existing_capacity_column and existing_capacity_column in locations.columns:
        average_charging_capacity = locations[existing_capacity_column].fillna(0).to_numpy().astype(np.float64)
    else:
        average_charging_capacity = np.zeros(n_locations, dtype=np.float64)
    availability = np.zeros((n_locations, simulation_steps), dtype=np.int16)
    assigned_locations = np.full(n_events, np.nan)

    # Existing locations get first priority, weighted by their real installed
    # point count rather than the generic weight_column (see
    # distribute_charging_events() for the full rationale) - captured once,
    # before any vehicle grows a location's charging_points further.
    existing_idx = np.nonzero(charging_points)[0]
    existing_weight = charging_points[existing_idx].astype(np.float64)

    # A dedicated, small, CONTIGUOUS mirror of just existing_idx's own rows
    # of `availability` - Phase 1 below checks it on every one of
    # potentially hundreds of thousands of vehicles for a big city, and
    # `availability` itself is (n_locations, simulation_steps) where
    # n_locations is EVERY candidate (e.g. every building in a Gemeinde),
    # not just existing_idx's real-infrastructure subset - could be a
    # multi-GB array. Fancy-indexing scattered rows out of a huge array on
    # every vehicle is a random-access gather, which is memory-bandwidth-
    # bound and cache-unfriendly regardless of being vectorized (confirmed:
    # Berlin's 2037 home_apartment placement, ~226k vehicles, was still only
    # managing ~3-5 vehicles/second after Phase 1 was vectorized - projected
    # 12-20+ hours for this one stage alone). A small, contiguous copy turns
    # every check into a cheap slice instead of a scattered gather. Kept in
    # sync explicitly (see the "existing_availability" update right after
    # every `availability[assigned, ...]` write below) only for the
    # candidates that are actually in existing_idx.
    existing_availability = availability[existing_idx, :].copy()
    existing_pos = {int(loc): i for i, loc in enumerate(existing_idx)}

    event_start_arr = events["event_start"].to_numpy()
    event_time_arr = events["event_time"].to_numpy()
    capacity_arr = events["station_charging_capacity"].to_numpy()

    # {vehicle_id: array of row positions}, in order of first appearance
    vehicle_groups = events.groupby(vehicle_column, sort=False).indices

    n_groups = len(vehicle_groups)
    # Prints at most ~200 times regardless of n_groups (a big city's home
    # charging can have 500k+ distinct vehicles) - printing every vehicle
    # would itself become a meaningful slowdown, and swamp the console.
    progress_step = max(1, n_groups // 200)

    # Locations with charging_points > 0, in the order they were opened.
    # Tracked incrementally instead of recomputing via np.nonzero(charging_points)
    # every vehicle - that alone is an O(n_locations) scan per vehicle, i.e.
    # O(n_vehicles * n_locations) overall regardless of any cap on the reuse
    # search itself (see docstring; this is what made Stuttgart-scale runs
    # intractable even after capping the search length). Seeded with
    # whatever's already open (existing infra / prior scenario) so Phase 2
    # below can still find them if they need to grow beyond what Phase 1
    # alone would place there.
    opened_list = list(existing_idx)

    for g_idx, rows in enumerate(vehicle_groups.values()):
        starts = event_start_arr[rows]
        ends = starts + event_time_arr[rows]
        windows = list(zip(starts, ends))

        assigned = None

        # Phase 1: real existing infrastructure / prior-scenario locations
        # first, weighted by their installed point count (see
        # distribute_charging_events()'s Phase 1 - same rationale).
        #
        # Vectorized across ALL of existing_idx at once per time window,
        # instead of a Python-level "for cand in existing_idx" loop (one
        # numpy comparison touching every candidate simultaneously, repeated
        # only once per this vehicle's own distinct time windows - typically
        # a handful - rather than once per candidate). For home/work,
        # existing_idx is seeded from an earlier scenario year's real
        # charging locations (see run_de.py's previous_scenario_dir), which
        # for a big city's home_apartment/home_detached can already be tens
        # of thousands of locations on its own (869,015/737,395 nationwide
        # in the 2024 run) - a Python-level loop over all of them for every
        # one of a later, higher-demand year's hundreds of thousands of
        # vehicles is tens of billions of Python-level checks, confirmed to
        # make Berlin's 2037 home_apartment placement run for hours. A first
        # attempt fixed this by capping the search to the highest-weight
        # candidates (mirroring Phase 2's max_reuse_candidates cap below) -
        # reverted, since that made every OTHER existing location invisible
        # to Phase 1 for the rest of the run, defeating the whole point of
        # carrying real infrastructure forward across scenario years and
        # pushing vehicles into needlessly opening new locations instead.
        # Vectorizing keeps every existing location reachable while removing
        # the O(existing_idx) Python-level factor entirely.
        if existing_idx.size:
            # Tried combining ALL of a vehicle's windows into one fancy-index
            # call here (a real home-charging vehicle has ~80 charging
            # events/year, not the 1-4 a first synthetic benchmark assumed -
            # confirmed via real SimBEV data) instead of looping the check
            # once per window. Measured SLOWER, not faster (993ms/vehicle vs
            # 470ms/vehicle at 30k existing candidates x 80 windows): building
            # one huge (n_existing x combined_steps) temporary array in one
            # shot costs more in allocation/memory-bandwidth than it saves in
            # reduced call count. Reverted - the per-window loop below stays.
            fits_mask = np.ones(existing_idx.size, dtype=bool)
            for s, e in windows:
                if e > s:
                    # existing_availability (a contiguous mirror of just
                    # these rows - see above), not availability[existing_idx,
                    # s:e] - avoids a scattered gather out of the much bigger
                    # main array on every single vehicle.
                    in_use = existing_availability[:, s:e].max(axis=1)
                    fits_mask &= in_use < charging_points[existing_idx]
            if fits_mask.any():
                fit_idx = existing_idx[fits_mask]
                fit_weights = existing_weight[fits_mask]
                weight_sum = fit_weights.sum()
                assigned = (rng.choice(fit_idx, p=fit_weights / weight_sum)
                            if weight_sum > 0 else rng.choice(fit_idx))

        # Phase 2: prefer any already-opened location that can fit every one
        # of this vehicle's events without exceeding its current capacity
        # anywhere - capped to the most recently opened `max_reuse_candidates`
        # locations (see docstring: an unbounded scan doesn't finish for a
        # big city). Vectorized the same way as Phase 1 above - one numpy
        # check across the whole search_pool per time window, instead of a
        # Python loop with an early break. search_pool is already bounded to
        # max_reuse_candidates (~200), so this is a smaller win than Phase
        # 1's fix (which had nothing bounding it), but the same pattern and
        # still meaningfully cheaper at hundreds of thousands of vehicles.
        if assigned is None:
            tail = opened_list[-max_reuse_candidates:] if len(opened_list) > max_reuse_candidates else opened_list
            search_pool = np.asarray(tail, dtype=np.int64)
            if search_pool.size:
                # See Phase 1 above: combining all of a vehicle's windows
                # into one fancy-index call was tried and measured slower,
                # not faster (one big temporary array costs more than the
                # reduced call count saves) - kept as the per-window loop.
                fits_mask = np.ones(search_pool.size, dtype=bool)
                for s, e in windows:
                    if e > s:
                        in_use = availability[search_pool, s:e].max(axis=1)
                        fits_mask &= in_use < charging_points[search_pool]
                first_fit = np.flatnonzero(fits_mask)
                if first_fit.size:
                    # First candidate in search_pool's own order that fits -
                    # same "earliest match wins" semantics as the old
                    # Python-loop-with-break version.
                    assigned = search_pool[first_fit[0]]

        if assigned is None:
            assigned = rng.choice(n_locations, p=probabilities)
            was_unopened = charging_points[assigned] == 0
            # How much capacity is missing to fit every window of this
            # vehicle at the chosen location (usually 1, but a fallback pick
            # can land on a location already partially used by other
            # vehicles, needing more than +1 to cover the worst overlap).
            max_needed = 0
            for s, e in windows:
                if e <= s:
                    continue
                needed = int(availability[assigned, s:e].max()) - int(charging_points[assigned]) + 1
                max_needed = max(max_needed, needed)
            if max_needed > 0:
                prev_count = charging_points[assigned]
                prev_avg = average_charging_capacity[assigned]
                new_avg = (prev_avg * prev_count + capacity_arr[rows].mean() * max_needed) / (prev_count + max_needed)
                charging_points[assigned] += max_needed
                average_charging_capacity[assigned] = new_avg
            if was_unopened and charging_points[assigned] > 0:
                opened_list.append(assigned)

        pos = existing_pos.get(int(assigned))
        for s, e in windows:
            if e > s:
                availability[assigned, s:e] += 1
                if pos is not None:
                    # Keep the compact mirror (see above) in sync whenever
                    # the chosen location happens to be one of existing_idx's
                    # own - via Phase 1, Phase 2 (early on, before enough new
                    # locations have opened, opened_list's tail is still
                    # existing_idx entries), or even the fallback picking one
                    # by chance through the normal weight_column draw.
                    existing_availability[pos, s:e] += 1
        assigned_locations[rows] = assigned

        if g_idx % progress_step == 0 or g_idx == n_groups - 1:
            percent = (g_idx + 1) / n_groups * 100
            utility.safe_print(
                f"\r--- {label or 'distribute_charging_events_per_vehicle'}: "
                f"{g_idx + 1}/{n_groups} vehicles ({percent:.1f}%) ---",
                end="", flush=True)
    if n_groups:
        utility.safe_print("")

    locations["charging_points"] = charging_points
    locations["average_charging_capacity"] = average_charging_capacity.astype(int)

    events = events.copy()
    events["assigned_location"] = assigned_locations + location_id_start
    locations.index = locations.index + location_id_start

    if return_mask:
        return locations, events, availability
    return locations, events

def distribute_charging_events_fill_existing_only(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    max_shift_steps: int = 0,
    rng: np.random.Generator = None,
    availability_mask: np.array = None,
    additional_street_input: bool = False,
    location_id_start: int = 0
):
    """
    Distributes charging events to existing locations with available charging points.
    Does not add new charging points. If all charging points are filled, no further charging events are assigned.
    Allows rescheduling events by up to `max_shift_steps` time steps if no immediate availability is found.
    """
    if additional_street_input:
        availability_mask = np.zeros((len(locations), 2000))

    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    # NaN in the weight column (e.g. a merged candidate source with some
    # rows missing a computed area) would otherwise poison the whole
    # probabilities array and crash rng.choice - treat it as zero weight
    # (never picked) instead. If EVERY candidate ends up zero-weight (e.g. a
    # small Gemeinde where this weight column is entirely missing/zero), fall
    # back to uniform probabilities instead of dividing by a zero sum, which
    # would make every entry NaN and crash rng.choice just the same.
    # astype(float64): pd.concat with an empty, object-dtype sibling frame
    # (e.g. a use case's other candidate layer being empty for this Gemeinde)
    # silently upcasts an otherwise-numeric weight column to object dtype -
    # rng.choice then fails to cast the resulting probabilities to float64.
    weights = locations[weight_column].fillna(0).to_numpy().astype(np.float64)
    weights_sum = weights.sum()
    if weights_sum > 0:
        probabilities = weights / weights_sum
    else:
        probabilities = np.full(n_locations, 1.0 / n_locations)

    # Initial setup
    locations = locations.reset_index().copy()
    # locations = locations.copy()
    locations["charging_points"] = locations["charging_points"].astype(int)  # Ensure the column is integer
    assigned_locations = np.full(n_events, np.nan)

    # Availability matrix: rows=locations, cols=timesteps
    availability = availability_mask.copy()

    counter_redistributed_events = 0
    for idx in range(n_events):
        original_start = events.at[idx, "event_start"]
        base_duration = events.at[idx, "event_time"]
        energy = events.at[idx, "energy"]
        capacity = events.at[idx, "station_charging_capacity"]  # in kW

        assigned = None
        for shift in range(0, max_shift_steps + 1):
            start = original_start + shift

            # Berechne neue Dauer je nach verbleibender Zeit
            if base_duration - shift < energy / capacity * 4:
                duration = min(math.ceil(energy / capacity * 4), base_duration)
            else:
                duration = base_duration - shift

            end = start + duration

            if end > simulation_steps:
                continue  # Don't assign if end exceeds simulation time

            free_mask = availability[:, start:end].sum(axis=1) < locations["charging_points"].values
            if free_mask.any():
                assigned = np.argmax(free_mask)
                counter_redistributed_events += 1
                events.at[idx, "event_start"] = start
                events.at[idx, "event_time"] = duration
                break  # Exit the shift loop once assigned

        if assigned is not None:
            availability[assigned, start:end] += 1
            assigned_locations[idx] = locations.index[assigned]
        # else: Event bleibt unzugewiesen




    # Mark locations with assigned events
    events = events.copy()

    events["assigned_location"] = assigned_locations + location_id_start

    locations.index = locations.index + location_id_start

    #return assigned_locations, events
    return locations, events

# used in preprocessing only
def poi_cluster(poi_data, max_radius, max_weight, increment):
    coords = []
    weights = []
    areas = []
    while len(poi_data):
        radius = increment
        weight = 0
        # take point of first row
        coord = poi_data.iat[0, 0]
        condition = True
        while condition:
            # create radius circle around point
            area = coord.buffer(radius)
            # select all POI within circle
            in_area_bool = poi_data["geometry"].within(area)
            in_area = poi_data.loc[in_area_bool]
            weight = in_area["weight"].sum()
            radius += increment
            condition = radius <= max_radius and weight <= max_weight

        # calculate combined weight
        coords.append(coord)
        weights.append(weight)
        areas.append(radius - increment)
        # delete all used points from poi data
        poi_data = poi_data.drop(in_area.index.tolist())

    # create cluster geodataframe
    result_dict = {"geometry": coords, "potential": weights, "radius": areas}

    return gpd.GeoDataFrame(result_dict, crs="EPSG:3035")