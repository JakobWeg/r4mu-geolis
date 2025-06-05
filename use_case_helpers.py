import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import numpy as np
import math

def postprocess_public_demands(charging_locations: gpd.GeoDataFrame, located_charging_events: gpd.GeoDataFrame):

    print("--- postprocessing of public demands started... ---")

    max_distance = 1000 # Meter

    # Filter street locations und home_street Events
    street_locations = charging_locations[charging_locations["mode"] == "street"].copy()
    home_events = located_charging_events[located_charging_events["mode"] == "home_street"].copy()
    street_events = located_charging_events[located_charging_events["mode"] == "street"].copy()

    # Maximaler Zeitschritt (für Maskenlänge)
    max_step = max(located_charging_events["event_start"].max(), (located_charging_events["event_start"]+located_charging_events["event_time"]).max())

    # Belegungs-Maske: location_id -> np.array (Zeitschritte)
    occupancy_mask = {}
    for loc_id in charging_locations["location_id"]:
        occupancy_mask[loc_id] = np.zeros(max_step + 1, dtype=int)

    # Maske mit bestehenden street Events füllen
    for _, event in located_charging_events.iterrows():
        loc_id = event["location_id"]
        mode = event["mode"]
        start = event["event_start"]
        end = event["event_start"]+event["event_time"]
        if loc_id in occupancy_mask:
            occupancy_mask[loc_id][start:end] += 1

    # Räumlicher Index für street locations
    street_locations_sindex = street_locations.sindex

    umverteilte_events = 0
    zugeschlagene_punkte = 0

    for idx, event in located_charging_events.iterrows():

        if event["mode"] == "home_street":
            event_point = event.geometry
            start = event["event_start"]
            end = event["event_start"]+event["event_time"]
            old_location_id = event["location_id"]

            # Kandidaten in 500m suchen
            candidate_idx = list(street_locations_sindex.intersection(event_point.buffer(max_distance).bounds))
            candidate_locs = street_locations.iloc[candidate_idx]
            candidate_locs = candidate_locs[candidate_locs.geometry.distance(event_point) <= max_distance]

            if candidate_locs.empty:
                continue  # Kein Standort in Reichweite

            # Prüfe freie Kapazität in Maske
            loc_with_free_capacity = None
            for _, loc in candidate_locs.iterrows():
                loc_id = loc["location_id"]
                charging_points = loc["charging_points"]
                occupancy = occupancy_mask[loc_id][start:end]

                if np.all(occupancy < charging_points):
                    loc_with_free_capacity = loc
                    break

            if loc_with_free_capacity is not None:
                new_location_id = loc_with_free_capacity["location_id"]
            # else:
                # Kein freier Punkt: nächstgelegenen Standort wählen, Ladepunkt hinzufügen
                # candidate_locs["dist"] = candidate_locs.geometry.distance(event_point)
                # nearest_loc = candidate_locs.sort_values("dist").iloc[0]
                # new_location_id = nearest_loc["location_id"]
                #
                # charging_locations.loc[
                #     charging_locations["location_id"] == new_location_id, "charging_points"
                # ] += 1
                #
                # zugeschlagene_punkte += 1

                # if new_location_id not in occupancy_mask:
                #     occupancy_mask[new_location_id] = np.zeros(max_step + 1, dtype=int)

                # Ladevent umverteilen
                located_charging_events.at[idx, "location_id"] = new_location_id
                located_charging_events.at[idx, "mode"] = "street"

                # Maske updaten
                # Abziehen der belegten Zeitschritte von der ursprünglichen home_street Location
                occupancy_mask[old_location_id][start:end] -= 1

                # Hinzufügen der belegten Zeitschritte zu der neuen location_id
                occupancy_mask[new_location_id][start:end] += 1

                umverteilte_events += 1

    print(f"Anzahl umverteilter Ladeevents von 'home_street' auf 'street': {umverteilte_events}")
    print(f"Anzahl neu hinzugefügter Ladepunkte an street-Standorten: {zugeschlagene_punkte}")

    # Berechnung der maximalen gleichzeitigen Belegung je Location
    max_concurrent_demand = {}
    for loc_id in occupancy_mask:
        # Maximale Belegung für die Location berechnen (maximale Anzahl von Ladeevents zu einem Zeitpunkt)
        max_concurrent_demand[loc_id] = occupancy_mask[loc_id].max()

        # Vergleiche maximale Belegung mit Ladepunkten der Location
        current_charging_points = charging_locations.loc[charging_locations["location_id"] == loc_id, "charging_points"].values[0]
        if max_concurrent_demand[loc_id] > current_charging_points:
            # Wenn mehr Ladeevents gleichzeitig laufen, als Ladepunkte vorhanden sind, erhöhen wir die Ladepunkte
            required_points = max_concurrent_demand[loc_id] - current_charging_points
            charging_locations.loc[charging_locations["location_id"] == loc_id, "charging_points"] += required_points


            # Auch in der Belegungsmaske für diese Location die Ladepunkte erhöhen
            zugeschlagene_punkte += required_points
        elif max_concurrent_demand[loc_id] < current_charging_points:
            # Wenn weniger Ladeevents gleichzeitig belegt sind als Ladepunkte verfügbar, reduzieren wir die Ladepunkte
            charging_locations.loc[charging_locations["location_id"] == loc_id, "charging_points"] = max_concurrent_demand[
                loc_id]
    print(f"Maximale gleichzeitige Belegung für jede Location berechnet.")

    return charging_locations, located_charging_events

def park_time_limitation(charging_events, data_dict, charging_use_case):
    print("limit parking time")

    df = charging_events # .loc[charging_events["charging_use_case"] == charging_use_case].copy()

    # if start >= start_grenze and start < end_grenze:
    #     if (row['energy'] / row['station_charging_capacity'] * 4) > limit_schritte:
    #         return row['energy'] / row['station_charging_capacity'] * 4
    #     return min(row['event_time'], limit_schritte)

    limit_schritte = data_dict["charging_time_limit_duration"] # 4h
    tag_laenge = 96 # 24h
    start_grenze = data_dict["charging_time_limit_start"] # 9:00
    end_grenze = data_dict["charging_time_limit_end"] # 21:00

    def begrenze_event(row):
        start = row['event_start']
        dauer = row['event_time']
        lade = row['energy'] / row['station_charging_capacity'] * 4
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
    df['event_time'].loc[charging_events["charging_use_case"] == charging_use_case] = df.loc[charging_events["charging_use_case"] == charging_use_case].apply(begrenze_event, axis=1)
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
        "depot": "7"
    }

    location_id = location_id.astype(int)
    uc_id = use_case_map.get(use_case_id)

    ids = location_id.astype(str).apply(lambda x: int(uc_id + x))

    return ids.values.astype(int)

#todo: mark charging events that got transfered to retail-parking
#todo: transfer multi-use events just from commercial street to retail

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
    return_mask: bool = False
):
    """
    Distributes charging events to locations with optional random assignment.
    Tracks number of charging points and average charging capacity per location.
    If 'fill_existing_only' is True, only existing charging points are filled.
    """

    if fill_existing_only:
        print("Using the 'fill_existing_only' method: Only existing charging points will be filled.")
        return distribute_charging_events_fill_existing_only(
            locations, events, weight_column, simulation_steps, flexibility_multi_use, rng, availability_mask
        )

    # if home_street:
    #     n_locations_home_street = len(locations[locations["mode"]== "home_street"])
    #     n_locations_not_home_street = len(locations[locations["mode"]== "not_home_street"])
    #
    # else:
    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    probabilities = locations[weight_column].values / locations[weight_column].sum()

    # Initial setup
    locations = locations.reset_index().copy()
    locations["charging_points"] = 0
    locations["average_charging_capacity"] = 0.0  # in kW
    assigned_locations = np.full(n_events, np.nan)

    # Create availability matrix: rows=locations, cols=timesteps
    # todo: exchange locations with availability_mask, hier gibt es ein Problem mit dem index aus der availability und dem index des DataFrames, Abgl
    # if home_street:
    #     availability_home_street = np.zeros((n_locations_home_street, simulation_steps), dtype=int)
    #     availability_not_home_street = np.zeros((n_locations_not_home_street, simulation_steps), dtype=int)
    # else:
    availability = np.zeros((n_locations, simulation_steps), dtype=int)

    print("Distributing charging events...")

    for idx in range(n_events):
        start = events.at[idx, "event_start"]
        duration = events.at[idx, "event_time"]
        end = start + duration
        capacity = events.at[idx, "station_charging_capacity"]  # in kW
        # if events.at[idx, "charging_use_case"] == "public" and events.at[idx, "location"] == "home":

        if fill_existing_first:

            if availability.size < 1:
                print()
            if start >= end:
                print("Fehler bei übergebener Maske zur Übertragung von Ladeevents")
            in_use = availability[:, start:end].max(axis=1)
            required = locations["charging_points"].values
            free_mask = in_use < required

            if free_mask.any():
                assigned = np.argmax(free_mask)
            else:
                assigned = rng.choice(n_locations, p=probabilities)
                # Increase number of charging points
                loc_idx = locations.index[assigned]
                prev_count = locations.at[loc_idx, "charging_points"]
                prev_avg = locations.at[loc_idx, "average_charging_capacity"]
                new_avg = (prev_avg * prev_count + capacity) / (prev_count + 1)
                locations.at[loc_idx, "charging_points"] += 1
                locations.at[loc_idx, "average_charging_capacity"] = new_avg
        else:
            assigned = rng.choice(n_locations, p=probabilities)
            loc_idx = locations.index[assigned]
            prev_count = locations.at[loc_idx, "charging_points"]
            prev_avg = locations.at[loc_idx, "average_charging_capacity"]
            new_avg = (prev_avg * prev_count + capacity) / (prev_count + 1)
            locations.at[loc_idx, "charging_points"] += 1
            locations.at[loc_idx, "average_charging_capacity"] = new_avg

        availability[assigned, start:end] += 1
        assigned_locations[idx] = locations.index[assigned]

        if n_events > 10000 and idx % (n_events // 10000 + 1) == 0:
            percent = (idx + 1) / n_events * 100
            print(f"\rProgress: {percent:.2f}%", end='', flush=True)

    print("\nDone.")

    locations["average_charging_capacity"] = locations["average_charging_capacity"].astype(int)

    events = events.copy()
    events["assigned_location"] = assigned_locations

    if return_mask:
        return locations, events, availability
    else:
        return locations, events

def distribute_charging_events_fill_existing_only(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    max_shift_steps: int = 0,
    rng: np.random.Generator = None,
    availability_mask: np.array = None
):
    """
    Distributes charging events to existing locations with available charging points.
    Does not add new charging points. If all charging points are filled, no further charging events are assigned.
    Allows rescheduling events by up to `max_shift_steps` time steps if no immediate availability is found.
    """

    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    probabilities = locations[weight_column].values / locations[weight_column].sum()

    # Initial setup
    locations = locations.reset_index().copy()
    locations["charging_points"] = locations["charging_points"].astype(int)  # Ensure the column is integer
    assigned_locations = np.full(n_events, np.nan)

    # Availability matrix: rows=locations, cols=timesteps
    availability = availability_mask.copy()

    print("Distributing charging events (only to existing charging points)...")
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
                break  # Exit the shift loop once assigned

        if assigned is not None:
            availability[assigned, start:end] += 1
            assigned_locations[idx] = locations.index[assigned]
        # else: Event bleibt unzugewiesen

    print(f"Total redistributed events: {counter_redistributed_events}")

    print("transfered multi-use charging events:", counter_redistributed_events)

    # Mark locations with assigned events
    events = events.copy()
    events["assigned_location"] = assigned_locations

    #return assigned_locations, events
    return locations, events


def distribute_charging_events_fill_existing_only_old(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    max_shift_steps: int = 8,
    rng: np.random.Generator = None,
    availability_mask: np.array = None
):
    """
    Distributes charging events to existing locations with available charging points.
    Does not add new charging points. If all charging points are filled, no further charging events are assigned.
    """

    szenario_time_reschedule = True

    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    probabilities = locations[weight_column].values / locations[weight_column].sum()

    # Initial setup
    locations = locations.reset_index().copy()
    locations["charging_points"] = locations["charging_points"].astype(int)  # Ensure the column is integer
    assigned_locations = np.full(n_events, np.nan)

    # Create availability matrix: rows=locations, cols=timesteps
    #availability = np.zeros((n_locations, simulation_steps), dtype=int)
    availability = availability_mask.copy()

    print("Distributing charging events (only to existing charging points)...")
    counter_redistributed_events = 0
    for idx in range(n_events):
        start = events.at[idx, "event_start"]
        duration = events.at[idx, "event_time"]
        end = start + duration
        capacity = events.at[idx, "station_charging_capacity"]  # in kW

        # Find locations with available charging points in the time range
        free_mask = availability[:, start:end].sum(axis=1) < locations["charging_points"].values
        # Assign event only to locations with available charging points
        if free_mask.any():
            assigned = np.argmax(free_mask)  # Assign to first free location
            #print("!!! free lp available")
            counter_redistributed_events += 1
        else:
            if szenario_time_reschedule:
                # todo jedes mal die Endzeit nicht verlängern, wenn der Zeitraum reicht um die Energie nachzuladen. In config
                start = events.at[idx, "event_start"] + 4
                capacity = events.at[idx, "station_charging_capacity"]  # in kW
                if events.at[idx, "event_time"] - 4 < events.at[idx, "energy"]/capacity*4:
                    duration = min(math.ceil(events.at[idx, "energy"]/capacity*4), events.at[idx, "event_time"])
                else:
                    duration = events.at[idx, "event_time"] - 4
                end = start + duration
                # Find locations with available charging points in the time range
                free_mask = availability[:, start:end].sum(axis=1) < locations["charging_points"].values
                if free_mask.any():
                    assigned = np.argmax(free_mask)  # Assign to first free location
                    # print("!!! free lp available")
                    counter_redistributed_events += 1
                else:
                    start = events.at[idx, "event_start"] + 8
                    capacity = events.at[idx, "station_charging_capacity"]  # in kW
                    if events.at[idx, "event_time"] - 8 < events.at[idx, "energy"] / capacity * 4:
                        duration = min(math.ceil(events.at[idx, "energy"] / capacity * 4), events.at[idx, "event_time"])
                    else:
                        duration = events.at[idx, "event_time"] - 8
                    end = start + duration
                    # Find locations with available charging points in the time range
                    free_mask = availability[:, start:end].sum(axis=1) < locations["charging_points"].values
                    if free_mask.any():
                        assigned = np.argmax(free_mask)  # Assign to first free location
                        # print("!!! free lp available")
                        counter_redistributed_events += 1
                    else:
                        # No more available charging points
                        #print(f"Event {idx} could not be assigned to any location (no available charging points).")
                        continue  # Skip this event as it cannot be assigned

        # Assign the event to the location
        availability[assigned, start:end] += 1
        assigned_locations[idx] = locations.index[assigned]

    print("transfered multi-use charging events:", counter_redistributed_events)

    # Mark locations with assigned events
    events = events.copy()
    events["assigned_location"] = assigned_locations

    return locations, events

def distribute_charging_events_old(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    fill_existing_first: bool = True,
    rng: np.random.Generator = None,
    return_mask: bool = False
):
    """
    Distributes charging events to locations with optional random assignment.
    Tracks number of charging points and average charging capacity per location.
    """

    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    probabilities = locations[weight_column].values / locations[weight_column].sum()

    # Initial setup
    locations = locations.reset_index().copy()
    locations["charging_points"] = 0
    locations["average_charging_capacity"] = 0.0  # in kW
    assigned_locations = np.full(n_events, np.nan)

    # Create availability matrix: rows=locations, cols=timesteps
    availability = np.zeros((n_locations, simulation_steps), dtype=int)

    print("Distributing charging events...")

    for idx in range(n_events):
        start = events.at[idx, "event_start"]
        duration = events.at[idx, "event_time"]
        end = start + duration
        capacity = events.at[idx, "station_charging_capacity"]  # in kW

        if fill_existing_first:
            in_use = availability[:, start:end].max(axis=1)
            required = locations["charging_points"].values
            free_mask = in_use < required

            if free_mask.any():
                assigned = np.argmax(free_mask)
            else:
                assigned = rng.choice(n_locations, p=probabilities)
                # Increase number of charging points
                loc_idx = locations.index[assigned]
                prev_count = locations.at[loc_idx, "charging_points"]
                prev_avg = locations.at[loc_idx, "average_charging_capacity"]
                new_avg = (prev_avg * prev_count + capacity) / (prev_count + 1)
                locations.at[loc_idx, "charging_points"] += 1
                locations.at[loc_idx, "average_charging_capacity"] = new_avg
        else:
            assigned = rng.choice(n_locations, p=probabilities)
            loc_idx = locations.index[assigned]
            prev_count = locations.at[loc_idx, "charging_points"]
            prev_avg = locations.at[loc_idx, "average_charging_capacity"]
            new_avg = (prev_avg * prev_count + capacity) / (prev_count + 1)
            locations.at[loc_idx, "charging_points"] += 1
            locations.at[loc_idx, "average_charging_capacity"] = new_avg

        availability[assigned, start:end] += 1
        assigned_locations[idx] = locations.index[assigned]

        if n_events > 10000 and idx % (n_events // 10000 + 1) == 0:
            percent = (idx + 1) / n_events * 100
            print(f"\rProgress: {percent:.2f}%", end='', flush=True)

    print("\nDone.")

    locations["average_charging_capacity"] = locations["average_charging_capacity"].astype(int)

    events = events.copy()
    events["assigned_location"] = assigned_locations

    # locations = locations[locations["charging_points"] != 0]

    if return_mask:
        return locations, events, availability
    else:
        return locations, events

def distribute_charging_events_with_seperation_of_power(
    locations: gpd.GeoDataFrame,
    events: pd.DataFrame,
    weight_column: str,
    simulation_steps: int,
    fill_existing_first: bool = True
):
    """
    Fast version of the charging event distributor using vectorized numpy logic.
    Supports dynamic charging power classes.
    """

    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    probabilities = locations[weight_column].values / locations[weight_column].sum()

    # Initial setup
    locations = locations.copy()
    assigned_locations = np.full(n_events, np.nan)

    # Detect all unique charging capacities from events
    charging_capacities = events["station_charging_capacity"].unique()
    charging_capacities.sort()

    # Create a column for each charging capacity
    for capacity in charging_capacities:
        col_name = f"charging_points_{int(capacity)}kW"
        locations[col_name] = 0

    # Create availability matrix: rows=locations, cols=timesteps
    availability = np.zeros((n_locations, simulation_steps), dtype=int)

    print("Distributing charging events...")

    for idx in range(n_events):
        start = events.at[idx, "event_start"]
        duration = events.at[idx, "event_time"]
        end = start + duration
        capacity = events.at[idx, "station_charging_capacity"]
        capacity_col = f"charging_points_{int(capacity)}kW"

        if fill_existing_first:
            # Check usage during event period
            in_use = availability[:, start:end].max(axis=1)
            required = locations[capacity_col].values
            free_mask = in_use < required

            if free_mask.any():
                assigned = np.argmax(free_mask)
            else:
                assigned = np.random.choice(n_locations, p=probabilities)
                locations.at[locations.index[assigned], capacity_col] += 1
        else:
            assigned = np.random.choice(n_locations, p=probabilities)
            locations.at[locations.index[assigned], capacity_col] += 1

        # Mark time range as occupied
        availability[assigned, start:end] += 1
        assigned_locations[idx] = locations.index[assigned]

        if n_events > 10000 and idx % (n_events // 10000 + 1) == 0:
            percent = (idx + 1) / n_events * 100
            print(f"\rProgress: {percent:.2f}%", end='', flush=True)

    print("\nDone.")

    # sum up charging_points


    events = events.copy()
    events["assigned_location"] = assigned_locations

    return locations, events

def distribute_charging_events_a(locations: gpd.GeoDataFrame, events: pd.DataFrame, weight_column: str,
                               simulation_steps: int, fill_existing_first: bool = True):
    """
    Fast version of the charging event distributor using vectorized numpy logic.
    """


    n_locations = len(locations)
    n_events = len(events)

    # Normalize weights
    probabilities = locations[weight_column].values / locations[weight_column].sum()

    # Initial setup
    locations = locations.copy()
    locations["charging_points"] = 0
    assigned_locations = np.full(n_events, np.nan)

    # Create availability matrix: rows=locations, cols=timesteps
    availability = np.zeros((n_locations, simulation_steps), dtype=int)

    print("Distributing charging events...")

    for idx in range(n_events):
        start = events.at[idx, "event_start"]
        duration = events.at[idx, "event_time"]
        end = start + duration

        if fill_existing_first:
            # Check usage during event period
            in_use = availability[:, start:end].max(axis=1)
            required = locations["charging_points"].values
            free_mask = in_use < required

            if free_mask.any():
                assigned = np.argmax(free_mask)
            else:
                assigned = np.random.choice(n_locations, p=probabilities)
                locations.at[locations.index[assigned], "charging_points"] += 1
        else:
            # Skip checking for availability, assign randomly
            assigned = np.random.choice(n_locations, p=probabilities)
            locations.at[locations.index[assigned], "charging_points"] += 1

        # Mark time range as occupied
        availability[assigned, start:end] += 1
        assigned_locations[idx] = locations.index[assigned]

        if n_events > 10000 and idx % (n_events // 10000 + 1) == 0:
            percent = (idx + 1) / n_events * 100
            print(f"\rProgress: {percent:.2f}%", end='', flush=True)

    print("\nDone.")

    events = events.copy()
    events["assigned_location"] = assigned_locations

    return locations, events

def distribute_charging_events_slow(locations: gpd.GeoDataFrame, events: pd.DataFrame, weight_column: str, simulation_steps: int):

    """
    Distributes charging events across charging locations considering temporal overlaps.

    Parameters:
        locations (gpd.GeoDataFrame): Geodataframe containing charging locations and their weights.
        events (pd.DataFrame): Dataframe containing charging events with start and end time.
        weight_column (str): Column in `locations` containing the weights for distribution.
        simulation_steps (int): Number of time steps in the simulation.

    Returns:
        gpd.GeoDataFrame: Updated locations with the number of required charging points.
        pd.DataFrame: Updated events with assigned charging locations.
    """

    # Normalize weights to probabilities
    locations["probability"] = locations[weight_column] / locations[weight_column].sum()

    # Initialize required points column in locations
    locations["required_points"] = 0

    # Create a time-step tracker for each location
    location_availability = {
        loc_id: [0] * simulation_steps for loc_id in locations.index
    }

    # Add a column to events to store assigned location
    events["assigned_location"] = np.nan

    len_events = len(events)
    print("distributing...")
    # Distribute events
    for event_idx, event in events.iterrows():
        start_time, end_time = event["event_start"], (event["event_start"] + event["event_time"])

        # 1. Suche einen Ort, der im ganzen Zeitraum noch Kapazität hat
        assigned_location = None
        for loc_id in locations.index:
            # Ladepunkt-Anzahl an diesem Standort
            max_points = locations.at[loc_id, "required_points"]

            # Belegte Punkte in dem Zeitraum
            max_in_use = max(location_availability[loc_id][start_time:end_time])

            if max_in_use < max_points:
                assigned_location = loc_id
                break  # Freier Ladepunkt gefunden

        # 2. Falls kein Standort Kapazität hat → zufällig einen auswählen und neuen Punkt hinzufügen
        if assigned_location is None:
            assigned_location = np.random.choice(locations.index, p=locations["probability"])
            locations.at[assigned_location, "required_points"] += 1

        # 3. Update Belegung für den zugewiesenen Standort
        for time in range(start_time, end_time):
            location_availability[assigned_location][time] += 1

        events.at[event_idx, "assigned_location"] = assigned_location

        # Progress output
        idx = int(event_idx)
        percent_complete = (idx + 1) / len_events * 100
        if (idx + 1) % (len_events // 10000 + 1) == 0:
            print(f"\rProgress: {percent_complete:.2f}%", end='', flush=True)

    events = events.sort_values(by="assigned_location")
    print()

    return locations, events

def distribute_charging_events_dumb(locations: gpd.GeoDataFrame, events: pd.DataFrame, weight_column: str, simulation_steps: int):

    """
    Distributes charging events across charging locations considering temporal overlaps.

    Parameters:
        locations (gpd.GeoDataFrame): Geodataframe containing charging locations and their weights.
        events (pd.DataFrame): Dataframe containing charging events with start and end time.
        weight_column (str): Column in `locations` containing the weights for distribution.
        simulation_steps (int): Number of time steps in the simulation.

    Returns:
        gpd.GeoDataFrame: Updated locations with the number of required charging points.
        pd.DataFrame: Updated events with assigned charging locations.
    """
    # Normalize weights to probabilities
    locations["probability"] = locations[weight_column] / locations[weight_column].sum()

    # Initialize required points column in locations
    locations["required_points"] = 0

    # Create a time-step tracker for each location
    location_availability = {
        loc_id: [0] * simulation_steps for loc_id in locations.index
    }

    # Add a column to events to store assigned location
    events["assigned_location"] = np.nan

    len_events = len(events)
    print("distributing...")
    # Distribute events
    for event_idx, event in events.iterrows():
        start_time, end_time = event["event_start"], (event["event_start"] + event["event_time"])

        # Select a location purely based on probability
        assigned_location = np.random.choice(locations.index, p=locations["probability"])
        events.at[event_idx, "assigned_location"] = assigned_location

        # Check if all existing points are occupied during the event's time range
        max_points_in_use = max(location_availability[assigned_location][start_time:end_time])

        # Increment the required points counter only if all current points are occupied
        for time in range(start_time, end_time):
            location_availability[assigned_location][time] += 1

        locations.at[assigned_location, "required_points"] = max(
            locations.at[assigned_location, "required_points"],
            max_points_in_use + 1
        )
        idx = int(event_idx)
        percent_complete = (idx + 1) / len_events * 100
        if (idx + 1) % (len_events // 10000) == 0:
            print(f"\rProgress: {percent_complete:.2f}%", end='', flush=True) #, end="\r"

    events = events.sort_values(by="assigned_location")
    print()

    return locations, events

# used in preprocessing only
def poi_cluster(poi_data, max_radius, max_weight, increment):
    coords = []
    weights = []
    areas = []
    print("POI in area: {}".format(len(poi_data)))
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