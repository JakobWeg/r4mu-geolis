import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import numpy as np
import math
import use_case_helpers as uc_helpers


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
    fill_existing_only: bool = False,  # New behavior
    availability_mask: np.array = None,
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
            locations, events, weight_column, simulation_steps, rng, availability_mask
        )

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
    availability = np.zeros((n_locations, simulation_steps), dtype=int)

    print("Distributing charging events...")

    for idx in range(n_events):
        start = events.at[idx, "event_start"]
        duration = events.at[idx, "event_time"]
        end = start + duration
        capacity = events.at[idx, "station_charging_capacity"]  # in kW

        if fill_existing_first:

            if availability.size < 1:
                print()
            if start >= end:
                print("HILFEEEEEEE")
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
    rng: np.random.Generator = None,
    availability_mask: np.array = None
):
    """
    Distributes charging events to existing locations with available charging points.
    Does not add new charging points. If all charging points are filled, no further charging events are assigned.
    """

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