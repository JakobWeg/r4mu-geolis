import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import numpy as np
import math
import use_case_helpers as uc_helpers


def distribute_charging_events(locations: gpd.GeoDataFrame, events: pd.DataFrame, weight_column: str, simulation_steps: int):

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

    events = events.sort_values(by="assigned_location")

    return locations, events
