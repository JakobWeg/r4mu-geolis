import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import math
import use_case_helpers as uc_helpers


def hpc(hpc_data: gpd.GeoDataFrame,
        uc_dict, timestep=15):
    """
    Calculate placements and energy distribution for use case hpc.

    :param hpc_points: gpd.GeoDataFrame
        GeoDataFrame of possible hpc locations
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    :param timestep: int
        time step of the simbev input series, default: 15 (minutes)
    """
    uc_id = 'hpc'
    print('Use case: ', uc_id)

    charging_events = uc_dict["charging_event"]
    in_region = hpc_data
    in_region = in_region.iloc[:800]

    num_hpc = charging_events.loc[charging_events["charging_use_case"] == "urban_fast"].shape[0]
    # num_hpc = charging_events.loc[charging_events["charging_use_case"].within(["urban_fast", "highway_fast"])]

    if num_hpc > 0:
        charging_locations_hpc, located_charging_events = uc_helpers.distribute_charging_events(in_region, charging_events,
                                                                                                   weight_column="potential",
                                                                                                   simulation_steps=2000)

        # Merge Chargin_events and Locations
        charging_locations_hpc["index"] = charging_locations_hpc.index
        located_charging_events = located_charging_events.merge(charging_locations_hpc, left_on="assigned_location",
                                                                right_on="index")

        located_charging_events_gdf = gpd.GeoDataFrame(located_charging_events, geometry="geometry")
        located_charging_events_gdf.set_crs(3035)

        cols = ["geometry", "charge_spots", "energy"]
        utility.save(charging_locations_hpc, uc_id, "charging-locations", uc_dict)
        utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    else:
        print("No hpc charging in timeseries")


def public(
        public_data: gpd.GeoDataFrame,
        uc_dict, timestep=15):
    uc_id = "public"
    print("Use case: " + uc_id)
    charging_events = uc_dict["charging_event"]

    in_region = public_data
    in_region = in_region.iloc[:800]
    charging_locations_public, located_charging_events = uc_helpers.distribute_charging_events(in_region, charging_events,
                                                                                             weight_column="potential",
                                                                                             simulation_steps=2000)

    # Merge Chargin_events and Locations
    charging_locations_public["index"]=charging_locations_public.index
    located_charging_events = located_charging_events.merge(charging_locations_public, left_on="assigned_location",
                                                            right_on="index")

    located_charging_events_gdf = gpd.GeoDataFrame(located_charging_events, geometry="geometry")
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_public, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

def home(
        home_data: gpd.GeoDataFrame,
        uc_dict, mode):
    """
    Calculate placements and energy distribution for use case hpc.

    :param home_data: gpd.GeoDataFrame
        info about house types
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    :param home_charge_prob: float
        probability of privately available home charging
    :param car_num: pd.Series
        total cars per car type in scenario
    :param timestep: int
        time step of the simbev input series, default: 15 (minutes)
    """

    # todo: add probability for charging infrastructure at home. Select homes that are not possible to be electrified

    uc_id = "home"
    print("Use case: " + uc_id)

    if mode == "apartment":
        uc_id = "home_apartment"
        print("Use case: " + uc_id)
        charging_events = uc_dict["charging_event"].loc[
            uc_dict["charging_event"]["charging_use_case"].isin(["home_apartment"])]
    elif mode == "detached":
        uc_id = "home_detached"
        print("Use case: " + uc_id)
        charging_events = uc_dict["charging_event"].loc[
            uc_dict["charging_event"]["charging_use_case"].isin(["home_detached"])]
    else:
        print("wrong mode")

    # filter houses by region
    #in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    #in_region = home_data.loc[in_region_bool].copy()
    in_region = home_data
    in_region = in_region.iloc[:800]
    charging_locations_home, located_charging_events = uc_helpers.distribute_charging_events(in_region, charging_events,
                                                                                             weight_column="building_area",
                                                                                             simulation_steps=2000)

    # Merge Chargin_events and Locations
    charging_locations_home["index"]=charging_locations_home.index
    located_charging_events = located_charging_events.merge(charging_locations_home, left_on="assigned_location",
                                                            right_on="index")
    drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    located_charging_events = located_charging_events.drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(located_charging_events, geometry="geometry")
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_home, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

def work(
        work_data,
        uc_dict, timestep=15):
    print("distributing uc work...")
    uc_id = "work"
    print("Use case: " + uc_id)

    charging_events = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["work"])]

    # filter houses by region
    #in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    #in_region = home_data.loc[in_region_bool].copy()
    in_region = work_data
    in_region = in_region.iloc[:800]
    charging_locations_retail, located_charging_events = uc_helpers.distribute_charging_events(in_region, charging_events,
                                                                                             weight_column="area",
                                                                                             simulation_steps=2000)

    # Merge Chargin_events and Locations
    charging_locations_retail["index"]=charging_locations_retail.index
    located_charging_events = located_charging_events.merge(charging_locations_retail, left_on="assigned_location",
                                                            right_on="index")
    # drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    located_charging_events = located_charging_events   #.drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(located_charging_events, geometry="geometry")
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_retail, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)


def retail(retail_data: gpd.GeoDataFrame,
        uc_dict):

    """
    Calculate placements and energy distribution for use case hpc.

    :param retail_data: gpd.GeoDataFrame
        info about house types
    :param uc_dict: dict
        contains basic run info like region boundary and save directory

    """
    uc_id = "retail"
    print("Use case: " + uc_id)

    charging_events = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["retail"])]

    # filter houses by region
    #in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    #in_region = home_data.loc[in_region_bool].copy()
    in_region = retail_data
    in_region = in_region.iloc[:800]
    charging_locations_retail, located_charging_events = uc_helpers.distribute_charging_events(in_region, charging_events,
                                                                                             weight_column="area",
                                                                                             simulation_steps=2000)

    # Merge Chargin_events and Locations
    charging_locations_retail["index"]=charging_locations_retail.index
    located_charging_events = located_charging_events.merge(charging_locations_retail, left_on="assigned_location",
                                                            right_on="index")
    # drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    located_charging_events = located_charging_events   #.drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(located_charging_events, geometry="geometry")
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_retail, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)