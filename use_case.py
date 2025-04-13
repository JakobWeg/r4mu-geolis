import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import math
import use_case_helpers as uc_helpers


def hpc(hpc_data: gpd.GeoDataFrame, uc_dict, timestep=15):
    """
    Calculate placements and energy distribution for use case hpc.

    :param hpc_points: gpd.GeoDataFrame
        GeoDataFrame of possible hpc locations
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    :param timestep: int
        time step of the simbev input series, default: 15 (minutes)
    """
    uc_id = "hpc"
    print("Use case: ", uc_id)

    # charging_events = uc_dict["charging_event"].loc[
    #     uc_dict["charging_event"]["charging_use_case"].isin(["urban_fast"])]

    charging_events = (
        uc_dict["charging_event"]
        .loc[
            uc_dict["charging_event"]["charging_use_case"].isin(["urban_fast"])
            & ~uc_dict["charging_event"]["location"].isin(["shopping"])
        ]
        .reset_index()
    )

    in_region = hpc_data
    # in_region = in_region.iloc[:800]

    num_hpc = charging_events.loc[
        charging_events["charging_use_case"] == "urban_fast"
    ].shape[0]
    # num_hpc = charging_events.loc[charging_events["charging_use_case"].within(["urban_fast", "highway_fast"])]

    if num_hpc > 0:
        (
            charging_locations_hpc,
            located_charging_events,
        ) = uc_helpers.distribute_charging_events(
            in_region, charging_events, weight_column="count", simulation_steps=2000
        )

        # Merge Chargin_events and Locations
        charging_locations_hpc["index"] = charging_locations_hpc.index
        located_charging_events = located_charging_events.merge(
            charging_locations_hpc, left_on="assigned_location", right_on="index"
        )

        located_charging_events_gdf = gpd.GeoDataFrame(
            located_charging_events, geometry="geometry"
        )
        located_charging_events_gdf.to_crs(3035)

        cols = ["geometry", "charge_spots", "energy"]
        utility.save(charging_locations_hpc, uc_id, "charging-locations", uc_dict)
        utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

        print("distribution of hpc-charging-points successful")

    else:
        print("No hpc charging in timeseries")


def public(
    public_data_home_street: gpd.GeoDataFrame,
    public_data_not_home_street: gpd.GeoDataFrame,
    uc_dict,
    timestep=15,
):
    uc_id = "public"
    print("Use case: " + uc_id)
    charging_events = (
        uc_dict["charging_event"]
        .loc[uc_dict["charging_event"]["charging_use_case"] == "street"]
        .reset_index()
    )
    charging_events_home_street = charging_events.loc[
        charging_events["location"] == "home"
    ].reset_index()
    charging_events_not_home_street = charging_events.loc[
        charging_events["location"] != "home"
    ].reset_index()
    in_region_home_street = public_data_home_street
    in_region_not_home_street = public_data_not_home_street

    # in_region = in_region.iloc[:800]
    (
        charging_locations_public_home,
        located_charging_events_public_home,
    ) = uc_helpers.distribute_charging_events(
        in_region_home_street,
        charging_events_home_street,
        weight_column="potential",
        simulation_steps=1000,
    )

    (
        charging_locations_public,
        located_charging_events_public,
    ) = uc_helpers.distribute_charging_events(
        in_region_not_home_street,
        charging_events_not_home_street,
        weight_column="potential",
        simulation_steps=1000,
    )

    located_charging_events_public_home[
        "assigned_location"
    ] = located_charging_events_public_home["assigned_location"] + len(
        charging_locations_public
    )

    # concat charging events and location at home and public
    charging_locations = pd.concat(
        [charging_locations_public, charging_locations_public_home], ignore_index=True
    )
    located_charging_events = pd.concat(
        [located_charging_events_public, located_charging_events_public_home],
        ignore_index=True,
    )

    # Merge Chargin_events and Locations
    charging_locations["index"] = charging_locations.index
    located_charging_events = located_charging_events.merge(
        charging_locations, left_on="assigned_location", right_on="index"
    )

    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_public, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print("distribution of public-charging-points successful")


def home(home_data: gpd.GeoDataFrame, uc_dict, mode):
    # todo: add probability for charging infrastructure at home. Select homes that are not possible to be electrified

    # uc_id = "home"
    # print("Use case: " + uc_id)

    if mode == "apartment":
        uc_id = "home_apartment"
        print("Use case: " + uc_id)
        charging_events = (
            uc_dict["charging_event"]
            .loc[
                uc_dict["charging_event"]["charging_use_case"].isin(["home_apartment"])
            ]
            .reset_index()
        )
    elif mode == "detached":
        uc_id = "home_detached"
        print("Use case: " + uc_id)
        charging_events = (
            uc_dict["charging_event"]
            .loc[uc_dict["charging_event"]["charging_use_case"].isin(["home_detached"])]
            .reset_index()
        )
    else:
        print("wrong mode")

    # filter houses by region
    # in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    # in_region = home_data.loc[in_region_bool].copy()
    in_region = home_data
    # in_region = in_region.iloc[:800]
    (
        charging_locations_home,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region,
        charging_events,
        weight_column="households_total",
        simulation_steps=2000,
    )

    # Merge Chargin_events and Locations
    charging_locations_home["index"] = charging_locations_home.index
    located_charging_events = located_charging_events.merge(
        charging_locations_home, left_on="assigned_location", right_on="index"
    )
    # drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    # located_charging_events = located_charging_events.drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_home, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print("distribution of home-charging-points successful")


def work(work_data, uc_dict, timestep=15):
    print("distributing uc work...")
    uc_id = "work"
    print("Use case: " + uc_id)

    charging_events = (
        uc_dict["charging_event"]
        .loc[uc_dict["charging_event"]["charging_use_case"].isin(["work"])]
        .reset_index()
    )

    # filter houses by region
    # in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    # in_region = home_data.loc[in_region_bool].copy()
    in_region = work_data
    # in_region = in_region.iloc[:800]
    (
        charging_locations_work,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="area", simulation_steps=2000
    )

    # Merge Chargin_events and Locations
    charging_locations_work["index"] = charging_locations_work.index
    located_charging_events = located_charging_events.merge(
        charging_locations_work, left_on="assigned_location", right_on="index"
    )
    # drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    located_charging_events = located_charging_events  # .drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_work, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print("distribution of work-charging-points successful")


def retail(retail_data: gpd.GeoDataFrame, uc_dict):
    """
    Calculate placements and energy distribution for use case hpc.

    :param retail_data: gpd.GeoDataFrame
        info about house types
    :param uc_dict: dict
        contains basic run info like region boundary and save directory

    """
    uc_id = "retail"
    print("Use case: " + uc_id)

    charging_events_retail_slow = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["retail"])
    ]

    charging_events_retail_hpc = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["urban_fast"])
        & uc_dict["charging_event"]["location"].isin(["shopping"])
    ]

    charging_events = pd.concat(
        [charging_events_retail_slow, charging_events_retail_hpc],
        axis=0,
        ignore_index=True,
    ).reset_index()

    # filter houses by region
    # in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    # in_region = home_data.loc[in_region_bool].copy()
    cols = [
        "id_0",
        "osm_way_id",
        "amenity",
        "other_tags",
        "id",
        "area",
        "category",
        "geometry",
    ]
    in_region = retail_data[cols]
    in_region = in_region.loc[in_region["area"] > 100]
    (
        charging_locations_retail,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="area", simulation_steps=2000
    )

    # Merge Chargin_events and Locations
    charging_locations_retail["index"] = charging_locations_retail.index
    located_charging_events = located_charging_events.merge(
        charging_locations_retail, left_on="assigned_location", right_on="index"
    )
    # drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    located_charging_events = located_charging_events  # .drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_retail, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print("distribution of work-charging-points successful")


def depot(depot_data: gpd.GeoDataFrame, uc_dict):
    uc_id = "depot"
    print("Use case: " + uc_id)
    charging_events_depot = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["depot"])
    ]

    charging_events = charging_events_depot.reset_index()

    in_region = depot_data
    in_region = in_region.loc[in_region["area_ha"] > 0.1]
    (
        charging_locations_depot,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="area_ha", simulation_steps=2000
    )

    # Merge Chargin_events and Locations
    charging_locations_depot["index"] = charging_locations_depot.index
    located_charging_events = located_charging_events.merge(
        charging_locations_depot, left_on="assigned_location", right_on="index"
    )

    located_charging_events = located_charging_events
    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    cols = ["geometry", "charge_spots", "energy"]
    utility.save(charging_locations_depot, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print("distribution of depot-charging-points successful")
