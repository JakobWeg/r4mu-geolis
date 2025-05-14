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
            in_region, charging_events, weight_column="count", simulation_steps=2000,
            rng=uc_dict["random_seed"])

        # Merge Chargin_events and Locations
        charging_locations_hpc["index"] = charging_locations_hpc.index
        located_charging_events = located_charging_events.merge(
            charging_locations_hpc, left_on="assigned_location", right_on="index"
        )

        located_charging_events_gdf = gpd.GeoDataFrame(
            located_charging_events, geometry="geometry"
        )
        located_charging_events_gdf.to_crs(3035)

        # charging_locations_hpc.rename(columns={
        #                                     'old_column1': 'new_column1',
        #                                     'old_column2': 'new_column2'
        #                                 })
        # located_charging_events_gdf = located_charging_events_gdf.rename(columns={
        #                                                                         'old_column1': 'new_column1',
        #                                                                         'old_column2': 'new_column2'
        #                                                                     })

        located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf["assigned_location"].astype(int))
        charging_locations_hpc["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_hpc.index).astype(int))

        charging_locations = charging_locations_hpc[uc_dict["columns_output_locations"]]
        located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

        utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
        utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

        print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
        print("distribution of hpc-charging-points successful")
        return (charging_locations["charging_points"].sum(), int(charging_events["energy"].sum()),
                (charging_locations["average_charging_capacity"] * charging_locations["charging_points"]).sum())
    else:
        print("No hpc charging in timeseries")
        return 0, 0, 0


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
    # charging_events_home_street = charging_events.loc[
    #     charging_events["location"] == "home"
    # ].reset_index()
    # charging_events_not_home_street = charging_events.loc[
    #     charging_events["location"] != "home"
    # ].reset_index()
    # in_region_home_street = public_data_home_street
    in_region_not_home_street = public_data_not_home_street


    # (
    #     charging_locations_public_home,
    #     located_charging_events_public_home,
    # ) = uc_helpers.distribute_charging_events(
    #     in_region_home_street,
    #     charging_events_home_street,
    #     weight_column="potential",
    #     simulation_steps=1000,
    # )

    (
        charging_locations_public,
        located_charging_events_public,
    ) = uc_helpers.distribute_charging_events(
        in_region_not_home_street,
        charging_events,
        weight_column="potential",
        simulation_steps=1000,
        rng=uc_dict["random_seed"]
    )

    # todo: datendatz für public home anpassen

    # located_charging_events_public_home[
    #     "assigned_location"
    # ] = located_charging_events_public_home["assigned_location"] + len(
    #     charging_locations_public
    # )

    # concat charging events and location at home and public
    # charging_locations = pd.concat(
    #     [charging_locations_public, charging_locations_public_home], ignore_index=True
    # )
    # located_charging_events = pd.concat(
    #     [located_charging_events_public, located_charging_events_public_home],
    #     ignore_index=True,
    # )
    charging_locations = charging_locations_public
    located_charging_events = located_charging_events_public

    # Merge Chargin_events and Locations
    charging_locations["index"] = charging_locations.index
    located_charging_events = located_charging_events.merge(
        charging_locations, left_on="assigned_location", right_on="index"
    )

    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations.index).astype(int))

    charging_locations = charging_locations[uc_dict["columns_output_locations"]]
    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]


    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of public-charging-points successful")
    return (charging_locations["charging_points"].sum(), int(charging_events["energy"].sum()),
            (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum())


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
        simulation_steps=2000, fill_existing_first=False,
        rng=uc_dict["random_seed"]
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

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_home["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_home.index).astype(int))

    charging_locations = charging_locations_home[uc_dict["columns_output_locations"]]

    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of home-charging-points successful")
    return (charging_locations["charging_points"].sum(), int(charging_events["energy"].sum()),
            (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum())

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
        in_region, charging_events, weight_column="area", simulation_steps=2000,
        rng=uc_dict["random_seed"]
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

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_work["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_work.index).astype(int))

    charging_locations = charging_locations_work[uc_dict["columns_output_locations"]]
    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of work-charging-points successful")
    return (charging_locations["charging_points"].sum(), int(charging_events["energy"].sum()),
            (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum())

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
        availability_mask,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="area", simulation_steps=2000,
        rng=uc_dict["random_seed"], return_mask=True
    )


    if uc_dict["multi_use_concept"]:
        print("multi-use-concept activated")

        # Depot Ladeevents in den Nachtstunden (Mo-Sa zwischen 21:00 und 8:00 Uhr)
        charging_events_depot = uc_dict["charging_event"].loc[
            uc_dict["charging_event"]["charging_use_case"].isin(["depot"])
        ]
        charging_events_depot = charging_events_depot.reset_index()

        # Definiere den Zeitrahmen für Mo-Sa zwischen 21:00 und 8:00 Uhr über fortlaufende Zeitschritte
        # Jeder Tag hat 96 Zeitschritte, daher ist der Zeitrahmen für 21:00 bis 8:00 Uhr folgender:
        night_time_start = 80  # 21:00 Uhr (Zeitschritt 84)
        night_time_end = 35  # 8:00 Uhr des nächsten Tages (Zeitschritt 31)

        depot_night_events = []

        # Überprüfe für jedes Ladeevent, ob es im Zeitrahmen zwischen 21:00 und 8:00 Uhr liegt
        for index, event in charging_events_depot.iterrows():
            start = event['event_start']
            dauer = event['event_time']
            ende = start + dauer

            # Berechne den Tag (von 0 bis 6, Montag bis Sonntag) und den Zeitschritt innerhalb des Tages
            day_of_week = start // 96  # Tag des Events (0=Montag, 6=Sonntag)
            time_of_day = start % 96  # Zeitschritt innerhalb des Tages (0-95)
            end_of_event = ende % 96
            end_of_event_week = ende // 96
            # Prüfe, ob das Ladeevent im Zeitraum zwischen 21:00 Uhr und 8:00 Uhr liegt
            if ((time_of_day >= night_time_start) or (time_of_day < night_time_end)) and ((end_of_event <= night_time_end)
                    and (end_of_event_week <= (day_of_week+2))):
                depot_night_events.append(event)

        # Wir haben nun alle Depot-Ladeevents im gewünschten Zeitraum gesammelt
        depot_night_events = pd.DataFrame(depot_night_events)

        # Verteilung der Depot-Ladeevents auf Retail-Standorte
        # todo abklären, ob wir hier alle depot ladeevents drinnen haben wollen zum auffüllen oder nur die über nacht
        charging_locations_retail_after_multi_use, located_depot_events = uc_helpers.distribute_charging_events(
            charging_locations_retail, charging_events_depot, weight_column="area", simulation_steps=2000,
            rng=uc_dict["random_seed"], fill_existing_only=True, availability_mask=availability_mask
        ) # charging_events_depot austauschen gegen depot_night_events

        # Kombiniere Retail- und Depot-Ladeevents
        located_charging_events = pd.concat([located_charging_events, located_depot_events], ignore_index=True)

        charging_events_depot_no_multi_use_possible = located_depot_events[located_depot_events["assigned_location"].isna()].reset_index()

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

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_retail["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_retail.index).astype(int))

    charging_locations = charging_locations_retail[uc_dict["columns_output_locations"]]
    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    # todo checken o alle ids stimmen (bei multi-use-szenario)
    # todo hier alle charging locations mit nr. ladepunkte gleich null raus nehmen

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of work-charging-points successful")

    if uc_dict["multi_use_concept"]:
        return (charging_locations["charging_points"].sum(), int(located_charging_events["energy"].sum()),
                (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum(),
                charging_events_depot_no_multi_use_possible)
    else:
        return (charging_locations["charging_points"].sum(), int(located_charging_events["energy"].sum()),
                (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum())

def depot(depot_data: gpd.GeoDataFrame, uc_dict, charging_locations_depot_after_multi_use: pd.DataFrame = None):
    uc_id = "depot"
    print("Use case: " + uc_id)
    charging_events_depot = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["depot"])
    ]

    if uc_dict["multi_use_concept"]:
        print("multi-use-consepts activated")
        charging_events = charging_locations_depot_after_multi_use
    else:
        charging_events = charging_events_depot.reset_index()

    in_region = depot_data
    in_region = in_region.loc[in_region["area_ha"] > 0.1]
    (
        charging_locations_depot,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="area_ha", simulation_steps=2000,
        rng=uc_dict["random_seed"]
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

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_depot["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_depot.index).astype(int))

    charging_locations = charging_locations_depot[uc_dict["columns_output_locations"]]
    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of depot-charging-points successful")
    return (charging_locations["charging_points"].sum(), int(charging_events["energy"].sum()),
            (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum())

def retail_multi_use(retail_data: gpd.GeoDataFrame, uc_dict):
    """
    Berechnet zuerst die Retail-Ladeevents und ordnet dann Depot-Ladeevents in der Nacht (Mo-Sa zwischen 21:00 und 8:00 Uhr)
    den Retail-Standorten zu, ohne das Maximum an Ladepunkten zu überschreiten.

    :param retail_data: gpd.GeoDataFrame
        Info über die Retail-Standorte
    :param uc_dict: dict
        Enthält grundlegende Informationen über das Szenario, z.B. Ladeereignisse, Region, etc.
    """
    # todo: delete

    uc_id = "retail_depot"
    print("Use case: " + uc_id)

    # Schritt 1: Retail Ladeevents berechnen
    charging_events_retail_slow = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["retail"])
    ]

    charging_events_retail_hpc = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["urban_fast"])
        & uc_dict["charging_event"]["location"].isin(["shopping"])
    ]

    charging_events_retail = pd.concat(
        [charging_events_retail_slow, charging_events_retail_hpc],
        axis=0,
        ignore_index=True,
    ).reset_index()

    # Retail Locations (Verteilung der Events auf die Standorte)
    cols = ["id_0", "osm_way_id", "amenity", "other_tags", "id", "area", "category", "geometry"]
    in_region = retail_data[cols]
    in_region = in_region.loc[in_region["area"] > 100]

    # Verteilung der Ladeevents auf die Retail-Standorte
    charging_locations_retail, located_charging_events = uc_helpers.distribute_charging_events(
        in_region, charging_events_retail, weight_column="area", simulation_steps=2000,
        rng=uc_dict["random_seed"]
    )

    # Maximale Anzahl an Ladepunkten für Retail-Standorte berechnen
    max_retail_charging_points = charging_locations_retail["charging_points"].sum()

    # Schritt 2: Depot Ladeevents in den Nachtstunden (Mo-Sa zwischen 21:00 und 8:00 Uhr)
    charging_events_depot = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["depot"])
    ]
    charging_events_depot = charging_events_depot.reset_index()

    # Erstelle eine Zeitmaske für Mo-Sa zwischen 21:00 und 8:00 Uhr
    charging_events_depot["timestamp"] = pd.to_datetime(charging_events_depot["timestamp"])
    is_night_time = (
        (charging_events_depot["timestamp"].dt.hour >= 21) |
        (charging_events_depot["timestamp"].dt.hour < 8)
    )

    depot_night_events = charging_events_depot[is_night_time]

    # Verteile Depot-Ladeevents auf Retail-Standorte, ohne das Maximum an Ladepunkten zu überschreiten
    remaining_retail_charging_points = max_retail_charging_points - charging_locations_retail["charging_points"].sum()

    # Nur Ladeevents, wenn noch Ladepunkte verfügbar sind
    depot_night_events = depot_night_events.head(remaining_retail_charging_points)

    # Verteilung der Depot-Ladeevents auf Retail-Standorte
    charging_locations_retail, located_depot_night_events = uc_helpers.distribute_charging_events(
        in_region, depot_night_events, weight_column="area", simulation_steps=2000,
        rng=uc_dict["random_seed"]
    )

    # Kombiniere Retail- und Depot-Ladeevents
    located_charging_events = pd.concat([located_charging_events, located_depot_night_events], ignore_index=True)

    # Merge Charging Locations und Events
    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)

    # Generiere IDs und reduziere die Spalten
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf["assigned_location"].astype(int))
    charging_locations_retail["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_retail.index).astype(int))

    charging_locations = charging_locations_retail[uc_dict["columns_output_locations"]]
    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    # Speicher die Daten
    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of retail and depot-charging-points successful")

    return (charging_locations["charging_points"].sum(), int(charging_events_retail["energy"].sum()) + int(depot_night_events["energy"].sum()),
            (charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum())