# import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import numpy as np
import math
import use_case_helpers as uc_helpers
import heapq

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
            in_region, charging_events, weight_column="gewicht", simulation_steps=2000,
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

        located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf["assigned_location"].astype(int))
        charging_locations_hpc["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_hpc.index).astype(int))

        charging_locations = charging_locations_hpc[uc_dict["columns_output_locations"]]
        located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

        charging_locations = charging_locations[charging_locations["charging_points"] != 0]

        utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
        utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

        print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
        print("distribution of hpc-charging-points successful")
        return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
                int((charging_locations["average_charging_capacity"] * charging_locations["charging_points"]).sum()))
    else:
        print("No hpc charging in timeseries")
        return 0, 0, 0


def public(
    public_data_not_home_street: gpd.GeoDataFrame,
    public_data_home_street: gpd.GeoDataFrame,
    uc_dict,
    charging_locations_public_after_multi_use: pd.DataFrame = None,
):
    uc_id = "public"
    print("Use case: " + uc_id)
    charging_events_public = (
        uc_dict["charging_event"]
        .loc[uc_dict["charging_event"]["charging_use_case"] == "street"]
        .reset_index()
    )

    if uc_dict["multi_use_concept"]:
        print("multi-use-consepts activated")

        if uc_dict["multi_use_group"] == ['Private', 'Commercial']:
            charging_events = charging_locations_public_after_multi_use
        else:
            charging_events_commerical = charging_locations_public_after_multi_use.reset_index(drop=True)

            charging_events_private = uc_dict["charging_event"].loc[
                uc_dict["charging_event"]["charging_use_case"].isin(["street"]) & uc_dict["charging_event"][
                    "Type"].isin(
                    ["Private"])
                ]

            charging_events = pd.concat([charging_events_private, charging_events_commerical], ignore_index=True)
    else:
        charging_events = charging_events_public.reset_index()

    if uc_dict["additional_public_input"]:

        # Inputs aus deinem Beispiel
        charging_locations = uc_dict['additional_public_locations']
        located_charging_events = uc_dict['additional_public_events']

        located_charging_events_gdf = located_charging_events[
            located_charging_events["event_id"].isin(charging_events["event_id"])
        ]

        events = located_charging_events_gdf.copy()
        locs = charging_locations.copy()

        # --- Typen & Endzeit (diskrete Zeit, [start, end) ) ---
        events["event_start"] = pd.to_numeric(events["event_start"], errors="coerce").astype("Int64")
        events["event_time"] = pd.to_numeric(events["event_time"], errors="coerce").astype("Int64")
        events = events.dropna(subset=["location_id", "event_start", "event_time"]).copy()
        events["event_end"] = events["event_start"] + events["event_time"]

        # --- Kapazitäten je Standort (harte Obergrenze) ---
        cap = (
            locs.set_index("location_id")["charging_points"]
            .fillna(0).astype(int)
            .to_dict()
        )
        all_locations = list(locs["location_id"].unique())

        # Aktuelle Belegung und Peaks
        load = {lid: 0 for lid in all_locations}
        max_load = {lid: 0 for lid in all_locations}

        # Verfügbarkeits-Heap: nur Locations mit cap>0 kommen rein
        # (Eintrag: (current_load, location_id); lazy updates bei Änderungen)
        availQ = [(0, lid) for lid in all_locations if cap.get(lid, 0) > 0]
        heapq.heapify(availQ)

        # Globale Endzeiten-Queue: (end_time, location_id)
        endQ = []

        # Events sortiert nach Start (bei Gleichstand längere zuerst ist optional)
        ev_idx = events.sort_values(by=["event_start", "event_time"], ascending=[True, False]).index

        # Ergebnis-Spalten
        events["assigned_location_id"] = pd.Series(pd.NA, index=events.index, dtype="Int64")
        events["was_reassigned"] = False
        events["unserved"] = False

        def release_finished(until_time: int):
            """Alle Enden <= until_time verarbeiten (Last senken, availQ updaten)."""
            while endQ and endQ[0][0] <= until_time:
                end_t, lid = heapq.heappop(endQ)
                load[lid] -= 1
                # Falls nach dem Freigeben noch Kapazität frei: neuen (akt. load, lid) eintragen
                if load[lid] < cap.get(lid, 0):
                    heapq.heappush(availQ, (load[lid], lid))

        for idx in ev_idx:
            start = int(events.at[idx, "event_start"])
            end = int(events.at[idx, "event_end"])
            orig = events.at[idx, "location_id"]

            # 1) Erst beendete Ladevorgänge freigeben
            release_finished(start)

            chosen = None

            # 2) Bevorzugt: Originalstandort, falls dort freie Kapazität
            if cap.get(orig, 0) > 0 and load[orig] < cap[orig]:
                chosen = orig
            else:
                # 3) Sonst: Standort mit minimaler aktueller Belegung aus availQ (lazy updates)
                while availQ:
                    cand_load, cand = heapq.heappop(availQ)
                    # veraltete Einträge überspringen
                    if cand_load != load[cand]:
                        continue
                    # nur nehmen, wenn noch Kapazität frei
                    if load[cand] < cap.get(cand, 0):
                        chosen = cand
                        break
                # wenn keiner frei ist -> unserved

            if chosen is None:
                events.at[idx, "unserved"] = True
                continue

            # 4) Zuweisen
            load[chosen] += 1
            max_load[chosen] = max(max_load[chosen], load[chosen])
            heapq.heappush(endQ, (end, chosen))

            # Standort bleibt ggf. weiter verfügbar -> neuen Zustand in availQ schreiben
            if load[chosen] < cap.get(chosen, 0):
                heapq.heappush(availQ, (load[chosen], chosen))

            events.at[idx, "assigned_location_id"] = chosen
            events.at[idx, "was_reassigned"] = (chosen != orig)

        # 5) Neue charging_points = beobachteter Peak je Standort
        #    (entspricht der minimal nötigen Anzahl simultaner Punkte unter Kapazitätszwang)
        peak_series = pd.Series(max_load, name="charging_points")
        locs = locs.copy()
        locs["charging_points"] = locs["location_id"].map(peak_series).fillna(0).astype(int)

        # 6) Überschreiben der location_id nur, wenn zugewiesen (nicht für unserved)
        mask_assigned = events["assigned_location_id"].notna()
        events.loc[mask_assigned, "location_id"] = events.loc[mask_assigned, "assigned_location_id"].astype(
            events["location_id"].dtype)

        # Outputs wie gehabt
        charging_locations = locs[locs["charging_points"] > 0].reset_index(drop=True)
        located_charging_events_gdf = events


    else:

        charging_events_home_street = charging_events.loc[
            charging_events["location"] == "home"
        ].reset_index(drop=True)
        charging_events_not_home_street = charging_events.loc[
            charging_events["location"] != "home"
        ].reset_index(drop=True)
        in_region_home_street = public_data_home_street

        in_region_not_home_street = public_data_not_home_street

        if in_region_home_street is not None:

            in_region_home_street = in_region_home_street.rename(columns={'households_total': 'Weight'})
            in_region_not_home_street = in_region_not_home_street.rename(columns={'@id': 'id'})
            if uc_dict["additional_public_input"]:
                in_region_home_street = in_region_home_street[["Weight", "charging_points", "average_charging_capacity", "geometry"]]
                in_region_not_home_street = in_region_not_home_street[["Weight", "charging_points", "average_charging_capacity", "geometry"]]
            else:
                in_region_home_street = in_region_home_street[["Weight", "geometry"]]
                if "Category_Weight" in in_region_not_home_street.columns:
                    in_region_not_home_street = in_region_not_home_street.rename(columns={'Category_Weight': 'Weight'})
                in_region_not_home_street = in_region_not_home_street[["Weight", "geometry"]]
            in_region_home_street["mode"] = "home_street"
            in_region_not_home_street["mode"] = "not_home_street"



            in_region = pd.concat([in_region_home_street, in_region_not_home_street], ignore_index=True)

        else:
            in_region = in_region_not_home_street

        fill_existing_only = bool(uc_dict["additional_public_input"])

        (
            charging_locations_public_home,
            located_charging_events_public_home,
        ) = uc_helpers.distribute_charging_events(
            in_region[in_region["mode"] == "home_street"],
            charging_events_home_street,
            weight_column="Weight",
            simulation_steps=2000,
            rng=uc_dict["random_seed"],
            # fill_existing_only=fill_existing_only,
            fill_existing_first=True,
            additional_street_input=bool(uc_dict["additional_public_input"])
        )
        charging_locations_public_home["mode"] = "home_street"
        located_charging_events_public_home["mode"] = "home_street"

        (
            charging_locations_public,
            located_charging_events_public,
        ) = uc_helpers.distribute_charging_events(
            in_region[in_region["mode"] == "not_home_street"],
            charging_events_not_home_street,
            weight_column="Weight",
            simulation_steps=2000,
            rng=uc_dict["random_seed"],
            #fill_existing_only=fill_existing_only,
            fill_existing_first=True,
            additional_street_input=bool(uc_dict["additional_public_input"])
            #home_street=uc_dict["run_home"]
        )

        charging_locations_public["mode"] = "street"
        located_charging_events_public["mode"] = "street"

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

        # charging_locations = charging_locations_public
        # located_charging_events = located_charging_events_public

        # Merge Chargin_events and Locations
        charging_locations["index"] = charging_locations.index
        located_charging_events = located_charging_events.merge(
            charging_locations, left_on="assigned_location", right_on="index"
        )

        located_charging_events_gdf = gpd.GeoDataFrame(
            located_charging_events, geometry="geometry"
        )
        located_charging_events_gdf.to_crs(3035)

        # generate_ids and reduce columns
        located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
            "assigned_location"].astype(int))
        charging_locations["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations.index).astype(int))

        columns_locations = uc_dict["columns_output_locations"].copy()
        columns_locations.append("mode")

        columns_events = uc_dict["columns_output_chargingevents"].copy()
        columns_events.append("mode")

        charging_locations = charging_locations[columns_locations]
        located_charging_events_gdf = located_charging_events_gdf.rename(columns={"mode_x": "mode"})
        located_charging_events_gdf = located_charging_events_gdf[columns_events]

        charging_locations = charging_locations[charging_locations["charging_points"] != 0]

        postprocessing = True
        if postprocessing:
            charging_locations, located_charging_events = uc_helpers.postprocess_public_demands(charging_locations,
            located_charging_events_gdf)

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of public-charging-points successful")
    return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

def home(home_data: gpd.GeoDataFrame, uc_dict, mode):
    # todo: add probability for charging infrastructure at home. Select homes that are not possible to be electrified
    # uc_id = "home"
    # print("Use case: " + uc_id)

    in_region = home_data

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
        # charging_events = charging_events.iloc[:500]
        (
            charging_locations_home,
            located_charging_events,
        ) = uc_helpers.distribute_charging_events(
            in_region,
            charging_events,
            weight_column="households_total",
            simulation_steps=2000, fill_existing_first=True,
            rng=uc_dict["random_seed"]
        )

    elif mode == "detached":
        uc_id = "home_detached"
        print("Use case: " + uc_id)
        charging_events = (
            uc_dict["charging_event"]
            .loc[uc_dict["charging_event"]["charging_use_case"].isin(["home_detached"])]
            .reset_index()
        )
        # charging_events = charging_events.iloc[:500]
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

    else:
        print("wrong mode")

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
    # located_charging_events_gdf.set_crs(3035)

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_home["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_home.index).astype(int))

    charging_locations = charging_locations_home[uc_dict["columns_output_locations"]]

    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of home-charging-points successful")
    return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

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

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of work-charging-points successful")
    return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

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

    if any(col not in retail_data.columns for col in cols):

        retail_data = retail_data.rename(columns={"nid": "id_0",
                                    'osm_id': "osm_way_id",
                                    # "amenity": "amenity",
                                    "building": "other_tags",
                                    # 'area': "area",
                                    "access": "category",
                                    # 'geometry': "geometry"
                                    })
        retail_data["id"] = retail_data["id_0"]

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
        charging_events_street = uc_dict["charging_event"].loc[
            uc_dict["charging_event"]["charging_use_case"].isin(["street"]) & uc_dict["charging_event"]["Type"].isin(uc_dict["multi_use_group"])
        ]
        charging_events_public = charging_events_street.reset_index()

        # Verteilung der Street-Ladeevents auf Retail-Standorte
        charging_locations_retail_after_multi_use, located_public_events = uc_helpers.distribute_charging_events(
            charging_locations_retail, charging_events_public, weight_column="area", simulation_steps=2000,
            rng=uc_dict["random_seed"], fill_existing_only=True, availability_mask=availability_mask,
            flexibility_multi_use=uc_dict["flexibility_multi_use"]
        ) # charging_events_depot austauschen gegen depot_night_events

        located_public_events_assigned = located_public_events[located_public_events["assigned_location"].notna()]

        located_public_events_assigned["multi_use"] = True

        located_charging_events["multi_use"] = False

        # Kombiniere Retail- und Depot-Ladeevents
        located_charging_events = pd.concat([located_charging_events, located_public_events_assigned], ignore_index=True)

        charging_events_public_no_multi_use_possible = located_public_events[located_public_events["assigned_location"].isna()].reset_index()

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
    if uc_dict["multi_use_concept"]:
        keys = uc_dict["columns_output_chargingevents"] + ["multi_use"]
        located_charging_events_gdf = located_charging_events_gdf[keys]
    else:
        located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    # todo checken o alle ids stimmen (bei multi-use-szenario)

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    # utility.plot_occupation_of_charging_points(located_charging_events, uc_id)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of work-charging-points successful")

    if uc_dict["multi_use_concept"]:
        return (int(charging_locations["charging_points"].sum()), int(located_charging_events["energy"].sum()),
                int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()),
                charging_events_public_no_multi_use_possible)
    else:
        return (int(charging_locations["charging_points"].sum()), int(located_charging_events["energy"].sum()),
                int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

def depot(depot_data: gpd.GeoDataFrame, uc_dict):
    uc_id = "depot"
    print("Use case: " + uc_id)
    charging_events_depot = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"].isin(["depot"])
    ]

    charging_events = charging_events_depot.reset_index()

    in_region = depot_data

    if "area" not in in_region.columns:
        in_region  = in_region.rename(columns={"Area[m2]": "area"})

    in_region = in_region.loc[in_region["area"] > 1]
    (
        charging_locations_depot,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="area", simulation_steps=2000,
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
    located_charging_events_gdf.set_crs(3035, allow_override=True)

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_depot["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_depot.index).astype(int))

    charging_locations = charging_locations_depot[uc_dict["columns_output_locations"]]
    located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    print(uc_id, "Anzahl der Ladepunkte: ", charging_locations["charging_points"].sum())
    print("distribution of depot-charging-points successful")
    return (int(charging_locations["charging_points"].sum()), int(located_charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))
