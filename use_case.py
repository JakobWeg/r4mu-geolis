# import plots as plots
import utility as utility
import pandas as pd
import geopandas as gpd
import numpy as np
import math
import use_case_helpers as uc_helpers
import heapq

# fester Marker für den eigenen Zufallsstrom der Retail-Verortung (siehe retail())
RETAIL_RNG_STREAM = 1

def hpc(hpc_data: gpd.GeoDataFrame, uc_dict, timestep=15, uc_id="hpc", weight_column="gewicht",
        charging_use_case="urban_fast", exclude_shopping=True, simulation_steps=2000,
        existing_points_column=None, existing_capacity_column=None):
    """
    Calculate placements and energy distribution for use case hpc.

    :param hpc_points: gpd.GeoDataFrame
        GeoDataFrame of possible hpc locations
    :param uc_dict: dict
        contains basic run info like region boundary and save directory
    :param timestep: int
        time step of the simbev input series, default: 15 (minutes)
    :param uc_id: str
        use case id used for output filenames and location ids; must be a key
        in use_case_helpers.get_id's use_case_map
    :param weight_column: str
        column in hpc_data used as sampling weight (Berlin: pre-computed
        "gewicht" from utility.calculate_hpc_locations; data_DE candidates
        already ship a "weight" column)
    :param charging_use_case: str
        value of charging_use_case to place ("urban_fast" or "highway_fast"
        for the DE urban/highway HPC split)
    :param exclude_shopping: bool
        exclude events parked at "shopping" locations (handled by retail()
        instead); only relevant for the urban_fast case
    """

    # == single value, not .isin([single value]) - equivalent result, but
    # .isin() against an Arrow-backed string column (pandas 3.x default)
    # has no fast hash-table path for a single-element list, while == does a
    # plain vectorized comparison - a real cost difference on this table
    # (uc_dict["charging_event"], up to a megacity's full per-Gemeinde event
    # count), same class of fix as documented at length in
    # restructure_output.py's process_gemeinde().
    mask = uc_dict["charging_event"]["charging_use_case"] == charging_use_case
    if exclude_shopping:
        mask &= uc_dict["charging_event"]["location"] != "shopping"
    charging_events = uc_dict["charging_event"].loc[mask].reset_index()

    in_region = hpc_data
    # in_region = in_region.iloc[:800]

    num_hpc = charging_events.loc[
        charging_events["charging_use_case"] == charging_use_case
    ].shape[0]

    if num_hpc > 0:
        (
            charging_locations_hpc,
            located_charging_events,
        ) = uc_helpers.distribute_charging_events(
            in_region, charging_events, weight_column=weight_column, simulation_steps=simulation_steps,
            rng=uc_dict["random_seed"], existing_points_column=existing_points_column,
            existing_capacity_column=existing_capacity_column)

        # Merge Chargin_events and Locations
        charging_locations_hpc["index"] = charging_locations_hpc.index
        _n_events_before_merge = len(located_charging_events)
        located_charging_events = located_charging_events.merge(
            charging_locations_hpc, left_on="assigned_location", right_on="index"
        )
        uc_helpers.check_event_loss(uc_id, _n_events_before_merge, len(located_charging_events), uc_dict)

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

        return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
                int((charging_locations["average_charging_capacity"] * charging_locations["charging_points"]).sum()))
    else:
        return 0, 0, 0


def public(
    public_data_not_home_street: gpd.GeoDataFrame,
    public_data_home_street: gpd.GeoDataFrame,
    uc_dict,
    charging_locations_public_after_multi_use: pd.DataFrame = None,
    simulation_steps=2000,
    existing_points_column=None,
    existing_capacity_column=None,
):
    uc_id = "public"
    charging_events_public = (
        uc_dict["charging_event"]
        .loc[uc_dict["charging_event"]["charging_use_case"] == "street"]
        .reset_index()
    )

    if uc_dict["multi_use_concept"]:

        if uc_dict["multi_use_group"] == ['Private', 'Commercial']:
            charging_events = charging_locations_public_after_multi_use
        else:
            charging_events_commerical = charging_locations_public_after_multi_use.reset_index(drop=True)

            charging_events_private = uc_dict["charging_event"].loc[
                (uc_dict["charging_event"]["charging_use_case"] == "street")
                & (uc_dict["charging_event"]["Type"] == "Private")
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

        # Max-Heap für Konsolidierung: Standort mit höchster aktueller Last bevorzugen.
        # Eintrag: (-current_load, location_id); lazy updates.
        # Dadurch werden Events auf möglichst wenige Standorte konzentriert,
        # sodass ungenutzte Standorte (max_load=0) wegfallen.
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
                # Falls nach dem Freigeben noch Kapazität frei: neuen (-load, lid) eintragen
                if load[lid] < cap.get(lid, 0):
                    heapq.heappush(availQ, (-load[lid], lid))

        for idx in ev_idx:
            start = int(events.at[idx, "event_start"])
            end = int(events.at[idx, "event_end"])
            orig = events.at[idx, "location_id"]

            # 1) Erst beendete Ladevorgänge freigeben
            release_finished(start)

            chosen = None

            # 2) Standort mit höchster aktueller Last und freier Kapazität (Max-Heap, lazy updates)
            while availQ:
                neg_cand_load, cand = heapq.heappop(availQ)
                # veraltete Einträge überspringen
                if neg_cand_load != -load[cand]:
                    continue
                # nur nehmen, wenn noch Kapazität frei
                if load[cand] < cap.get(cand, 0):
                    chosen = cand
                    break
            # wenn keiner frei ist -> unserved

            if chosen is None:
                events.at[idx, "unserved"] = True
                continue

            # 3) Zuweisen
            load[chosen] += 1
            max_load[chosen] = max(max_load[chosen], load[chosen])
            heapq.heappush(endQ, (end, chosen))

            # Standort bleibt ggf. weiter verfügbar -> neuen Zustand in availQ schreiben
            if load[chosen] < cap.get(chosen, 0):
                heapq.heappush(availQ, (-load[chosen], chosen))

            events.at[idx, "assigned_location_id"] = chosen
            events.at[idx, "was_reassigned"] = (chosen != orig)

        # 5) Neue charging_points = beobachteter Peak je Standort
        #    (entspricht der minimal nötigen Anzahl simultaner Punkte unter Kapazitätszwang)
        peak_series = pd.Series(max_load, name="charging_points")
        locs = locs.copy()
        locs["charging_points"] = locs["location_id"].map(peak_series).fillna(0).astype(int)

        # 6) Überschreiben der location_id und Geometrie auf den zugewiesenen Standort (nicht für unserved)
        mask_assigned = events["assigned_location_id"].notna()
        events.loc[mask_assigned, "location_id"] = events.loc[mask_assigned, "assigned_location_id"].astype(
            events["location_id"].dtype)
        # Geometrie der Events auf die Koordinaten des zugewiesenen Standorts aktualisieren,
        # da nach dem Max-Heap-Reassignment location_id und Geometrie sonst auseinanderfallen.
        loc_geom_map = locs.set_index("location_id")["geometry"]
        new_geom = events.loc[mask_assigned, "location_id"].map(loc_geom_map)
        events.loc[mask_assigned, "geometry"] = new_geom.values

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

            in_region_not_home_street = in_region_not_home_street.rename(columns={'@id': 'id'})
            # carry existing-infrastructure and candidate-identity columns
            # through the fixed column-selections below if present (see
            # existing_infrastructure.py / data_de_candidates.py)
            extra_cols = [c for c in (existing_points_column, existing_capacity_column, "candidate_uid")
                          if c and c in in_region_not_home_street.columns]
            home_extra_cols = [c for c in ("candidate_uid",) if c in in_region_home_street.columns]
            if uc_dict["additional_public_input"]:
                in_region_home_street = in_region_home_street[["weight", "charging_points", "average_charging_capacity", "geometry"] + home_extra_cols]
                in_region_not_home_street = in_region_not_home_street[["weight", "charging_points", "average_charging_capacity", "geometry"] + extra_cols]
            else:
                home_seed_src = in_region_home_street
                in_region_home_street = in_region_home_street[["weight", "geometry"] + home_extra_cols].copy()
                # home_street's own seed: the previous scenario year's street
                # locations on these residential buildings (see location_registry.
                # seed_home_street_columns). NOT the layer's existing_points -
                # that is the apartment-wallbox seed of home_apartment().
                if existing_points_column and "street_existing_points" in home_seed_src.columns:
                    in_region_home_street[existing_points_column] = home_seed_src["street_existing_points"].to_numpy()
                if existing_capacity_column and "street_existing_capacity_kw" in home_seed_src.columns:
                    in_region_home_street[existing_capacity_column] = home_seed_src["street_existing_capacity_kw"].to_numpy()
                if "Category_Weight" in in_region_not_home_street.columns:
                    in_region_not_home_street = in_region_not_home_street.rename(columns={'Category_Weight': 'weight'})
                in_region_not_home_street = in_region_not_home_street[["weight", "geometry"] + extra_cols]
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
            weight_column="weight",
            simulation_steps=simulation_steps,
            rng=uc_dict["random_seed"],
            # fill_existing_only=fill_existing_only,
            fill_existing_first=True,
            additional_street_input=bool(uc_dict["additional_public_input"]),
            # home_street's candidates are the SAME home_apartment layer that
            # home_apartment() places onto, but they must NOT be seeded with
            # that layer's existing_points (the apartment-wallbox seed): the
            # two calls place independent event populations, so sharing the
            # seed would double-count. Instead, the candidate selection above
            # copies the layer's street_existing_points/_capacity_kw columns
            # (the previous scenario year's home_street locations on these
            # buildings, see location_registry.seed_home_street_columns)
            # into the existing_* columns this call reads. Before 2026-10-22
            # the column selection above dropped every existing_* column for
            # home_street (so this pass restarted at 0 every scenario year),
            # while the same buildings were re-appended to the public_poi pool
            # as orphaned candidates instead.
            existing_points_column=existing_points_column,
            existing_capacity_column=existing_capacity_column,
        )
        charging_locations_public_home["mode"] = "home_street"
        located_charging_events_public_home["mode"] = "home_street"

        (
            charging_locations_public,
            located_charging_events_public,
        ) = uc_helpers.distribute_charging_events(
            in_region[in_region["mode"] == "not_home_street"],
            charging_events_not_home_street,
            weight_column="weight",
            simulation_steps=simulation_steps,
            rng=uc_dict["random_seed"],
            #fill_existing_only=fill_existing_only,
            fill_existing_first=True,
            additional_street_input=bool(uc_dict["additional_public_input"]),
            #home_street=uc_dict["run_home"]
            existing_points_column=existing_points_column,
            existing_capacity_column=existing_capacity_column,
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
        _n_events_before_merge = len(located_charging_events)
        located_charging_events = located_charging_events.merge(
            charging_locations, left_on="assigned_location", right_on="index"
        )
        uc_helpers.check_event_loss("public", _n_events_before_merge, len(located_charging_events), uc_dict)

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

        # installed power the locations were seeded with (real BNetzA / previous-year capacity) - the
        # postprocessing below recomputes the power after moving events and must not fall below it
        if (existing_points_column and existing_capacity_column
                and existing_points_column in charging_locations.columns
                and existing_capacity_column in charging_locations.columns):
            charging_locations["seed_installed_kw"] = (charging_locations[existing_points_column].fillna(0)
                                                       * charging_locations[existing_capacity_column].fillna(0))
            columns_locations.append("seed_installed_kw")
        charging_locations = charging_locations[columns_locations]
        located_charging_events_gdf = located_charging_events_gdf.rename(columns={"mode_x": "mode"})
        located_charging_events_gdf = located_charging_events_gdf[columns_events]

        charging_locations = charging_locations[charging_locations["charging_points"] != 0]

        postprocessing = True
        if postprocessing:
            charging_locations, located_charging_events = uc_helpers.postprocess_public_demands(charging_locations,
            located_charging_events_gdf)
        charging_locations = charging_locations.drop(columns=["seed_installed_kw"], errors="ignore")

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

def home(home_data: gpd.GeoDataFrame, uc_dict, mode, simulation_steps=2000, vehicle_column=None,
         existing_points_column=None, existing_capacity_column=None, label=None,
         # Genuine reuse of an already-opened NEW (not pre-existing) location
         # is rare for home charging - a household doesn't share another
         # household's wallbox, so Phase 2 in distribute_charging_events_
         # per_vehicle almost never finds a fit here regardless of how many
         # candidates it checks. opened_list grows toward the vehicle count
         # itself for a big city's home charging (most vehicles open their
         # own dedicated point) - once it exceeds max_reuse_candidates, EVERY
         # vehicle pays the full capped-search cost for a match that almost
         # never comes. Confirmed on Berlin's 2037 home_apartment (226,072
         # vehicles): throughput roughly halved once opened_list grew past
         # a 20,000 cap that's better suited to work's genuinely higher
         # reuse rate below. A small cap here barely changes which vehicles
         # find a real match (there's so little to find) while bounding
         # this cost.
         max_reuse_candidates=200,
         # home_detached only: hard limit of charging points per one-/two-family-house address. Vehicles that find no
         # address below the limit are handed over via uc_dict["home_detached_overflow"] and placed by the
         # home_apartment() call that runs afterwards (run_de.py runs detached first when this is set).
         max_points_per_address=None):
    # todo: add probability for charging infrastructure at home. Select homes that are not possible to be electrified
    # uc_id = "home"
    # utility.safe_print("Use case: " + uc_id)

    in_region = home_data

    if mode == "apartment":
        uc_id = "home_apartment"
        charging_events = (
            uc_dict["charging_event"]
            .loc[
                uc_dict["charging_event"]["charging_use_case"] == "home_apartment"
            ]
            .reset_index()
        )
        # vehicles that did not fit onto a one-/two-family house (home_detached run before this call)
        overflow_in = uc_dict.get("home_detached_overflow")
        if overflow_in is not None and len(overflow_in):
            charging_events = pd.concat([charging_events, overflow_in], ignore_index=True)
        uc_dict["home_detached_overflow"] = None
        # charging_events = charging_events.iloc[:500]
        if vehicle_column:
            charging_locations_home, located_charging_events = uc_helpers.distribute_charging_events_per_vehicle(
                in_region, charging_events, weight_column="weight",
                simulation_steps=simulation_steps, vehicle_column=vehicle_column, rng=uc_dict["random_seed"],
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
                label=f"{label} {uc_id}" if label else uc_id, max_reuse_candidates=max_reuse_candidates,
            )
        else:
            (
                charging_locations_home,
                located_charging_events,
            ) = uc_helpers.distribute_charging_events(
                in_region,
                charging_events,
                weight_column="weight",
                simulation_steps=simulation_steps, fill_existing_first=True,
                rng=uc_dict["random_seed"],
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
            )

    elif mode == "detached":
        uc_id = "home_detached"
        charging_events = (
            uc_dict["charging_event"]
            .loc[uc_dict["charging_event"]["charging_use_case"] == "home_detached"]
            .reset_index()
        )
        # charging_events = charging_events.iloc[:500]
        overflow_energy = 0.0
        if vehicle_column and max_points_per_address:
            charging_locations_home, located_charging_events, overflow_events = uc_helpers.distribute_charging_events_household_capped(
                in_region, charging_events, weight_column="weight",
                vehicle_column=vehicle_column, household_column=None, rng=uc_dict["random_seed"],
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
                label=f"{label} {uc_id}" if label else uc_id,
                max_points_per_address=max_points_per_address, return_overflow=True,
            )
            uc_dict["home_detached_overflow"] = overflow_events
            overflow_energy = float(overflow_events["energy"].sum())
        elif vehicle_column:
            # Household-capped, not the time-window search used for
            # home_apartment - see distribute_charging_events_household_
            # capped's own docstring for why that's the right tradeoff for
            # detached houses specifically (at most 1-2 real households per
            # address, each with its own dedicated point - no genuine
            # shared-infrastructure timing question the way an apartment
            # building has).
            charging_locations_home, located_charging_events = uc_helpers.distribute_charging_events_household_capped(
                in_region, charging_events, weight_column="weight",
                vehicle_column=vehicle_column, household_column=None, rng=uc_dict["random_seed"],
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
                label=f"{label} {uc_id}" if label else uc_id,
            )
        else:
            (
                charging_locations_home,
                located_charging_events,
            ) = uc_helpers.distribute_charging_events(
                in_region,
                charging_events,
                weight_column="weight",
                simulation_steps=simulation_steps, fill_existing_first=True,
                rng=uc_dict["random_seed"],
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
            )

    else:
        pass

    # Merge Chargin_events and Locations
    charging_locations_home["index"] = charging_locations_home.index
    _n_events_before_merge = len(located_charging_events)
    located_charging_events = located_charging_events.merge(
        charging_locations_home, left_on="assigned_location", right_on="index"
    )
    uc_helpers.check_event_loss(f"home_{mode}", _n_events_before_merge, len(located_charging_events), uc_dict)
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

    # energy of the events placed by THIS call (home_detached overflow events are counted with home_apartment)
    energy = charging_events["energy"].sum() - (overflow_energy if mode == "detached" else 0.0)
    return (int(charging_locations["charging_points"].sum()), int(energy),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

def work(work_data, uc_dict, office_data=None, timestep=15, weight_column="weight", simulation_steps=2000, vehicle_column=None,
         existing_points_column=None, existing_capacity_column=None, label=None,
         # Unlike home (see home()'s own comment), work charging plausibly
         # has genuine reuse - several employees at the same workplace can
         # share one office charging point across different days/shifts -
         # so a larger search is more likely to actually pay off here, not
         # just cost more for nothing.
         max_reuse_candidates=1_000):
    uc_id = "work"

    charging_events = (
        uc_dict["charging_event"]
        .loc[uc_dict["charging_event"]["charging_use_case"] == "work"]
        .reset_index()
    )

    charging_events = charging_events

    charging_events['office'] = np.random.choice([True, False], size=len(charging_events),
                                                 p=[uc_dict["share_office_parking"],
                                                    1 - uc_dict["share_office_parking"]])

    # filter houses by region
    # in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    # in_region = home_data.loc[in_region_bool].copy()

    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "work":

        in_region_not_office = work_data
        in_region_not_office["office"] = False
        in_region_office = office_data
        in_region_office["office"] = True
        # in_region = in_region.iloc[:800]

        charging_events_office = charging_events[charging_events["office"] == True].reset_index(drop=True)

        charging_events_not_office = charging_events[charging_events["office"] == False].reset_index(drop=True)

        (
            charging_locations_work_office,
            located_charging_events_office,
            availability_mask_office
        ) = uc_helpers.distribute_charging_events(
            in_region_office, charging_events_office, weight_column=weight_column, simulation_steps=simulation_steps,
            rng=uc_dict["random_seed"], return_mask=True, location_id_start= len(in_region_not_office)
        )
        charging_locations_work_office["office"] = True
        located_charging_events_office["office"] = True
        (
            charging_locations_work_not_office,
            located_charging_events_not_office,
            availability_mask_not_office
        ) = uc_helpers.distribute_charging_events(
            in_region_not_office, charging_events_not_office, weight_column=weight_column, simulation_steps=simulation_steps,
            rng=uc_dict["random_seed"], return_mask=True
        )
        charging_locations_work_not_office["office"] = False
        located_charging_events_not_office["office"] = False

        # Depot Ladeevents in den Nachtstunden (Mo-Sa zwischen 21:00 und 8:00 Uhr)
        # Same class of fix as the isin() rewrites above/in restructure_output.py:
        # "charging_use_case" == single value (no .isin([x]) fast-path issue),
        # and "Type" checked via a Python set + np.fromiter instead of
        # .isin(uc_dict["multi_use_group"]) - .isin() against an Arrow-backed
        # object column has no fast hash-table path regardless of how many
        # candidate values it's checking against.
        multi_use_group_set = set(uc_dict["multi_use_group"])
        type_np = uc_dict["charging_event"]["Type"].to_numpy(dtype=object)
        in_multi_use_group = np.fromiter(
            (t in multi_use_group_set for t in type_np), dtype=bool, count=type_np.size
        )
        charging_events_street = uc_dict["charging_event"].loc[
            (uc_dict["charging_event"]["charging_use_case"] == "street") & in_multi_use_group
        ]
        charging_events_public = charging_events_street.reset_index()
        charging_events_public["office"] = True

        # Verteilung der Street-Ladeevents auf Retail-Standorte (Multi-Use)
        charging_locations_work_after_multi_use, located_public_events = uc_helpers.distribute_charging_events(
            charging_locations_work_office, charging_events_public, weight_column=weight_column, simulation_steps=simulation_steps,
            rng=uc_dict["random_seed"], fill_existing_only=True, availability_mask=availability_mask_office,
            flexibility_multi_use=uc_dict["flexibility_multi_use"], location_id_start= len(in_region_not_office)
        ) # charging_events_depot austauschen gegen depot_night_events



        located_public_events_assigned = located_public_events[located_public_events["assigned_location"].notna()]

        located_public_events_assigned["multi_use"] = True

        located_charging_events = pd.concat([located_charging_events_not_office, located_charging_events_office])

        located_charging_events["multi_use"] = False

        # Kombiniere Retail- und Depot-Ladeevents
        located_charging_events = pd.concat([located_charging_events, located_public_events_assigned], ignore_index=True)

        charging_events_public_no_multi_use_possible = located_public_events[located_public_events["assigned_location"].isna()].reset_index()
        charging_events_public_no_multi_use_possible["office"] = False

        charging_locations_work_office = charging_locations_work_office.to_crs( charging_locations_work_not_office.crs)
        charging_locations_work = pd.concat([charging_locations_work_not_office, charging_locations_work_office])

    else:
        in_region = work_data

        if vehicle_column:
            charging_locations_work, located_charging_events, availability_mask = uc_helpers.distribute_charging_events_per_vehicle(
                in_region, charging_events, weight_column=weight_column, simulation_steps=simulation_steps,
                vehicle_column=vehicle_column, rng=uc_dict["random_seed"], return_mask=True,
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
                label=f"{label} {uc_id}" if label else uc_id, max_reuse_candidates=max_reuse_candidates,
                max_points_per_location=uc_dict.get("work_max_points"),
            )
        else:
            (
                charging_locations_work,
                located_charging_events,
                availability_mask
            ) = uc_helpers.distribute_charging_events(
                in_region, charging_events, weight_column=weight_column, simulation_steps=simulation_steps,
                rng=uc_dict["random_seed"], return_mask=True,
                existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
            )


    # Merge Chargin_events and Locations
    charging_locations_work["index"] = charging_locations_work.index
    _n_events_before_merge = len(located_charging_events)
    located_charging_events = located_charging_events.merge(
        charging_locations_work, left_on="assigned_location", right_on="index"
    )
    uc_helpers.check_event_loss("work", _n_events_before_merge, len(located_charging_events), uc_dict)
    # drop_cols = ["osm_id", "amenity", "building", "building_area", "synthetic", "ags", "overlay_id", "nuts", "bus_id", "probability"]
    located_charging_events = located_charging_events  # .drop(columns=drop_cols)
    located_charging_events_gdf = gpd.GeoDataFrame(
        located_charging_events, geometry="geometry"
    )
    located_charging_events_gdf.set_crs(3035)
    located_charging_events_gdf = located_charging_events_gdf.rename(columns={"office_x": "office"})

    # generate_ids and reduce columns
    located_charging_events_gdf["location_id"] = uc_helpers.get_id(uc_id, located_charging_events_gdf[
        "assigned_location"].astype(int))
    charging_locations_work["location_id"] = uc_helpers.get_id(uc_id, pd.Series(charging_locations_work.index).astype(int))

    # charging_locations = charging_locations_work[uc_dict["columns_output_locations"]]
    # located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "work":
        keys_events = uc_dict["columns_output_chargingevents"] + ["multi_use", "office"]
        keys_locations = uc_dict["columns_output_locations"] + ["office"]
        located_charging_events_gdf = located_charging_events_gdf[keys_events]
        charging_locations = charging_locations_work[keys_locations]
    else:
        located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]
        charging_locations = charging_locations_work[uc_dict["columns_output_locations"]]

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)


    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "work":
        charging_locations_office = charging_locations.loc[charging_locations["office"] == True]
        located_charging_events_gdf_office = located_charging_events_gdf.loc[located_charging_events_gdf["office"] == True]

        charging_locations_not_office = charging_locations.loc[charging_locations["office"] == False]
        located_charging_events_gdf_not_office = located_charging_events_gdf.loc[located_charging_events_gdf["office"] == False]

        utility.save(charging_locations, uc_id, "charging-locations-office-spezial", uc_dict)
        utility.save(located_charging_events_gdf, uc_id, "charging-events-office-spezial", uc_dict)
        utility.save(charging_locations_not_office, uc_id, "charging-locations-not_office", uc_dict)
        utility.save(located_charging_events_gdf_not_office, uc_id, "charging-events-not_office", uc_dict)

    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "work":
        return (int(charging_locations_office["charging_points"].sum()), int(located_charging_events_gdf_office["energy"].sum()),
            int((charging_locations_office["average_charging_capacity"]*charging_locations_office["charging_points"]).sum()),
                int(charging_locations_not_office["charging_points"].sum()), int(located_charging_events_gdf_not_office["energy"].sum()),
                int((charging_locations_not_office["average_charging_capacity"] * charging_locations["charging_points"]).sum()),
                charging_events_public_no_multi_use_possible)
    else:
        return (int(charging_locations["charging_points"].sum()), int(charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

def retail(retail_data: gpd.GeoDataFrame, uc_dict, simulation_steps=2000,
           existing_points_column=None, existing_capacity_column=None):
    """
    Calculate placements and energy distribution for use case hpc.

    :param retail_data: gpd.GeoDataFrame
        info about house types
    :param uc_dict: dict
        contains basic run info like region boundary and save directory

    """
    uc_id = "retail"

    charging_events_retail_slow = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"] == "retail"
    ]

    charging_events_retail_hpc = uc_dict["charging_event"].loc[
        (uc_dict["charging_event"]["charging_use_case"] == "urban_fast")
        & (uc_dict["charging_event"]["location"] == "shopping")
    ]

    charging_events = pd.concat(
        [charging_events_retail_slow, charging_events_retail_hpc],
        axis=0,
        ignore_index=True,
    ).reset_index()

    # filter houses by region
    # in_region_bool = home_data["geometry"].within(uc_dict["boundaries"].iloc[0,0])
    # in_region = home_data.loc[in_region_bool].copy()

    cols = ["weight", "geometry"]

    # carry existing-infrastructure and candidate-identity columns through
    # the column-selection below if present (see existing_infrastructure.py
    # / data_de_candidates.py); they aren't part of the fixed ALKIS/OSM
    # candidate schema `cols` selects.
    for extra_col in (existing_points_column, existing_capacity_column, "candidate_uid"):
        if extra_col and extra_col in retail_data.columns and extra_col not in cols:
            cols.append(extra_col)

    in_region = retail_data[cols].reset_index(drop=True)

    if in_region.empty or charging_events.empty:
        if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "retail":
            return 0, 0, 0, pd.DataFrame()
        else:
            return 0, 0, 0

    # Eigener RNG: Retail-Standorte sind szenariounabhängig deterministisch (unabhängig
    # davon, wie viele Zufallszahlen die anderen Use Cases der Gemeinde verbraucht haben).
    # Bis 2026-10-22 war der Strom für ALLE Gemeinden derselbe (default_rng(seed)): jede
    # Gemeinde startete mit identischen Zufallszahlen, die Retail-Ziehungen waren zwischen
    # den Gemeinden nicht unabhängig. Jetzt je Gemeinde (seed, AGS) plus ein fester Strom-
    # Marker, damit der Strom auch nicht mit dem allgemeinen Gemeinde-Generator
    # (seed, AGS) identisch ist. Ohne AGS im uc_dict (Berlin-Pipeline) bleibt es beim alten.
    if uc_dict.get("ags") is not None:
        rng_retail = np.random.default_rng((uc_dict["seed"], int(uc_dict["ags"]), RETAIL_RNG_STREAM))
    else:
        rng_retail = np.random.default_rng(uc_dict["seed"])
    (
        charging_locations_retail,
        located_charging_events,
        availability_mask,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="weight", simulation_steps=simulation_steps,
        rng=rng_retail, return_mask=True, existing_points_column=existing_points_column,
        existing_capacity_column=existing_capacity_column
    )

    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "retail":

        # Depot Ladeevents in den Nachtstunden (Mo-Sa zwischen 21:00 und 8:00 Uhr)
        # Same class of fix as the isin() rewrites above/in restructure_output.py:
        # "charging_use_case" == single value (no .isin([x]) fast-path issue),
        # and "Type" checked via a Python set + np.fromiter instead of
        # .isin(uc_dict["multi_use_group"]) - .isin() against an Arrow-backed
        # object column has no fast hash-table path regardless of how many
        # candidate values it's checking against.
        multi_use_group_set = set(uc_dict["multi_use_group"])
        type_np = uc_dict["charging_event"]["Type"].to_numpy(dtype=object)
        in_multi_use_group = np.fromiter(
            (t in multi_use_group_set for t in type_np), dtype=bool, count=type_np.size
        )
        charging_events_street = uc_dict["charging_event"].loc[
            (uc_dict["charging_event"]["charging_use_case"] == "street") & in_multi_use_group
        ]
        charging_events_public = charging_events_street.reset_index()

        # Verteilung der Street-Ladeevents auf Retail-Standorte
        charging_locations_retail_after_multi_use, located_public_events = uc_helpers.distribute_charging_events(
            charging_locations_retail, charging_events_public, weight_column="weight", simulation_steps=simulation_steps,
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
    _n_events_before_merge = len(located_charging_events)
    located_charging_events = located_charging_events.merge(
        charging_locations_retail, left_on="assigned_location", right_on="index"
    )
    uc_helpers.check_event_loss("retail", _n_events_before_merge, len(located_charging_events), uc_dict)
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
    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "retail":
        keys = uc_dict["columns_output_chargingevents"] + ["multi_use"]
        located_charging_events_gdf = located_charging_events_gdf[keys]
    else:
        located_charging_events_gdf = located_charging_events_gdf[uc_dict["columns_output_chargingevents"]]

    # todo checken o alle ids stimmen (bei multi-use-szenario)

    charging_locations = charging_locations[charging_locations["charging_points"] != 0]

    utility.save(charging_locations, uc_id, "charging-locations", uc_dict)
    utility.save(located_charging_events_gdf, uc_id, "charging-events", uc_dict)

    # utility.plot_occupation_of_charging_points(located_charging_events, uc_id)


    if uc_dict["multi_use_concept"] and uc_dict["use_case_multi_use"] == "retail":
        return (int(charging_locations["charging_points"].sum()), int(located_charging_events["energy"].sum()),
                int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()),
                charging_events_public_no_multi_use_possible)
    else:
        return (int(charging_locations["charging_points"].sum()), int(located_charging_events["energy"].sum()),
                int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))

def depot(depot_data: gpd.GeoDataFrame, uc_dict, simulation_steps=2000,
          existing_points_column=None, existing_capacity_column=None):
    uc_id = "depot"
    charging_events_depot = uc_dict["charging_event"].loc[
        uc_dict["charging_event"]["charging_use_case"] == "depot"
    ]

    charging_events = charging_events_depot.reset_index()

    in_region = depot_data

    (
        charging_locations_depot,
        located_charging_events,
    ) = uc_helpers.distribute_charging_events(
        in_region, charging_events, weight_column="weight", simulation_steps=simulation_steps,
        rng=uc_dict["random_seed"],
        existing_points_column=existing_points_column, existing_capacity_column=existing_capacity_column,
    )

    # Merge Chargin_events and Locations
    charging_locations_depot["index"] = charging_locations_depot.index
    _n_events_before_merge = len(located_charging_events)
    located_charging_events = located_charging_events.merge(
        charging_locations_depot, left_on="assigned_location", right_on="index"
    )
    uc_helpers.check_event_loss("depot", _n_events_before_merge, len(located_charging_events), uc_dict)

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

    return (int(charging_locations["charging_points"].sum()), int(located_charging_events["energy"].sum()),
            int((charging_locations["average_charging_capacity"]*charging_locations["charging_points"]).sum()))
