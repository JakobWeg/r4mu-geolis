import pandas as pd
import geopandas as gpd
import numpy as np
import configparser as cp
import argparse
from datetime import datetime
import pathlib
from pyogrio import read_dataframe
import os
import json
import csv

import use_case as uc
import use_case_helpers
import utility


# todo Output einer Metadatei programmieren: Info zu Anzahl an Ladepunkten, installierter Leistung, Energie

def parse_data(args):
    # read config file
    print("--- reading config file ---")
    parser = cp.ConfigParser()
    scenario_path = pathlib.Path('scenario')
    cfg_file = pathlib.Path(scenario_path, 'config_stralsund.cfg')
    data_dir = pathlib.Path('data')

    if not cfg_file.is_file():
        raise FileNotFoundError(f'Config file {cfg_file} not found.')
    try:
        parser.read(cfg_file)
    except Exception:
        raise FileNotFoundError(f'Cannot read config file {cfg_file} - malformed?')

    run_hpc = parser.getboolean('use_cases', 'hpc')
    run_public = parser.getboolean('use_cases', 'public')
    run_home = parser.getboolean('use_cases', 'home')
    run_work = parser.getboolean('use_cases', 'work')
    run_retail = parser.getboolean('use_cases', 'retail')
    run_depot = parser.getboolean('use_cases', 'depot')

    # always used parameters
    boundaries = gpd.read_file(pathlib.Path(data_dir, parser.get('data', 'boundaries')))
    boundaries = boundaries.to_crs(3035)

    # create results dir
    timestamp_now = datetime.now()
    timestamp = timestamp_now.strftime("%y-%m-%d_%H%M%S")
    result_dir = pathlib.Path('results', '_{}'.format(timestamp))
    result_dir.mkdir(exist_ok=True, parents=True)

    rng_seed = parser['basic'].getint('random_seed', None)
    rng = np.random.default_rng(rng_seed)

    config_dict = {
        'boundaries': boundaries,
        'run_hpc': run_hpc,
        'run_public': run_public,
        'run_home': run_home,
        'run_work': run_work,
        'run_retail': run_retail,
        'run_depot': run_depot,
        'charging_time_limit': parser.getboolean('uc_params', 'charging_time_limit'),
        'charging_time_limit_duration': parser.getint('uc_params', 'charging_time_limit_duration'),
        'charging_time_limit_start': parser.getint('uc_params', 'charging_time_limit_start'),
        'charging_time_limit_end': parser.getint('uc_params', 'charging_time_limit_end'),
        'visual': parser.getboolean("basic", "plots"),
        # 'charge_info': charge_info_dict,
        'scenario_name': args.scenario,
        'seed': rng_seed,
        'random_seed': rng,
        'mode': args.mode,
        'multi_use_concept': parser['basic'].getboolean('multi_use_concept', None),
        'multi_use_group': parser['basic'].get('multi_use_group').split(', '),
        'flexibility_multi_use': parser['basic'].getint('flexibility_multi_use', 0),
        'charge_events_private_path': parser.get('data', 'charging_events_private'),
        'charge_events_commercial_path': parser.get('data', 'charging_events_commercial'),
        'result_dir': result_dir,
        'results_summary': {}
    }

    if run_hpc:
        hpc_pos_file = parser.get('data', 'hpc_positions_fuel_stations')
        hpc_traffic = parser.get('data', 'hpc_traffic_count')
        positions = gpd.read_file(pathlib.Path(data_dir, hpc_pos_file), encoding='latin1')
        traffic = gpd.read_file(pathlib.Path(data_dir, hpc_traffic), encoding='latin1')

        # calculate weighted hpc locations
        hpc_locations = utility.calculate_hpc_locations(positions, traffic)
        config_dict["hpc_points"] = hpc_locations
        # if run_retail:
        #     config_dict["hpc_share_retail"] = parser.getfloat("uc_params", "hpc_share_retail"),
        print("--- parsing hpc data done ---")

    if run_home or run_public:
        buildings_data_file = parser.get('data', 'building_data')
        demand_profiles_data = parser.get('data', 'home_demand_profiles')

        home_data = gpd.read_file(pathlib.Path(data_dir, buildings_data_file),
                                       engine='pyogrio', use_arrow=True) # engine='pyogrio',

        demand_profiles = pd.read_csv(pathlib.Path(data_dir, demand_profiles_data))
        demand_profiles.rename(columns={'building_id': 'id'}, inplace=True)
        home_data = home_data.merge(demand_profiles[["id", "households_total"]], on='id', how='left')

        home_data = home_data.loc[(home_data["cts_demand"].astype(float) == 0) & (home_data["households_total"].notna())]

        home_data_detached = home_data.loc[home_data["households_total"].isin([1, 2])]
        home_data_apartment = home_data.loc[~home_data["households_total"].isin([1, 2])]
        # buildings_data = read_dataframe(pathlib.Path(data_dir, buildings_data_file))
        print("--- parsing home data done ---")
        buildings_data_file_detached = home_data_detached.to_crs(3035)
        buildings_data_file_apartment = home_data_apartment.to_crs(3035)

        config_dict.update({
            "home_data_apartment": buildings_data_file_apartment,
            "home_data_detached": buildings_data_file_detached,
            "share_home_detached": parser.getfloat('uc_params', 'share_home_detached'),
            "share_home_apartment": parser.getfloat('uc_params', 'share_home_apartment')
        })
        home_data_detached.to_file("data/home_data_detached.gpkg")
        home_data_apartment.to_file("data/home_data_apartment.gpkg")

    if run_public:
        public_data_file = parser.get('data', 'public_poi')
        public_data = gpd.read_file(pathlib.Path(data_dir, public_data_file))
        public_data = public_data.to_crs(3035)

        public_home_street_data = home_data_apartment

        additional_public_input = parser.getboolean('data', 'additional_input_public_locations')

        config_dict.update({'additional_public_input': additional_public_input,
                            'poi_data': public_data,
                            'home_street_data': public_home_street_data,
                            })

        if additional_public_input:
            additional_public_locations_file = parser.get('data', 'additional_public_locations')
            public_locations = gpd.read_file(pathlib.Path(data_dir, additional_public_locations_file))

            # Sicherstellen, dass beide denselben CRS haben
            # additional_public_data = additional_public_locations.to_crs(public_data.crs)

            additional_public_events_file = parser.get('data', 'additional_public_events')
            public_events = gpd.read_file(pathlib.Path(data_dir, additional_public_events_file))

            # Innerer räumlicher Join: nur Punkte, die in beiden vorkommen (genau gleiche Geometrie)
            # public_data = public_data.merge(additional_public_data, on='geometry', how='inner', suffixes=('_1', '_2'))

            # additional_home_street_data_file = parser.get('data', 'additional_home_street_locations')
            # additional_home_street_data = gpd.read_file(pathlib.Path(data_dir, additional_public_data_file))

            # Sicherstellen, dass beide denselben CRS haben
            # additional_public_data = additional_home_street_data.to_crs(public_home_street_data.crs)

            # public_home_street_data = public_home_street_data.merge(additional_home_street_data,
            #                                                         on='geometry', how='inner', suffixes=('_1', '_2'))


            config_dict.update({'additional_public_input': additional_public_input,
                                'additional_public_locations': public_locations,
                                'additional_public_events': public_events,
                                'poi_data': public_data,
                                'home_street_data': public_home_street_data,
                                })
        print("--- parsing public data done ---")

    if run_work:
        work_retail = float(parser.get('uc_params', 'work_weight_retail'))
        work_commercial = float(parser.get('uc_params', 'work_weight_commercial'))
        work_industrial = float(parser.get('uc_params', 'work_weight_industrial'))
        buildings_data_file = parser.get('data', 'building_data')
        work_data = gpd.read_file(pathlib.Path(data_dir, buildings_data_file),
                             engine='pyogrio', use_arrow=True)
        work_data = work_data.loc[work_data["cts_demand"].astype(float) != 0]
        work_data = work_data.to_crs(3035)
        work_dict = {'retail': work_retail, 'commercial': work_commercial, 'industrial': work_industrial}
        config_dict.update({'work': work_data, 'work_dict': work_dict})
        print("--- parsing work data done ---")

    if run_retail:
        # zensus_data_file = parser.get('data', 'zensus_data')
        # zensus_data = gpd.read_file(pathlib.Path(data_dir, zensus_data_file))
        # zensus_data = zensus_data.to_crs(3035)
        retail_data_file = parser.get('data', 'retail_data')

        retail_data = gpd.read_file(pathlib.Path(data_dir, retail_data_file),
                                       engine='pyogrio', use_arrow=True) # engine='pyogrio',
        # buildings_data = read_dataframe(pathlib.Path(data_dir, buildings_data_file))
        print("--- parsing retail data done ---")
        retail_data = retail_data.to_crs(3035)
        retail_data["area"] = retail_data.geometry.area
        retail_data["geometry"] = retail_data["geometry"].centroid
        config_dict.update({'retail_parking_lots': retail_data})

    if run_depot:
        depot_data_file = parser.get('data', 'depot_data')

        depot_data = gpd.read_file(pathlib.Path(data_dir, depot_data_file),
                                    engine='pyogrio', use_arrow=True)  # engine='pyogrio',

        config_dict.update({'depot': depot_data})

    return config_dict


def parse_car_data(args, data_dict):
    scenario_path = pathlib.Path(args.scenario)
    ts_private_path = pathlib.Path(scenario_path, data_dict["charge_events_private_path"])
    ts_commercial_path = pathlib.Path(scenario_path, data_dict["charge_events_commercial_path"])

    dataframes = []

    for file in os.listdir(ts_private_path):
        if file.endswith(".parquet"):
            file_path = os.path.join(ts_private_path, file)
            df = pd.read_parquet(file_path)  # Read the Parquet file
            # write car-type into column
            antriebsarten = ['bev', 'phev']
            fahrzeugklassen = ['mini', 'medium', 'luxury']

            found_antrieb = next((word for word in antriebsarten if word in file_path.lower()), None)
            found_klasse = next((word for word in fahrzeugklassen if word in file_path), None)

            df["car_type"] = f"{found_antrieb}_{found_klasse}"
            dataframes.append(df)

    # Concatenate all DataFrames vertically
    charging_events_private = pd.concat(dataframes, ignore_index=True)

    # charging_events = pd.read_csv(ts_path, sep=",")
    # charging_events = pd.read_parquet(ts_path)
    charging_events_private = charging_events_private.loc[charging_events_private["station_charging_capacity"] != 0]
    # cut of first week and limit to one week
    charging_events_private = charging_events_private.loc[charging_events_private["event_start"] > (24*7*4)]
    charging_events_private["event_start"] = charging_events_private["event_start"] - (24*7*4)

    charging_events_commercial = pd.read_parquet(ts_commercial_path)
    charging_events_commercial["use_case"] = charging_events_commercial["use_case"].str.replace(
        "public", "street", regex=False)

    charging_events_private["Type"] = "Private"
    charging_events_commercial["Type"] = "Commercial"

    charging_events_commercial = charging_events_commercial.drop(columns=["charge_end"])

    # verteilung der commercial retail Ladeevents war falsch. Umverteilung von Retail commercial auf street

    # charging_events_commercial["use_case"].loc[
    #     charging_events_commercial["use_case"].isin(["retail"])] = "street"
    #
    charging_events_commercial["charging_use_case"] = charging_events_commercial["use_case"]

    # todo: check, ob beide Datensätze zur gleichen Zeit am gleichen Tag starten.
    charging_events = pd.concat([charging_events_commercial, charging_events_private], ignore_index=True, sort=False)

    charging_events = charging_events.drop(columns=["average_charging_power"])

    charging_events = charging_events[charging_events["event_start"] <= (24*7*4)].reset_index(drop=True)

    charging_events["event_id"] = range(1, len(charging_events) + 1)

    print ("--- parsing charging events done")

    if data_dict["run_home"]:
        # Maske: nur dort, wo der zielwert vorkommt
        maske = charging_events["charging_use_case"] == "home"
        anzahl = maske.sum()

        # Neue Werte zufällig generieren
        neue_werte = data_dict["random_seed"].choice(["home_apartment", "home_detached"],
                                      size=anzahl, p=[data_dict["share_home_apartment"], data_dict["share_home_detached"]])

        # Einsetzen der neuen Werte
        charging_events.loc[maske, "charging_use_case"] = neue_werte

    if data_dict["charging_time_limit"]:

        charging_use_case = "street"
        charging_events = use_case_helpers.park_time_limitation(charging_events, data_dict, charging_use_case)

        print("Ladezeitbegrenzung für public charging: ",data_dict["charging_time_limit"],
          " Begrenzung auf ", data_dict["charging_time_limit_duration"], " Stunden zwischen ",
          data_dict["charging_time_limit_start"], " und ", data_dict["charging_time_limit_end"], " Uhr")

    return charging_events


def parse_default_data(args):
    data_dict = parse_data(args)
    charging_event_data = parse_car_data(args, data_dict)
    data_dict["charging_event"] = charging_event_data
    data_dict["columns_output_locations"] = ["location_id", "charging_points", "average_charging_capacity", "geometry"]
    data_dict["columns_output_chargingevents"] = ["event_id", "charging_use_case", "car_type", "event_start", "event_time",
                                                  "energy", "soc_start", "soc_end", "station_charging_capacity",
                                                  "location_id", "geometry"]
    return data_dict


def parse_potential_data(args):
    data_dict = parse_data(args)
    scenario_path = pathlib.Path('scenarios', args.scenario)
    region_data = pd.read_csv(pathlib.Path(scenario_path, "regions.csv"), converters={"AGS": lambda x: str(x)})

    region_data = region_data.to_dict()
    data_dict.update(region_data)
    return data_dict


def run_use_cases(data_dict):

    if data_dict['run_home']:
        uc_name = "home_detached"
        data_dict["results_summary"][uc_name] = {}
        data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
            data_dict["results_summary"][uc_name]["installed_power"] = uc.home(data_dict['home_data_detached'],
                data_dict, mode="detached")
        uc_name = "home_apartment"
        data_dict["results_summary"][uc_name] = {}
        data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
            data_dict["results_summary"][uc_name]["installed_power"] = uc.home(data_dict['home_data_apartment'],
                data_dict, mode="apartment")

    if data_dict['run_work']:
        uc_name = "work"
        data_dict["results_summary"][uc_name] = {}
        data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
            data_dict["results_summary"][uc_name]["installed_power"] = uc.work(data_dict[uc_name],
                data_dict)

    if data_dict['run_hpc']:
        uc_name = "hpc"
        data_dict["results_summary"][uc_name] = {}
        data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
            data_dict["results_summary"][uc_name]["installed_power"] = uc.hpc(data_dict['hpc_points'], data_dict)

    if data_dict['run_retail']:
        uc_name = "retail"
        data_dict["results_summary"][uc_name] = {}
        if data_dict["multi_use_concept"]:
            data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
                data_dict["results_summary"][uc_name]["installed_power"], charging_events_public_after_multi_use = (
                uc.retail(data_dict['retail_parking_lots'],
                    data_dict))
        else:
            data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
                data_dict["results_summary"][uc_name]["installed_power"] = uc.retail(data_dict['retail_parking_lots'],
                    data_dict)

    if data_dict['run_public']:
        uc_name = "public"
        data_dict["results_summary"][uc_name] = {}

        if data_dict["multi_use_concept"]:
            data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
                data_dict["results_summary"][uc_name]["installed_power"] = uc.public(data_dict['poi_data'],
                                                                                     data_dict['home_street_data'],
                                                                                     data_dict,
                                                                                     charging_locations_public_after_multi_use=charging_events_public_after_multi_use)
        else:
            data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
                data_dict["results_summary"][uc_name]["installed_power"] = uc.public(data_dict['poi_data'],
                                                                                     data_dict['home_street_data'],
                                                                                     data_dict)

    if data_dict['run_depot']:
        uc_name = "depot"
        data_dict["results_summary"][uc_name] = {}

        data_dict["results_summary"][uc_name]["charging_points"], data_dict["results_summary"][uc_name]["energy"], \
        data_dict["results_summary"][uc_name]["installed_power"] = uc.depot(data_dict[uc_name],
                data_dict)

    return data_dict["results_summary"]

def main():
    print('Reading input data...')

    parser = argparse.ArgumentParser(description='Tool for allocation of charging infrastructure')
    parser.add_argument('scenario', nargs='?',
                           help='Set name of the scenario directory', default="scenario")
    parser.add_argument('--mode', default="default", type=str, help="Choose simulation mode: default "
                                                                    "(using SimBEV inputs) or potential "
                                                                    "(returning all potential spots in the region)")
    p_args = parser.parse_args()

    data = parse_default_data(p_args)

    result_summary = run_use_cases(data)

    if data["visual"]:
        print("--- starting visualisation ---")
        # utility.plot_occupation_of_charging_points(result_summary)

    # save meta data
    meta_data = {k: data.get(k) for k in ['charge_events_private_path', 'charge_events_commercial_path',
                                          'multi_use_concept', 'flexibility_multi_use', 'multi_use_group',
                                          'charging_time_limit', 'charging_time_limit_duration',
                                          'charging_time_limit_start', 'charging_time_limit_end']}

    with open(os.path.join(data["result_dir"],'metadata.json'), 'w') as f:
        json.dump(meta_data, f)

    flattened_data = []

    for key, value in result_summary.items():
        # Kombiniere den äußeren Schlüssel mit den inneren Daten
        flattened_data.append({'location': key, **value})

    print(result_summary)
    print(flattened_data)

    with open(os.path.join(data["result_dir"],'result_summary.csv'), mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["location", "charging_points", "energy", "installed_power"])
        # Schreibe die Kopfzeile (Schlüssel)
        writer.writeheader()
        # Schreibe die Zeilen
        writer.writerows(flattened_data)


if __name__ == '__main__':
    main()