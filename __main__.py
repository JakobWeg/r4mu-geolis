import pandas as pd
import geopandas as gpd
import numpy as np
import configparser as cp
import argparse
from datetime import datetime
import pathlib
from pyogrio import read_dataframe
import os

import use_case as uc


def parse_data(args):
    # read config file
    parser = cp.ConfigParser()
    scenario_path = pathlib.Path('scenario')
    cfg_file = pathlib.Path(scenario_path, 'config.cfg')
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
        'visual': parser.getboolean("basic", "plots"),
        # 'charge_info': charge_info_dict,
        'scenario_name': args.scenario,
        'random_seed': rng,
        'mode': args.mode,
        'charge_events_path': parser.get('data', 'charging_events'),
        'result_dir': result_dir
    }

    if run_hpc:
        hpc_pos_file = parser.get('data', 'hpc_positions')
        positions = gpd.read_file(pathlib.Path(data_dir, hpc_pos_file))
        config_dict["hpc_points"] = positions
        if run_retail:
            config_dict["hpc_share_retail"] = parser.getfloat("uc_params", "hpc_share_retail"),
        print("--- parsing hpc data done ---")

    if run_public:
        public_data_file = parser.get('data', 'public_poi')
        public_data = gpd.read_file(pathlib.Path(data_dir, public_data_file))
        # public_pos_file = parser.get('data', 'public_positions')
        # public_positions = gpd.read_file(pathlib.Path(data_dir, public_pos_file))
        config_dict.update({'poi_data': public_data})
        print("--- parsing public data done ---")

    if run_home:
        # zensus_data_file = parser.get('data', 'zensus_data')
        # zensus_data = gpd.read_file(pathlib.Path(data_dir, zensus_data_file))
        # zensus_data = zensus_data.to_crs(3035)
        buildings_data_file = parser.get('data', 'building_data')
        demand_profiles_data = parser.get('data', 'home_demand_profiles')

        home_data = gpd.read_file(pathlib.Path(data_dir, buildings_data_file),
                                       engine='pyogrio', use_arrow=True) # engine='pyogrio',

        demand_profiles = pd.read_csv(pathlib.Path(data_dir, demand_profiles_data))
        demand_profiles.rename(columns={'building_id': 'id'}, inplace=True)
        home_data = home_data.merge(demand_profiles[["id", "households_total"]], on='id', how='left')

        home_data = home_data.loc[(home_data["cts"].astype(float) == 0) & (home_data["households_total"].notna())]

        home_data_detached = home_data.loc[home_data["households_total"].isin([1, 2])]
        home_data_apartment = home_data.loc[~home_data["households_total"].isin([1, 2])]
        # buildings_data = read_dataframe(pathlib.Path(data_dir, buildings_data_file))
        print("--- parsing home data done ---")
        buildings_data_file_detached = home_data_detached.to_crs(3035)
        buildings_data_file_apartment = home_data_apartment.to_crs(3035)

        config_dict.update({
            "sfh_available": parser.getfloat("uc_params", "single_family_home_share"),
            "sfh_avg_spots": parser.getfloat("uc_params", "single_family_home_spots"),
            "mfh_available": parser.getfloat("uc_params", "multi_family_home_share"),
            "mfh_avg_spots": parser.getfloat("uc_params", "multi_family_home_spots"),
            "home_data_apartment": buildings_data_file_apartment,
            "home_data_detached": buildings_data_file_detached,
        })

    if run_work:
        work_retail = float(parser.get('uc_params', 'work_weight_retail'))
        work_commercial = float(parser.get('uc_params', 'work_weight_commercial'))
        work_industrial = float(parser.get('uc_params', 'work_weight_industrial'))
        buildings_data_file = parser.get('data', 'building_data')
        work_data = gpd.read_file(pathlib.Path(data_dir, buildings_data_file),
                             engine='pyogrio', use_arrow=True)
        work_data = work_data.loc[work_data["cts"].astype(float) != 0]
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
        retail_data["geometry"] = retail_data["geometry"].centroid

        config_dict.update({
            "sfh_available": parser.getfloat("uc_params", "single_family_home_share"),
            "sfh_avg_spots": parser.getfloat("uc_params", "single_family_home_spots"),
            "mfh_available": parser.getfloat("uc_params", "multi_family_home_share"),
            "mfh_avg_spots": parser.getfloat("uc_params", "multi_family_home_spots"),
            "retail_parking_lots": retail_data
        })

    return config_dict


def parse_car_data(args, data_dict):
    scenario_path = pathlib.Path(args.scenario, data_dict["charge_events_path"])
    ts_path = pathlib.Path(scenario_path, )

    dataframes = []

    for file in os.listdir(ts_path):
        if file.endswith(".parquet"):
            file_path = os.path.join(ts_path, file)
            df = pd.read_parquet(file_path)  # Read the Parquet file
            dataframes.append(df)

    # Concatenate all DataFrames vertically
    charging_events = pd.concat(dataframes, ignore_index=True)

    # charging_events = pd.read_csv(ts_path, sep=",")
    # charging_events = pd.read_parquet(ts_path)
    charging_events = charging_events.loc[charging_events["station_charging_capacity"] != 0]
    print ("--- parsing charging events done")

    return charging_events


def parse_default_data(args):
    data_dict = parse_data(args)
    charging_event_data = parse_car_data(args, data_dict)
    data_dict["charging_event"] = charging_event_data
    return data_dict


def parse_potential_data(args):
    data_dict = parse_data(args)
    scenario_path = pathlib.Path('scenarios', args.scenario)
    region_data = pd.read_csv(pathlib.Path(scenario_path, "regions.csv"), converters={"AGS": lambda x: str(x)})

    region_data = region_data.to_dict()
    data_dict.update(region_data)
    return data_dict


def run_use_cases(data_dict):
    if data_dict['run_hpc']:
        uc.hpc(data_dict['hpc_points'], data_dict)

    if data_dict['run_public']:
        uc.public(data_dict['poi_data'],
                  data_dict)

    if data_dict['run_home']:

        uc.home(data_dict['home_data_detached'],
                data_dict, mode="detached")
        uc.home(data_dict['home_data_apartment'],
                data_dict, mode="apartment")

    if data_dict['run_work']:
        uc.work(data_dict['work'],
                data_dict)

    if data_dict['run_retail']:
        uc.retail(data_dict['retail_parking_lots'],
                data_dict)
def main():
    print('Reading TracBEV input data...')

    parser = argparse.ArgumentParser(description='TracBEV tool for allocation of charging infrastructure')
    parser.add_argument('scenario', nargs='?',
                           help='Set name of the scenario directory', default="scenario")
    parser.add_argument('--mode', default="default", type=str, help="Choose simulation mode: default "
                                                                    "(using SimBEV inputs) or potential "
                                                                    "(returning all potential spots in the region)")
    p_args = parser.parse_args()

    data = parse_default_data(p_args)

    run_use_cases(data)

if __name__ == '__main__':
    # todo: einarbeiten der bestehenden LIS (Im UC Public, Retail und hpc)
    main()