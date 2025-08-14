import geopandas as gpd
import pandas as pd
import os
import math
import datetime
from datetime import timedelta
import matplotlib.pyplot as plt


def plot_occupation_of_charging_points(events_df, uc_id, year, scenario):
    print("start of plotting timeline")

    # Zeitraster
    start = math.floor(events_df["event_start"].min())
    events_df["event_end"] = events_df["event_start"] + events_df["event_time"]
    end = events_df["event_end"].max()
    zeitindex = list(range(start, end))
    zeitindex_df = pd.DataFrame({'zeitindex': zeitindex})

    # Zeitraster vorbereiten
    timeline = pd.DataFrame(index=zeitindex_df["zeitindex"])
    for use_case in events_df["charging_use_case"].unique():
        timeline[use_case] = 0

    # Ladeevents einsortieren
    for _, row in events_df.iterrows():
        ladezeiten = pd.DataFrame(
            list(range(int(round(row["event_start"], 0)),
                       int(round(row["event_end"], 0))))
        )
        for zeit in ladezeiten[0]:
            if zeit in timeline.index:
                timeline.at[zeit, row["charging_use_case"]] += 1

    use_cases = timeline.columns.tolist()

    # Neuen fortlaufenden Zeitschritt erzeugen
    timeline = timeline.reset_index(drop=True)  # alten Index entfernen
    timeline["Zeitschritt"] = range(1, len(timeline) + 1)

    timeline.to_csv(
        f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{uc_id}.csv",
        index=False
    )

    # Daten für Stackplot vorbereiten
    werte = [timeline[uc].values for uc in use_cases]

    print("start plotting 1")
    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline['Zeitschritt'], timeline['retail'], labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeitschritt")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(
        f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{uc_id}"
    )

    plt.close()
    print("start plotting 2")
    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline['Zeitschritt'], werte, labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeitschritt")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(
        f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{scenario}_{year}_{uc_id}"
    )

def weights_to_dict(weights: pd.DataFrame):
    result = {}
    for i in weights.index:
        osm_key = weights.at[i, "OSM-Key"]
        osm_value = weights.at[i, "OSM-Value"]
        value = weights.at[i, "weight"]
        key = osm_key + ':' + osm_value
        result[key] = value
    return result


# save in .csv format
def save(data: gpd.GeoDataFrame, uc, dataset_name, uc_dict):

    filename = 'output_{}_{}'.format(uc, dataset_name)
    save_path_csv = os.path.join(uc_dict["result_dir"], filename + '.csv')
    save_path_gpkg = os.path.join(uc_dict["result_dir"], filename + '.gpkg')
    data.reset_index(drop=True, inplace=True)
    data.to_csv(save_path_csv, sep=',', decimal='.')
    if isinstance(data, gpd.GeoDataFrame):
        data.to_file(save_path_gpkg, driver="GPKG")
    print('saving {} in region {} successful'.format(uc, dataset_name))

def save_data(data: gpd.GeoDataFrame, uc, dataset_name, uc_dict):

    filename = 'output_{}_{}'.format(uc, dataset_name)
    save_path_csv = os.path.join(uc_dict["result_dir"], filename + '.csv')
    save_path_gpkg = os.path.join(uc_dict["result_dir"], filename + '.gpkg')
    data.reset_index(drop=True, inplace=True)
    data.to_csv(save_path_csv, sep=',', decimal='.')
    print('saving {} in region {} successful'.format(uc, dataset_name))


if __name__ == '__main__':
    year = 2045
    scenario = "multi-use-flex"
    df = pd.read_csv("results/_25-08-13_091910/output_retail_charging-events.csv", index_col=0)
    df.loc[df["multi_use"], "charging_use_case"] = "retail_multi-use"
    df.loc[df["charging_use_case"] == "urban_fast", "charging_use_case"] = "retail"
    plot_occupation_of_charging_points(df, "retail", year, scenario)