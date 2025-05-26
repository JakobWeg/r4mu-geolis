import geopandas as gpd
import pandas as pd
import os
import math
import datetime
from datetime import timedelta
import matplotlib.pyplot as plt


def plot_occupation_of_charging_points(events_df, uc_id):

    print("start of plotting timeline")

    # Zeitraster
    start = math.floor(events_df["event_start"].min())
    events_df["event_end"] = events_df["event_start"]+events_df["event_time"]
    end = math.ceil(events_df["event_end"]).max()
    zeitindex = list(range(start, end))
    zeitindex_df = pd.DataFrame({'zeitindex': zeitindex})

    # Zeitraster vorbereiten
    timeline = pd.DataFrame(index=zeitindex_df["zeitindex"])
    for use_case in events_df["use_case"].unique():
        timeline[use_case] = 0

    # Ladeevents einsortieren
    for _, row in events_df.iterrows():
        ladezeiten_dt = pd.date_range(start=row["event_start"], end=row["event_end"], freq="H",
                                      closed="left")
        ladezeiten = pd.DataFrame(
            list(range(int(round(row["event_start"], 0)), int(round(row["event_end"] + 1, 0)))))
        for zeit in ladezeiten[0]:
            if zeit in timeline.index:
                timeline.at[zeit, row["charging_use_case"]] += 1

    use_cases = timeline.columns.tolist()

    # add day and time
    # Starte Montag 00:00 Uhr
    startzeit_date = datetime.strptime("2024-01-01 00:00", "%Y-%m-%d %H:%M")  # 1. Jan. 2024 ist ein Montag

    # Erstelle eine neue Spalte mit Datum + Uhrzeit
    times = pd.Series(timeline.index.values)
    timeline['Datum_Uhrzeit'] = times.apply(lambda h: startzeit_date + timedelta(hours=h - 2))

    # Extrahiere Wochentag + Uhrzeit
    timeline['Wochentag_Uhrzeit'] = timeline['Datum_Uhrzeit'].dt.strftime('%A %H:%M')

    # Optional: deutsche Wochentage (wenn du willst)
    wochentage_de = {
        'Monday': 'Montag',
        'Tuesday': 'Dienstag',
        'Wednesday': 'Mittwoch',
        'Thursday': 'Donnerstag',
        'Friday': 'Freitag',
        'Saturday': 'Samstag',
        'Sunday': 'Sonntag'
    }
    timeline['Wochentag_Uhrzeit'] = timeline['Datum_Uhrzeit'].dt.strftime('%A %H:%M').replace(wochentage_de,
                                                                                              regex=True)

    timeline.to_csv(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{uc_id}.csv",
                    index=False)
    # Daten für Stackplot vorbereiten
    # use_cases = timeline.columns.tolist()
    werte = [timeline[uc].values for uc in use_cases]

    # Gestapelter Linien-Plot (stackplot = gestapelte Flächen)
    # plt.figure(figsize=(12, 6))
    # plt.stackplot(timeline.index, werte, labels=use_cases)
    #
    # plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    # plt.xlabel("Zeit")
    # plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    # plt.legend(title="Use-Case")
    # plt.grid(True)
    # plt.tight_layout()
    # plt.savefig(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{uc_id}")
    #
    # plt.close()

    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline['Datum_Uhrzeit'], timeline['depot'], labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeit")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{uc_id}")

    plt.close()

    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline['Datum_Uhrzeit'], werte, labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeit")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_Uhrzeit_{uc_id}")


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