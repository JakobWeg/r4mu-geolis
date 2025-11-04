import geopandas as gpd
import pandas as pd
import os
import math
import datetime
from datetime import timedelta
#import matplotlib.pyplot as plt

import geopandas as gpd
from shapely.geometry import Point

def calculate_hpc_locations(standorte_gdf, linien_gdf, verkehrs_spalte='dtvw_kfz', max_entfernung=200):


    """
    Berechnet ein Gewicht für jeden Standort basierend auf der Verkehrsstärke der nächstgelegenen Linie
    und der Entfernung zu dieser Linie. Gewicht wird auf Bereich [0, 1] normalisiert.

    Parameter:
    - standorte_gdf: GeoDataFrame mit Punkt-Geometrien
    - linien_gdf: GeoDataFrame mit Linien-Geometrien und Verkehrsstärke
    - verkehrs_spalte: Name der Spalte mit der Verkehrsstärke
    - max_entfernung: maximale Entfernung in Metern, ab der Punkte als „nicht angebunden“ gelten

    Rückgabe:
    - GeoDataFrame mit zusätzlichen Spalten: 'gewicht', 'entfernung_zur_linie', 'keine_linie_im_umkreis'
    """
    # Einheitliches Koordinatensystem
    standorte_gdf = standorte_gdf.to_crs(linien_gdf.crs)

    max_verkehr = linien_gdf[verkehrs_spalte].max()

    rohe_gewichte = []
    entfernungen = []
    markierungen = []
    verkehre = []

    for punkt in standorte_gdf.geometry:
        linien_gdf['distanz'] = linien_gdf.geometry.distance(punkt)
        naechste = linien_gdf.loc[linien_gdf['distanz'].idxmin()]
        entfernung = naechste['distanz']

        if entfernung > max_entfernung:
            rohe_gewichte.append(0)
            verkehre.append(0)
            entfernungen.append(entfernung)
            markierungen.append(True)
        else:
            verkehr = naechste[verkehrs_spalte]
            verkehr_norm = naechste[verkehrs_spalte] / max_verkehr
            entfernung_faktor = 1 - (entfernung / max_entfernung)
            gewicht = 0.8 * verkehr_norm + 0.2 * entfernung_faktor
            rohe_gewichte.append(gewicht)
            entfernungen.append(entfernung)
            markierungen.append(False)
            verkehre.append(verkehr)

    # Normalisierung auf Bereich [0, 1]
    min_wert = min(rohe_gewichte)
    max_wert = max(rohe_gewichte)
    normierte_gewichte = [
        0 if markierungen[i] else (gw - min_wert) / (max_wert - min_wert) if max_wert > min_wert else 1
        for i, gw in enumerate(rohe_gewichte)
    ]

    # Ergebnisse hinzufügen
    standorte_gdf['gewicht'] = normierte_gewichte
    standorte_gdf['entfernung_zur_linie'] = entfernungen
    standorte_gdf['verkehr'] = verkehre
    standorte_gdf['keine_linie_im_umkreis'] = markierungen

    return standorte_gdf


def rename_charging_locations():
    df = pd.read_excel("Ladestandorte_R4MU.xlsx")
    df["Beschreibung"] = "Dieser Standort wurde vom Retail4Multi-Use Projektteam erfasst und nicht vom Betreiber selbst hochgeladen - " + df["Beschreibung"].astype(str)
    df["Name"] = df["Name"].astype(str) + " - ohne Betreiberkontakt"
    df["Ladepunkte"] = df["Ladepunkte"].str.replace(r"=.*", "", regex=True)

    df["Ladepunkte"] = df["Ladepunkte"].apply(
        lambda s: s[:2] + "x" + s[3:] if isinstance(s, str) and len(s) > 2 else s)

    df.to_excel("Ladestandorte_R4MU_angepasst.xlsx")
    print("done")


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
    # df = pd.read_csv("results/_25-08-13_091910/output_retail_charging-events.csv", index_col=0)
    # df.loc[df["multi_use"], "charging_use_case"] = "retail_multi-use"
    # f.loc[df["charging_use_case"] == "urban_fast", "charging_use_case"] = "retail"
    # plot_occupation_of_charging_points(df, "retail", year, scenario)
    rename_charging_locations()