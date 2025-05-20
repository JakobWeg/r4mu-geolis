import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime, timedelta
from datetime import timedelta
import random

year = 2045

test = True


print("year:", year)

# Ladeparameter pro Use Case
# ladeparameter = {
#     "public": {"standdauer_h": 5.1, "leistung_kw": 22, "dauer_h": 2.1},
#     "retail": {"standdauer_h": 6.2, "leistung_kw": 22, "dauer_h": 2.4},
#     "other_private": {"standdauer_h": 5.5, "leistung_kw": 22, "dauer_h": 2.4},
#     "home": {"standdauer_h": 6.2, "leistung_kw": 22, "dauer_h": 1.2},
#     "depot": {"standdauer_h": 6.6, "leistung_kw": 22, "dauer_h": 1.6},
#     "agrar/bau": {"standdauer_h": 5.3, "leistung_kw": 22, "dauer_h": 0.9},
#     # Ergänze weitere Use-Cases hier...
# }

ladeparameter = {
    101: {"standdauer_h": 6.8, "leistung_kw": 22, "dauer_h": 1.1},
    102: {"standdauer_h": 6.9, "leistung_kw": 22, "dauer_h": 2.4},
    103: {"standdauer_h": 3.1, "leistung_kw": 22, "dauer_h": 0.7},
    104: {"standdauer_h": 6.7, "leistung_kw": 22, "dauer_h": 1.7},
    201: {"standdauer_h": 5.3, "leistung_kw": 22, "dauer_h": 0.9},
    301: {"standdauer_h": 7.0, "leistung_kw": 22, "dauer_h": 0.9},
    302: {"standdauer_h": 5.4, "leistung_kw": 22, "dauer_h": 1.4},
    401: {"standdauer_h": 5.5, "leistung_kw": 22, "dauer_h": 2.4},
    402: {"standdauer_h": 6.2, "leistung_kw": 22, "dauer_h": 2.4},
    403: {"standdauer_h": 5.1, "leistung_kw": 22, "dauer_h": 2.1},
    # Ergänze weitere Use-Cases hier...
}

# Lade CSV (angenommen: Index ist Zeitstempel, Spalten sind Use-Cases)
df = pd.read_csv(f"data/dlr_data/{year}/aggregated_result_table_ppc_id_and_week_hour.csv", index_col=0, parse_dates=True)

lade_use_cases_def = pd.read_csv(f"data/dlr_data/{year}/charging_stations_availability.csv", sep=";")

# mapping = {
#     101: "depot",
#     102: "depot",
#     103: "depot",
#     104: "street",
#     201: "street",
#     301: "street",
#     302: "street",
#     401: "home",
#     402: "retail",
#     403: "street"
# }
# mapping = {
#     101: "depot",
#     102: "depot",
#     103: "depot",
#     104: "street",
#     201: "street",
#     301: "street",
#     302: "street",
#     401: "home",
#     402: "retail",
#     403: "street"
# }
#
#
# df["use_case"] = df["ppc_id"].map(mapping)

use_cases = ["street", "street", "retail","depot"]

# Output-Liste für Events
ladeevents = []

# todo Mit Aron klären, ob ein Ladeveent immer die ganze Standzeit abdeckt.

# Für jeden Use-Case
for use_case_id in ladeparameter.keys():
    avg_dauer = ladeparameter[use_case_id]["dauer_h"]
    leistung = ladeparameter[use_case_id]["leistung_kw"]
    avg_standzeit = ladeparameter[use_case_id]["standdauer_h"]

    aktive_events = []  # Liste laufender Events (mit Endzeit)

    for index, row in df.loc[df["ppc_id"] == use_case_id].iterrows():

        zeitpunkt = row["weekhour"]
        anzahl_ladevorgaenge = row["charging_vehicles"]
        # Entferne beendete Ladeevents
        aktive_events = [e for e in aktive_events if (e["event_start"]+e["event_time"]) > zeitpunkt]

        aktuelle_anzahl = len(aktive_events)
        differenz = int(anzahl_ladevorgaenge - aktuelle_anzahl)

        # Wenn neue Events benötigt werden
        for _ in range(differenz):
            startzeit = zeitpunkt
            # Leicht zufällige Dauer um Verteilung zu erzeugen
            dauer = np.random.normal(avg_dauer, 1)  # Std-Abw. von 0.3h 1
            dauer = max(0.5, dauer)  # Mindestdauer 0.5h
            standzeit = np.random.normal(avg_standzeit, 4)  # Std-Abw. von 0.3h 4
            standzeit = max(dauer, standzeit)  # Mindestdauer = Ladedauer

            endzeit = startzeit + dauer
            endstandzeit = startzeit + standzeit
            energie = dauer * leistung

            event = {
                "event_start": startzeit, # *4-3,
                "event_time": endstandzeit-startzeit, # *4),
                "charge_end": endzeit, # *4-3),
                "event_end_parking": endstandzeit,
                "ppc_id": use_case_id,
                # "station_charging_capacity": leistung,
                "station_charging_capacity": 22,
                "energy": energie,
                "average_charging_power": (energie / (endstandzeit - startzeit))
            }

            ladeevents.append(event)
            aktive_events.append(event)



# In DataFrame umwandeln und speichern
events_df = pd.DataFrame(ladeevents)

mapping = {
    101: "depot",
    102: "depot",
    103: "depot",
    104: "street",
    201: "street",
    301: "home", # 302 war in Arons übersicht nicht mit drinnen
    302: "street",
    401: "home",
    402: "retail",
    403: "street"
}

events_df["use_case"] = events_df["ppc_id"].map(mapping)

# norm data from hours to 15 min timesteps
events_df_timesteps = events_df.copy()
events_df_timesteps["event_start"] = (events_df_timesteps["event_start"]*4-3).astype(int)
events_df_timesteps["event_time"] = (events_df_timesteps["event_time"]*4).astype(int)
events_df_timesteps["charge_end"] = (events_df_timesteps["charge_end"]*4-3).astype(int)
events_df_timesteps["event_end_parking"] = (events_df_timesteps["event_end_parking"]*4-3).astype(int)

events_df_timesteps.to_csv(f"data/dlr_data/results_decomposition/simulierte_ladeevents_{year}.csv", index=False)
events_df_timesteps.to_parquet(f"data/dlr_data/results_decomposition/simulierte_ladeevents_{year}.parquet", index=False)


if test:
    # test
    print("start of test")
    # sicherstellen, dass Zeitspalten datetime sind
    #events_df["startzeit"] = pd.to_datetime(events_df["startzeit"])
    #events_df["endzeit"] = pd.to_datetime(events_df["endzeit"])

    # Zeitraster
    start = math.floor(events_df["event_start"].min())
    end = math.ceil(events_df["event_end_parking"].max())
    zeitindex = list(range(start, end))
    zeitindex_df = pd.DataFrame({'zeitindex': zeitindex})

    # Zeitraster vorbereiten
    timeline = pd.DataFrame(index=zeitindex_df["zeitindex"])
    for use_case in events_df["use_case"].unique():
        timeline[use_case] = 0

    # Ladeevents einsortieren
    for _, row in events_df.iterrows():
        ladezeiten_dt = pd.date_range(start=row["event_start"], end=row["event_end_parking"], freq="H", closed="left")
        ladezeiten = pd.DataFrame(list(range(int(round(row["event_start"], 0)), int(round(row["event_end_parking"]+1, 0)))))
        for zeit in ladezeiten[0]:
            if zeit in timeline.index:
                timeline.at[zeit, row["use_case"]] += 1

    use_cases = timeline.columns.tolist()

    # add day and time
    # Starte Montag 00:00 Uhr
    startzeit_date = datetime.strptime("2024-01-01 00:00", "%Y-%m-%d %H:%M")  # 1. Jan. 2024 ist ein Montag

    # Erstelle eine neue Spalte mit Datum + Uhrzeit
    times = pd.Series(timeline.index.values)
    timeline['Datum_Uhrzeit'] = times.apply(lambda h: startzeit_date + timedelta(hours=h-2))

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
    timeline['Wochentag_Uhrzeit'] = timeline['Datum_Uhrzeit'].dt.strftime('%A %H:%M').replace(wochentage_de, regex=True)

    timeline.to_csv(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_2035{year}.csv", index=False)
    # Daten für Stackplot vorbereiten
    #use_cases = timeline.columns.tolist()
    werte = [timeline[uc].values for uc in use_cases]

    # Gestapelter Linien-Plot (stackplot = gestapelte Flächen)
    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline.index, werte, labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeit")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{year}")

    plt.close()

    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline['Datum_Uhrzeit'],timeline['depot'], labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeit")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_{year}")

    plt.close()


    plt.figure(figsize=(12, 6))
    plt.stackplot(timeline['Datum_Uhrzeit'], werte, labels=use_cases)

    plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
    plt.xlabel("Zeit")
    plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
    plt.legend(title="Use-Case")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_Uhrzeit_{year}")
