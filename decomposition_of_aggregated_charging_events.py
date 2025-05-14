import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime, timedelta
from datetime import timedelta
import random


# Beispielhafte Ladeparameter pro Use Case
ladeparameter = {
    "public": {"standdauer_h": 5.1, "leistung_kw": 22, "dauer_h": 2.1},
    "retail": {"standdauer_h": 6.2, "leistung_kw": 22, "dauer_h": 2.4},
    "other_private": {"standdauer_h": 5.5, "leistung_kw": 22, "dauer_h": 2.4},
    "home": {"standdauer_h": 6.2, "leistung_kw": 22, "dauer_h": 1.2},
    "depot": {"standdauer_h": 6.6, "leistung_kw": 22, "dauer_h": 1.6},
    "agrar/bau": {"standdauer_h": 5.3, "leistung_kw": 22, "dauer_h": 0.9},
    # Ergänze weitere Use-Cases hier...
}

# Lade CSV (angenommen: Index ist Zeitstempel, Spalten sind Use-Cases)
df = pd.read_csv("data/dlr_data/2045/aggregated_result_table_ppc_id_and_week_hour.csv", index_col=0, parse_dates=True)

lade_use_cases_def = pd.read_csv("data/dlr_data/2045/charging_stations_availability.csv", sep=";")

mapping = {
    101: "depot",
    102: "depot",
    103: "depot",
    104: "street",
    201: "street",
    301: "home",
    302: "home",
    401: "home",
    402: "retail",
    403: "street"
}

df["use_case"] = df["ppc_id"].map(mapping)

# Output-Liste für Events
ladeevents = []

# Für jeden Use-Case
for use_case in ladeparameter.keys():
    avg_dauer = ladeparameter[use_case]["dauer_h"]
    leistung = ladeparameter[use_case]["leistung_kw"]
    avg_standzeit = ladeparameter[use_case]["standdauer_h"]

    aktive_events = []  # Liste laufender Events (mit Endzeit)

    for index, row in df.loc[df["use_case"] == use_case].iterrows():

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
            dauer = np.random.normal(avg_dauer, 1)  # Std-Abw. von 0.3h
            dauer = max(0.5, dauer)  # Mindestdauer 0.5h
            standzeit = np.random.normal(avg_standzeit, 4)  # Std-Abw. von 0.3h
            standzeit = max(dauer, standzeit)  # Mindestdauer = Ladedauer

            endzeit = startzeit + dauer
            endstandzeit = startzeit + standzeit
            energie = dauer * leistung

            event = {
                "event_start": startzeit*4-3,
                "event_time": int((endstandzeit-startzeit)*4),
                "charge_end": int(endzeit*4-3),
                # "endzeit_parken": endstandzeit,
                "charging_use_case": use_case,
                # "station_charging_capacity": leistung,
                "station_charging_capacity": 22,
                "energy": energie,
                "average_charging_power": (energie / (endstandzeit - startzeit))
            }

            ladeevents.append(event)
            aktive_events.append(event)

# In DataFrame umwandeln und speichern
events_df = pd.DataFrame(ladeevents)
events_df.to_csv("data/dlr_data/results_decomposition/simulierte_ladeevents_2045.csv", index=False)
events_df.to_parquet("data/dlr_data/results_decomposition/simulierte_ladeevents_2045.parquet", index=False)

# test
print("start of test")
# sicherstellen, dass Zeitspalten datetime sind
#events_df["startzeit"] = pd.to_datetime(events_df["startzeit"])
#events_df["endzeit"] = pd.to_datetime(events_df["endzeit"])

# Zeitraster
start = math.floor(events_df["event_start"].min())
end = math.ceil(events_df["charge_end"].max())
zeitindex = list(range(start, end))
zeitindex_df = pd.DataFrame({'zeitindex': zeitindex})

# Zeitraster vorbereiten
timeline = pd.DataFrame(index=zeitindex_df["zeitindex"])
for use_case in events_df["charging_use_case"].unique():
    timeline[use_case] = 0

# Ladeevents einsortieren
for _, row in events_df.iterrows():
    ladezeiten_dt = pd.date_range(start=row["event_start"], end=row["charge_end"], freq="H", closed="left")
    ladezeiten = pd.DataFrame(list(range(int(round(row["event_start"], 0)), int(round(row["charge_end"]+1, 0)))))
    for zeit in ladezeiten[0]:
        if zeit in timeline.index:
            timeline.at[zeit, row["charging_use_case"]] += 1

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

timeline.to_csv("data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_2045.csv", index=False)
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
plt.savefig("data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_2045")

plt.close()

plt.figure(figsize=(12, 6))
plt.stackplot(timeline['Datum_Uhrzeit'],timeline['depot'], labels=use_cases)

plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
plt.xlabel("Zeit")
plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
plt.legend(title="Use-Case")
plt.grid(True)
plt.tight_layout()
plt.savefig("data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_2045")

plt.close()


plt.figure(figsize=(12, 6))
plt.stackplot(timeline['Datum_Uhrzeit'], werte, labels=use_cases)

plt.title("Gestapelte gleichzeitige Ladeevents pro Use-Case")
plt.xlabel("Zeit")
plt.ylabel("Anzahl gleichzeitig ladender Fahrzeuge")
plt.legend(title="Use-Case")
plt.grid(True)
plt.tight_layout()
plt.savefig("data/dlr_data/results_decomposition/simulierte_ladeevents_kumuliert_Uhrzeit_2045")
