import geopandas as gpd
import folium
from folium.plugins import TimestampedGeoJson
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import os

def visualisation_cumulated_charging_events():
    import pandas as pd
    import geopandas as gpd
    import matplotlib.pyplot as plt
    import numpy as np
    import os

    # 1. Lade alle Datensätze und verbinde sie in einem großen GeoDataFrame
    directory = "results/3_Mehrfachnutzung_Flex_2045/"
    # charging_use_cases = ["home_detached", "home_apartment", "work", "hpc", "retail", "public", "depot"]
    charging_use_cases = ["retail", "public"]
    pfade = [os.path.join(directory, f"output_{use_case}_charging-events.gpkg") for use_case in charging_use_cases]

    alle_gdfs = []
    ziel_crs = "EPSG:4326"
    for pfad in pfade:
        gdf = gpd.read_file(pfad)
        if gdf.crs != ziel_crs:
            gdf = gdf.to_crs(ziel_crs)
        alle_gdfs.append(gdf)

    # 2. Zusammenführen in einem DataFrame
    gesamt_gdf = pd.concat(alle_gdfs, ignore_index=True)

    # 3. Die Spalten 'event_start' und 'event_time' als Zeitschritte interpretieren
    # Angenommen: 'event_start' und 'event_time' sind bereits in Zeitschritten angegeben!
    # (z.B. event_start=0, event_time=4 → Start=0, Ende=4)
    # Du musst sicherstellen, dass diese Spalten in den GeoPackages **ohne** Zeitstempel sind!

    # Falls noch nicht Integer, konvertiere:
    gesamt_gdf["event_start"] = gesamt_gdf["event_start"].astype(int)
    gesamt_gdf["event_time"] = gesamt_gdf["event_time"].astype(int)

    # Event-Endzeitpunkt (exklusiv)
    gesamt_gdf["event_end"] = gesamt_gdf["event_start"] + gesamt_gdf["event_time"]

    # 4. Zeitschritte berechnen
    max_step = gesamt_gdf["event_end"].max()
    zeitschritte = np.arange(0, max_step + 1)

    # 5. Lade-Use-Cases: aktive Ladeevents pro Zeitschritt zählen
    lade_use_cases = gesamt_gdf["charging_use_case"].unique()
    aktiv_counts = pd.DataFrame(index=zeitschritte, columns=lade_use_cases).fillna(0)

    for use_case in lade_use_cases:
        gdf_uc = gesamt_gdf[gesamt_gdf["charging_use_case"] == use_case]
        for _, event in gdf_uc.iterrows():
            # Zeitschritte von start (inkl.) bis end (exkl.)
            aktiv_counts.loc[event["event_start"]:event["event_end"] - 1, use_case] += 1

    print("--- start csvs erstellen ---")
    # 6. CSV-Export für jeden Lade-Use-Case
    for use_case in lade_use_cases:
        csv_name = f"{use_case.replace(' ', '_')}_zeitreihe.csv"
        csv_pfad = os.path.join(directory, csv_name)
        df_use_case = pd.DataFrame({
            "zeitschritt": aktiv_counts.index,
            "anzahl_aktiver_ladeevents": aktiv_counts[use_case]
        })
        df_use_case.to_csv(csv_pfad, index=False)
        print(f"CSV für {use_case} gespeichert: {csv_pfad}")

    # 7. Plot: nur Zeitschritte auf X-Achse
    plt.figure(figsize=(14, 8))
    for use_case in lade_use_cases:
        plt.plot(aktiv_counts.index, aktiv_counts[use_case], label=use_case)

    plt.xlabel("Zeitschritt (0=Mo 0:00, 1=0:15, ...)")
    plt.ylabel("Anzahl aktiver Ladeevents")
    plt.title("Anzahl aktiver Ladeevents pro Lade-Use-Case (15-min-Zeitschritte, 0-basiert)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def visualisation_cumulated_charging_events_alt():
    # 1. Lade alle 8 Datensätze und verbinde sie in einem großen GeoDataFrame
    # Angenommen: alle liegen in einem Ordner und heißen "ladeevents1.gpkg", "ladeevents2.gpkg", etc.

    directory = "results/1_Ref_2035/"

    charging_use_cases = ["home_detached", "home_apartment", "work", "hpc", "retail", "public", "depot"]
    pfade = []
    for use_case in charging_use_cases:
        pfade.append(
            directory + f"output_{use_case}_charging-events.gpkg")

    alle_gdfs = []
    ziel_crs = "EPSG:4326"
    for pfad in pfade:
        gdf = gpd.read_file(pfad)
        # In Ziel-CRS umprojizieren
        if gdf.crs != ziel_crs:
            gdf = gdf.to_crs(ziel_crs)

        alle_gdfs.append(gdf)

    # Zusammenführen in einem DataFrame
    gesamt_gdf = pd.concat(alle_gdfs, ignore_index=True)

    # 2. Zeit in datetime konvertieren
    gesamt_gdf["event_start"] = pd.to_datetime(gesamt_gdf["event_start"])
    gesamt_gdf["event_time"] = pd.to_timedelta(gesamt_gdf["event_time"], unit="m")  # Minuten zu timedelta

    # 3. Event-Endzeit berechnen
    gesamt_gdf["event_end"] = gesamt_gdf["event_start"] + gesamt_gdf["event_time"]

    # 4. Zeitschritte alle 15 Minuten erzeugen
    start_zeit = gesamt_gdf["event_start"].min().floor("15T")
    end_zeit = gesamt_gdf["event_end"].max().ceil("15T")
    zeitpunkte = pd.date_range(start=start_zeit, end=end_zeit, freq="15T")

    # 5. Für jeden Lade-Use-Case: aktive Ladeevents pro Zeitschritt zählen
    lade_use_cases = gesamt_gdf["charging_use_case"].unique()
    aktiv_counts = pd.DataFrame(index=zeitpunkte, columns=lade_use_cases).fillna(0)

    for use_case in lade_use_cases:
        gdf_uc = gesamt_gdf[gesamt_gdf["charging_use_case"] == use_case]
        for _, event in gdf_uc.iterrows():
            # alle Zeitpunkte, an denen dieses Event aktiv ist
            aktive_zeitpunkte = zeitpunkte[(zeitpunkte >= event["event_start"]) & (zeitpunkte < event["event_end"])]
            aktiv_counts.loc[aktive_zeitpunkte, use_case] += 1

    # for use_case in lade_use_cases:
        csv_name = f"{use_case.replace(' ', '_')}_zeitreihe.csv"
        csv_pfad = os.path.join(directory, csv_name)
        # DataFrame mit zwei Spalten: Zeitschritt und Anzahl aktiver Events
        df_use_case = pd.DataFrame({
            "zeitschritt": aktiv_counts.index,
            "anzahl_aktiver_ladeevents": aktiv_counts[use_case]
        })
        df_use_case.to_csv(csv_pfad, index=False)
        print(f"CSV für {use_case} gespeichert: {csv_name}")

    # 6. Plot: Zeitreihe der aktiven Ladeevents je Use-Case
    plt.figure(figsize=(14, 8))
    for use_case in lade_use_cases:
        plt.plot(aktiv_counts.index, aktiv_counts[use_case], label=use_case)

    plt.xlabel("Zeit")
    plt.ylabel("Anzahl aktiver Ladeevents")
    plt.title("Anzahl aktiver Ladeevents pro Lade-Use-Case (15-min-Zeitschritte)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def create_dynamic_map_multiple_sources(sources, base_time=datetime(2024, 1, 1, 0, 0)):
    """
    Erstellt eine dynamische Karte mit mehreren Datensätzen.

    Parameters:
    - sources: Liste von Dictionaries, z.B.:
        [
            {
                'gpkg': 'daten1.gpkg',
                'layer': 'layer_a',
                'color': 'red',
                'start_col': 'start',
                'end_col': 'end'
            },
            {
                'gpkg': 'daten2.gpkg',
                'layer': 'layer_b',
                'color': 'green'
            }
        ]
    - base_time: Startzeitpunkt der Woche (default: Montag 2024-01-01 00:00)
    """

    all_features = []

    for source in sources:
        gpkg = source['gpkg']
        layer = source['layer']
        color = source.get('color', 'blue')
        start_col = source.get('start_col', 'event_start')
        time_col = source.get('end_col', 'event_time')

        gdf = gpd.read_file(gpkg)# , layer=layer)
        gdf = gdf.to_crs(epsg=4326)

        for _, row in gdf.iterrows():
            lon, lat = row.geometry.x, row.geometry.y
            start_time = base_time + timedelta(minutes=15 * int(row[start_col]))
            end_time = base_time + timedelta(minutes=15 * int(row[start_col] + row[time_col]))

            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lon, lat],
                },
                'properties': {
                    'times': [start_time.isoformat(), end_time.isoformat()],
                    'style': {
                        'color': color,
                        'fillColor': color,
                        'radius': 1,
                        'weight': 1,
                        'fillOpacity': 0.8
                    },
                    'icon': 'circle',
                    'popup': f"{layer}: {start_time.strftime('%a %H:%M')} – {end_time.strftime('%a %H:%M')}"
                }
            }

            all_features.append(feature)

    geojson = {
        'type': 'FeatureCollection',
        'features': all_features
    }

    m = folium.Map(location=[52.52, 13.405], zoom_start=11)

    TimestampedGeoJson(
        geojson,
        period='PT60M',
        add_last_point=False,
        transition_time=100,
        loop=False,
        auto_play=False,
        max_speed=50,
        loop_button=True,
        date_options='YYYY-MM-DD HH:mm',
        time_slider_drag_update=True
    ).add_to(m)

    return m


sources = [
    {
        'gpkg': 'results/_25-05-19_185128/output_hpc_charging-events.gpkg',
        'layer': 'privat',
        'color': 'blue'
    },
    # {
    #     'gpkg': 'ladeevents_e_auto.gpkg',
    #     'layer': 'gewerblich',
    #     'color': 'orange'
    # },
    # {
    #     'gpkg': 'schnellladestationen.gpkg',
    #     'layer': 'dc_fast',
    #     'color': 'red',
    #     'start_col': 'von',
    #     'end_col': 'bis'
    # }
]

# karte = create_dynamic_map_multiple_sources(sources)
# karte.save("berlin_ladeevents_mehrfarbig.html")

visualisation_cumulated_charging_events()