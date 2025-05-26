import geopandas as gpd
import folium
from folium.plugins import TimestampedGeoJson
from datetime import datetime, timedelta


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

karte = create_dynamic_map_multiple_sources(sources)
karte.save("berlin_ladeevents_mehrfarbig.html")