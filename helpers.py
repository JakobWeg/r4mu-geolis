import pandas as pd
import os
from pathlib import Path
import geopandas as gpd
import numpy as np
from shapely.ops import unary_union, polygonize
from shapely.geometry import Point
from sklearn.cluster import DBSCAN
#from osgeo import ogr
import fiona
from shapely.geometry import shape, mapping

def cluster_public_data(eps=0.001, min_samples=1):
    """
    Create charging locations from POIs using clustering.

    Parameters:
        pois_gdf (gpd.GeoDataFrame): GeoDataFrame containing POIs with a geometry column.
        eps (float): Maximum distance between points to form a cluster (in coordinate units).
        min_samples (int): Minimum number of points to form a cluster.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame of charging locations with weights based on cluster sizes.
    """
    # Ensure POIs have a geometry column

    #points_gdf = gpd.read_file("data/poi_cluster_berlin.gpkg")
    points_gdf = gpd.read_file("data/teo_data/osm_buildings_residential_cts_berlin.gpkg")

    if "geometry" not in points_gdf.columns:
        raise ValueError("POIs GeoDataFrame must contain a 'geometry' column.")

    # Extract coordinates from the geometry column
    coords = np.array([[point.x, point.y] for point in points_gdf.geometry])

    # Perform DBSCAN clustering
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(coords)

    # Add cluster labels to the POIs GeoDataFrame
    points_gdf["cluster"] = clustering.labels_

    # Filter out noise points (label = -1)
    clustered_pois = points_gdf[points_gdf["cluster"] != -1]

    # Group by cluster and calculate cluster centroids and weights
    cluster_groups = clustered_pois.groupby("cluster").geometry.apply(lambda x: x.unary_union.centroid)
    cluster_weights = clustered_pois.groupby("cluster").size()

    # Create a GeoDataFrame for charging locations
    charging_locations = gpd.GeoDataFrame({
        "geometry": cluster_groups,
        "weight": cluster_weights
    }, crs=points_gdf.crs).reset_index(drop=True)

    charging_locations.to_file("data/home_street_cluster_new.gpkg", driver='GPKG')
    print(f"Gefilterte Punkte erfolgreich in data gespeichert.")

    return charging_locations


def merge_geometries_to_polygon():
    """
    Converts multiple geometries in a GeoDataFrame into a single polygon.

    :param geo_df: GeoDataFrame containing the geometries to merge.
    :param output_path: Optional path to save the resulting polygon as a GeoPackage or other file formats.
    :return: A GeoDataFrame with a single polygon.
    """
    output_path = "data/Boundaries_Berlin_polygon.gpkg"

    geo_df = gpd.read_file("data/Boundaries_Berlin.gpkg")

    # Merge all geometries into one (union of all geometries)
    merged_geometry = unary_union(geo_df.geometry)

    # Convert merged geometry to a polygon (if it's a set of LineStrings/MultiLineStrings)
    polygons = list(polygonize(merged_geometry))

    if len(polygons) > 1:
        print(f"Warning: The merged geometry resulted in multiple polygons. Using the largest.")
        # Optionally handle multiple polygons (e.g., take the largest)
        polygon = max(polygons, key=lambda p: p.area)
    elif polygons:
        polygon = polygons[0]
    else:
        raise ValueError("Failed to create a valid polygon from the input geometries.")

    # Create a new GeoDataFrame for the polygon
    polygon_gdf = gpd.GeoDataFrame(geometry=[polygon], crs=geo_df.crs)

    # Optionally save the resulting polygon to a file
    if output_path:
        polygon_gdf.to_file(output_path, driver="GPKG")
        print(f"Polygon saved to {output_path}")

    return polygon_gdf


def filter_points_within_boundary():
    """
    Filtert alle Punkte eines Datensatzes, die innerhalb einer umschlossenen Grenze liegen,
    und speichert das Ergebnis als GeoPackage.

    :param input_points_path: Pfad zur GeoJSON-, Shapefile- oder GeoPackage-Datei mit Punkten.
    :param boundary_path: Pfad zur GeoJSON-, Shapefile- oder GeoPackage-Datei mit der Grenze (Polygon).
    :param output_path: Pfad, um den gefilterten Datensatz als GeoPackage zu speichern.
    """
    # Lade die Punkte- und Grenzdaten
    print("loading boundaries")
    boundary_gdf = gpd.read_file("data_stralsund/Admin_Level_8_Stralsund_Polygon.gpkg")
    boundary_gdf = boundary_gdf.to_crs(3035)

    # Überprüfen, ob die Grenze ein einzelnes Polygon ist, und sicherstellen, dass es validiert ist
    if boundary_gdf.geometry.unary_union.is_valid:
        boundary = boundary_gdf.geometry.unary_union
    else:
        raise ValueError("Die Grenze ist ungültig. Bitte überprüfen Sie die Eingabedaten.")
    # Lade die Punkte- und Grenzdaten
    print("loading points")
    points_gdf = gpd.read_file(r"C:/Users/jakob.wegner/OneDrive - Reiner Lemoine Institut gGmbH/Dokumente/02_QGIS\R4MU/teo_geodaten/374_demand_data2/mobi_data2/osm_buildings_residential_cts.gpkg",
                               engine='pyogrio', use_arrow=True)
    points_gdf = points_gdf.to_crs(3035)
    # Filtere Punkte, die innerhalb der Grenze liegen
    points_within_boundary = points_gdf[points_gdf.geometry.within(boundary)]

    # Speichere das gefilterte Ergebnis als GeoPackage
    points_within_boundary.to_file("data_stralsund/osm_buildungs_residential_cts_stralsund.gpkg", driver='GPKG')
    print(f"Gefilterte Punkte erfolgreich in data gespeichert.")

    # # Öffne das Geopackage mit dem Polygon
    # polygon_ds = ogr.Open("data/Boundaries_Berlin_polygon.gpkg")
    # polygon_layer = polygon_ds
    #
    # # Nimm das erste Polygon aus dem Layer (du kannst auch über mehrere iterieren)
    # polygon_feature = polygon_layer.GetNextFeature()
    # polygon_geom = polygon_feature.GetGeometryRef()
    #
    # # Öffne den Punktlayer (kann auch im selben GPKG sein)
    # points_ds = ogr.Open("data/Boundaries_Berlin_polygon.gpkg")
    # points_layer = points_ds.GetLayerByName("punkte_layername")
    #
    # # Schleife durch Punkte und prüfe, ob sie im Polygon liegen
    # for point_feature in points_layer:
    #     point_geom = point_feature.GetGeometryRef()
    #     if point_geom.Within(polygon_geom):
    #         print("Treffer:", point_geom.ExportToWkt())

# # Eingabedateien
# polygon_path = "data/Boundaries_Berlin_polygon.gpkg"
#
# punkte_path = "data/teo_data/2025_03_28_new/osm_buildings_residential_cts.gpkg"
#
#
# # Ausgabedatei
# output_path = "data/teo_data/data/teo_data/osm_buildings_residential_cts_berlin_new.gpkg"
#
#
# # Lade erstes Polygon aus dem Polygon-Layer
# with fiona.open(polygon_path) as polygon_src:
#     polygon_geom = shape(polygon_src['geometry'])
#     crs = polygon_src.crs  # Koordinatensystem für spätere Ausgabe
#
# # Lade Punkte und filtere
# gefilterte_punkte = []
# with fiona.open(punkte_path) as point_src:
#     schema = point_src.schema  # Geometrie- und Attributschema übernehmen
#
#     for feature in point_src:
#         punkt_geom = shape(feature['geometry'])
#         if punkt_geom.within(polygon_geom):
#             gefilterte_punkte.append({
#                 'geometry': mapping(punkt_geom),
#                 'properties': feature['properties']
#             })
#
# # Speichere gefilterte Punkte in ein neues Geopackage
# with fiona.open(
#     output_path,
#     mode='w',
#     driver='GPKG',
#     schema=schema,
#     crs=crs
# ) as output:
#     for punkt in gefilterte_punkte:
#         output.write(punkt)
#
# print(f"✅ {len(gefilterte_punkte)} Punkte wurden gespeichert in '{output_path}'")


def combine_csv_to_parquet(csv_folder_path, output_file):
    """
    Liest eine große Anzahl von CSV-Dateien mit denselben Spalten ein und kombiniert sie in eine große Tabelle.
    Die resultierende Tabelle wird im Parquet-Format gespeichert.

    :param csv_folder_path: Pfad zum Ordner, der die CSV-Dateien enthält.
    :param output_file: Pfad zur Ausgabedatei im Parquet-Format.
    """
    combined_df = pd.DataFrame()
    idx = 0
    print("starting...")

    if not os.path.isdir(csv_folder_path):
        print(f"Der Ordner {csv_folder_path} existiert nicht.")
        return 0

    # for file_name in os.listdir(csv_folder_path):
    #     idx+=1
    #     print(f"Lese Datei: {file_name}")
    #     # Lese die CSV-Datei ohne Header (ignoriere den Header)
    #     temp_df = pd.read_csv(file_name, header=0)
    #     combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
    #     if idx % 5000 == 0:
    #         print(f"Iteration {idx} erreicht.")

    # Iteriere durch alle CSV-Dateien im angegebenen Ordner
    for idx, csv_file in enumerate(Path(csv_folder_path).glob("*.csv")):
        # idx+=1
        # print(f"Lese Datei: {csv_file}")
        # Lese die CSV-Datei ohne Header (ignoriere den Header)
        temp_df = pd.read_csv(csv_file, header=0)
        temp_df = temp_df.loc[temp_df["station_charging_capacity"] != 0]
        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

        if idx % 1000 == 0:
            print(f"Iteration {idx} erreicht.")

    # Speichern als Parquet
    print(f"Speichere kombinierte Daten als Parquet: {output_file}")
    combined_df.to_parquet(output_file, index=False)
    print("Fertig!")

def convert_geodata_for_uc_work(landusepath, alkispath):
    print("converting_geodata_for_uc_work")
    landuse_gdf = gpd.read_file(landusepath)# .to_crs(3035)
    alkis_gdf = gpd.read_file(alkispath)# .to_crs(3035)
    landuse_work = landuse_gdf.loc[landuse_gdf['nutzung'].isin(['Gewerbe- und Industrienutzung, großflächiger Einzelhandel', 'Mischnutzung'])]
    alkis_keys_work = ['Fabrik', 'Lagerhalle, Lagerschuppen, Lagerhaus', b'Geb\xe4ude f\xfcr Gewerbe und Industrie',
                       'Seniorenheim', b'Land- und forstwirtschaftliches Betriebsgeb\xe4ude', 'Laden',
                       b'Geb\xe4ude zur Freizeitgestaltung', b'Geb\xe4ude f\xfcr soziale Zwecke', 'Einkaufszentrum',
                       b'Geb\xe4ude zur Versorgung', b'Freizeit- und Vergn\xfcgungsst\xe4tte',
                       'Heilanstalt, Pflegeanstalt, Pflegestation', b'Kinderkrippe, Kindergarten, Kindertagesst\xe4tte',
                       'Hotel, Motel, Pension', 'Rathaus', 'Tankstelle', b'Geb\xe4ude f\xfcr Gesundheitswesen',
                       'Justizvollzugsanstalt', b'Sonstiges Geb\xe4ude f\xfcr Gewerbe und Industrie',
                       b'B\xfcrogeb\xe4ude', 'Krankenhaus', b'Betriebsgeb\xe4ude f\xfcr Schienenverkehr', 'Feuerwehr',
                       'Gemeindehaus', b'Geb\xe4ude f\xfcr Sicherheit und Ordnung',
                       b'Wohngeb\xe4ude mit Gewerbe und Industrie', b'Geb\xe4ude f\xfcr Handel und Dienstleistungen',
                       'Kreditinstitut', b'Verwaltungsgeb\xe4ude',
                       b'Geb\xe4ude f\xfcr Gewerbe und Industrie mit Wohnen', b'Gesch\xe4ftsgeb\xe4ude', 'Messehalle',
                       b'Geb\xe4ude f\xfcr Bildung und Forschung',
                       b'Hochschulgeb\xe4ude (Fachhochschule, Universit\xe4t)', 'Botschaft, Konsulat', 'Theater, Oper',
                       b'Geb\xe4ude f\xfcr Handel und Dienstleistung mit Wohnen', 'Polizei', 'Versicherung',
                       b'Gemischt genutztes Geb\xe4ude mit Wohnen', 'Kaserne', 'Kaufhaus', 'Forschungsinstitut',
                       'Berufsbildende Schule', b'Speditionsgeb\xe4ude', b'Geb\xe4ude f\xfcr Forschungszwecke',
                       'Gericht', b'Stra\xdfenmeisterei', 'Rundfunk, Fernsehen', b'Flughafengeb\xe4ude', 'Zollamt'
                       ]
    alkis_work = alkis_gdf.loc[alkis_gdf["bezgfk"].isin(alkis_keys_work)]
    alkis_work["area"] = alkis_work.area
    alkis_work['centroid'] = alkis_work.geometry.centroid  # Zentroid berechnen (optional, wenn du die Originalgeometrien behalten willst)
    alkis_work.set_geometry('centroid', inplace=True)
    alkis_work = alkis_work.drop(columns=["geometry"])

    landuse_work["area"] = landuse_work.area
    landuse_work['centroid'] = landuse_work.geometry.centroid  # Zentroid berechnen (optional, wenn du die Originalgeometrien behalten willst)
    landuse_work.set_geometry('centroid', inplace=True)
    landuse_work = landuse_work.drop(columns=["geometry"])

    for col in alkis_work.select_dtypes(include='object').columns:
        alkis_work[col] = alkis_work[col].astype(str)

    alkis_work.to_file("data/work_points_alkis.gpkg", driver='GPKG')
    landuse_work.to_file("data/work_points_landuse.gpkg", driver='GPKG')
    print(f"Gefilterte Punkte erfolgreich in data gespeichert.")

def convert_geodata_for_uc_street(landusepath, alkispath):
    print("converting_geodata_for_uc_street")

def convert_geodata_for_uc_retail(path):
    retail_gdf = gpd.read_file(path)
    columns = ['id_0', 'osm_id', 'osm_way_id', 'area',
       'category', 'geometry']
    retail_gdf = retail_gdf.loc[:,columns]
    retail_gdf["area"] = retail_gdf.area
    retail_gdf['centroid'] = retail_gdf.geometry.centroid  # Zentroid berechnen (optional, wenn du die Originalgeometrien behalten willst)
    retail_gdf.set_geometry('centroid', inplace=True)
    retail_gdf = retail_gdf.drop(columns=["geometry"])

    retail_gdf.to_file("data/retail_parking_lots_points.gpkg", driver='GPKG')
    print(f"Gefilterte Punkte erfolgreich in data gespeichert.")

def calculate_share_of_public_home_charging():
    print("calculate")

    ts_path = Path("scenario/Ladeprofile_Privatverkehr_parquet")

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

    charging_events_street = charging_events.loc[charging_events["charging_use_case"] == "street"]

    charging_events_street_home = charging_events_street.loc[charging_events_street["location"] == "home"]

    share_street_home_charging = len(charging_events_street_home)/len(charging_events_street)

    share_street_not_at_home_charging = len(charging_events_street.loc[charging_events_street["location"] != "home"])/len(charging_events_street)

    print("share_street while beeing at home:", share_street_home_charging, share_street_not_at_home_charging)

# Beispielnutzung
if __name__ == "__main__":
    # charging_events = pd.read_parquet("combined_charging_events.parquet")
    # print(charging_events)
    # combine_csv_to_parquet(r"C:\Users\jakob.wegner\PycharmProjects\simbev\scenarios\r4mu_stralsund_2035\results\default_2025-11-20_161319_simbev_run\LR_Zentr",
    #                        "scenario/Ladeprofile_Privatverkehr_stralsund/combined_charging_events_2035_stralsund.parquet")
    # convert_geodata_for_uc_work(landusepath="data/Reale_Nutzung_2021_Umweltatlas.gpkg", alkispath="data/ALKIS_Berlin_Gebäude.gpkg")
    filter_points_within_boundary()
    # merge_geometries_to_polygon()
    # cluster_public_data()
    # convert_geodata_for_uc_retail("data/Retailer_parking_lots.gpkg")
    #calculate_share_of_public_home_charging()