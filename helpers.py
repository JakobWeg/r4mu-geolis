import pandas as pd
import os
from pathlib import Path
import geopandas as gpd
from shapely.ops import unary_union, polygonize

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
    boundary_gdf = gpd.read_file("data/Boundaries_Berlin_polygon.gpkg")

    # Überprüfen, ob die Grenze ein einzelnes Polygon ist, und sicherstellen, dass es validiert ist
    if boundary_gdf.geometry.unary_union.is_valid:
        boundary = boundary_gdf.geometry.unary_union
    else:
        raise ValueError("Die Grenze ist ungültig. Bitte überprüfen Sie die Eingabedaten.")
    # Lade die Punkte- und Grenzdaten
    print("loading points")
    points_gdf = gpd.read_file("data/374-all_buildings.gpkg",
                                   engine='pyogrio', use_arrow=True)
    # Filtere Punkte, die innerhalb der Grenze liegen
    points_within_boundary = points_gdf[points_gdf.geometry.within(boundary)]

    # Speichere das gefilterte Ergebnis als GeoPackage
    points_within_boundary.to_file("data/374-all_buildings_berlin.gpkg", driver='GPKG')
    print(f"Gefilterte Punkte erfolgreich in data gespeichert.")


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
        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
        if idx % 1000 == 0:
            print(f"Iteration {idx} erreicht.")

    # Speichern als Parquet
    print(f"Speichere kombinierte Daten als Parquet: {output_file}")
    combined_df.to_parquet(output_file, index=False)
    print("Fertig!")

# Beispielnutzung
if __name__ == "__main__":
    # charging_events = pd.read_parquet("combined_charging_events.parquet")
    # print(charging_events)
    combine_csv_to_parquet("//FS01/RedirectedFolders/Jakob.Wegner/Desktop/r4mu_übergabe/2035/default_2024-10-28_141930_simbev_run/SR_Metro", "combined_charging_events.parquet")
    # filter_points_within_boundary()
    # merge_geometries_to_polygon()