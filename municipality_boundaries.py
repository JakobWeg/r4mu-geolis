"""Download and prepare German municipality (Gemeinde) boundaries from the
BKG VG250 open data product, used to spatially restrict data_DE candidate
locations to a single Gemeinde in the DE-wide pipeline (run_de.py).
"""
import pathlib
import shutil
import urllib.request
import zipfile

import geopandas as gpd
import utility

VG250_URL = "https://daten.gdz.bkg.bund.de/produkte/vg/vg250_ebenen_0101/aktuell/vg250_01-01.utm32s.gpkg.ebenen.zip"
VG250_LAYER = "vg250_gem"
TARGET_CRS = 3035


def download_vg250(target_path: pathlib.Path, download_dir: pathlib.Path = None) -> pathlib.Path:
    """Download the VG250 Gemeinden layer and store a slim (AGS, GEN, geometry)
    GeoPackage at ``target_path``. No-op if ``target_path`` already exists.
    """
    target_path = pathlib.Path(target_path)
    if target_path.is_file():
        return target_path

    download_dir = pathlib.Path(download_dir) if download_dir else target_path.parent / "_vg250_download"
    download_dir.mkdir(parents=True, exist_ok=True)
    zip_path = download_dir / "vg250.zip"

    if not zip_path.is_file():
        utility.safe_print(f"--- downloading VG250 from {VG250_URL} (~70 MB) ---")
        urllib.request.urlretrieve(VG250_URL, zip_path)

    extract_dir = download_dir / "extracted"
    if not extract_dir.is_dir():
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)

    gpkg_candidates = list(extract_dir.rglob("*.gpkg"))
    if not gpkg_candidates:
        raise FileNotFoundError(f"No .gpkg found in extracted VG250 archive at {extract_dir}")
    source_gpkg = gpkg_candidates[0]

    utility.safe_print(f"--- reading layer '{VG250_LAYER}' from {source_gpkg} ---")
    gemeinden = gpd.read_file(source_gpkg, layer=VG250_LAYER)
    # GF (Gebietsfaktor) == 4 selects actual land-based Gemeinde polygons,
    # excluding duplicate water-body entries also present in the layer.
    gemeinden = gemeinden.loc[gemeinden["GF"] == 4, ["AGS", "GEN", "geometry"]].copy()
    gemeinden["AGS"] = gemeinden["AGS"].astype(str).str.zfill(8)
    gemeinden = gemeinden.to_crs(TARGET_CRS)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    gemeinden.to_file(target_path, driver="GPKG")
    utility.safe_print(f"--- saved {len(gemeinden)} Gemeinden to {target_path} ---")

    shutil.rmtree(download_dir, ignore_errors=True)
    return target_path


def load_municipalities(path: pathlib.Path) -> gpd.GeoDataFrame:
    gemeinden = gpd.read_file(path)
    gemeinden["AGS"] = gemeinden["AGS"].astype(str).str.zfill(8)
    return gemeinden


if __name__ == "__main__":
    download_vg250(pathlib.Path("data_DE/municipalities_vg250.gpkg"))
