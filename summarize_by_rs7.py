"""Builds a summary table of installed charging infrastructure and energy
throughput per use case, broken down by RegioStaR7 type and once for
Germany as a whole - from an already-completed run_de.py result directory's
result_summary_DE.csv (per-Gemeinde x use-case charging_points/energy/
installed_power), joined against each Gemeinde's RegioStaR7 type.

Usage:
    python summarize_by_rs7.py --result_dir results/_DE_26-08-25_122428 \
        --vehicle_input_file scenario/input_vehicles_2024.xlsx \
        --out results/summary_by_rs7.xlsx
"""
import argparse
import pathlib

import pandas as pd

import vehicle_input as vi

# Official BBSR RegioStaR7 codes/labels (see restructure_output.py).
RS7_CODE = {
    "SR_Metro": (71, "Metropolen"),
    "SR_Gross": (72, "Regiopolen und Großstädte"),
    "SR_Mitte": (73, "Mittelstädte, städtischer Raum"),
    "SR_Klein": (74, "Kleinstädtischer, dörflicher Raum (Stadtregion)"),
    "LR_Zentr": (75, "Zentrale Stadt (Ländliche Region)"),
    "LR_Mitte": (76, "Städtischer Raum (Ländliche Region)"),
    "LR_Klein": (77, "Kleinstädtischer, dörflicher Raum (Ländliche Region)"),
}

# Same translation restructure_output.py applies to the consolidated tables'
# own use_case column (USE_CASE_SPEC_NAMES there) - duplicated here rather
# than imported to avoid a circular import (restructure_output.py itself
# imports build_evaluation_report.py, which imports this module). Without
# it, this sheet's Use_Case values (run_de.py's own internal names) didn't
# match the Kennzahlen/Auslastung sheets (spec names, from the consolidated
# tables) - same data, two vocabularies in one workbook, and any join across
# sheets by use case failed silently.
USE_CASE_SPEC_NAMES = {
    "hpc_urban": "urban_fast",
    "hpc_highway": "highway_fast",
    "public": "street",
}

# SimBEV pool profiles cover a full simulated year - used to turn total
# energy into a daily average. Not derived per-Gemeinde because event
# horizons vary slightly (last event of the year vs. simulation_steps
# rounding); 365 is the standard SimBEV run length this pool was built for.
SIMULATED_DAYS = 365


def build_summary(result_dir: pathlib.Path, vehicle_input_file: str) -> pd.DataFrame:
    summary = pd.read_csv(result_dir / "result_summary_DE.csv", dtype={"AGS": str})
    # A result_summary_DE.csv produced by a run_de.py older than the energy-
    # naming fix (grid vs. battery energy, see run_de.py's results_summary
    # comment) still has the old bare "energy" column - e.g. 2037's raw run
    # predates that fix even though this file doesn't. Accept both so an
    # already-completed raw run doesn't need to be redone just to consolidate.
    if "energy" in summary.columns and "energy_grid_kWh" not in summary.columns:
        summary = summary.rename(columns={"energy": "energy_grid_kWh"})
    # Match the consolidated tables' vocabulary (see USE_CASE_SPEC_NAMES
    # above) - result_summary_DE.csv still carries run_de.py's own internal
    # names (public/hpc_urban/hpc_highway).
    summary["use_case"] = summary["use_case"].map(lambda u: USE_CASE_SPEC_NAMES.get(u, u))

    demand_df = vi.load_municipality_demand(vehicle_input_file)
    ags_to_rs7 = demand_df.drop_duplicates("AGS").set_index("AGS")["RegioStaR7"]
    summary["RegioStaR7"] = summary["AGS"].map(ags_to_rs7)

    missing = summary["RegioStaR7"].isna().sum()
    if missing:
        print(f"WARNING: {missing} rows have no RegioStaR7 match (AGS not in {vehicle_input_file}) - excluded")
        summary = summary.dropna(subset=["RegioStaR7"])

    by_rs7 = summary.groupby(["RegioStaR7", "use_case"], as_index=False)[
        ["charging_points", "energy_grid_kWh", "installed_power"]
    ].sum()
    by_rs7["rs7_id"] = by_rs7["RegioStaR7"].map(lambda c: RS7_CODE[c][0])
    by_rs7["RegioStaR7_Bezeichnung"] = by_rs7["RegioStaR7"].map(lambda c: RS7_CODE[c][1])

    de_total = summary.groupby("use_case", as_index=False)[
        ["charging_points", "energy_grid_kWh", "installed_power"]
    ].sum()
    de_total["RegioStaR7"] = "DE_gesamt"
    de_total["rs7_id"] = 0
    de_total["RegioStaR7_Bezeichnung"] = "Gesamt Deutschland"

    result = pd.concat([by_rs7, de_total], ignore_index=True)
    result["energy_pro_tag_kwh"] = (result["energy_grid_kWh"] / SIMULATED_DAYS).round(1)
    # Auslastung: durchschnittlich pro Tag je Ladepunkt verladene Energiemenge
    # (Gesamtenergie / Ladepunkte / 365) - ein Nutzungsgrad-Indikator, nicht
    # nur eine absolute Energiemenge. 0 statt inf/NaN, falls ein use_case in
    # diesem RS7-Typ keine Ladepunkte hat (kann bei der DE_gesamt-Zeile eines
    # sonst leeren use_case theoretisch vorkommen).
    # Alle drei Energie-Spalten hier sind NETZ-seitige Energie (inkl.
    # Ladeverlusten, aus SimBEVs "energy_grid" - siehe run_de.py's
    # results_summary-Kommentar), nicht die BATTERIE-seitige
    # chargingdemand_battery_kWh aus den konsolidierten ev_event-Tabellen. Beide
    # unterscheiden sich um den Ladewirkungsgrad (~1/0.9 bundesweit 2024) -
    # das ist kein Fehler, aber die beiden Größen dürfen nicht ungekennzeichnet
    # vermischt werden, deshalb das explizite "_Netz_" im Spaltennamen.
    result["auslastung_kwh_pro_lp_tag"] = (
        result["energy_grid_kWh"] / result["charging_points"] / SIMULATED_DAYS
    ).replace([float("inf"), -float("inf")], 0).fillna(0).round(2)
    result = result.rename(columns={
        "use_case": "Use_Case",
        "charging_points": "Ladepunkte",
        "energy_grid_kWh": "Energie_Netz_kWh_Jahr",
        "installed_power": "Installierte_Leistung_kW",
        "energy_pro_tag_kwh": "Energie_Netz_kWh_pro_Tag",
        "auslastung_kwh_pro_lp_tag": "Auslastung_Netz_kWh_pro_LP_Tag",
    })
    result = result[["RegioStaR7", "rs7_id", "RegioStaR7_Bezeichnung", "Use_Case",
                      "Ladepunkte", "Energie_Netz_kWh_Jahr", "Energie_Netz_kWh_pro_Tag",
                      "Auslastung_Netz_kWh_pro_LP_Tag", "Installierte_Leistung_kW"]]
    result = result.sort_values(["rs7_id", "Use_Case"]).reset_index(drop=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--vehicle_input_file", default="scenario/input_vehicles_2024.xlsx")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    result = build_summary(pathlib.Path(args.result_dir), args.vehicle_input_file)

    out_path = pathlib.Path(args.out)
    if out_path.suffix == ".xlsx":
        result.to_excel(out_path, index=False, sheet_name="Ladeinfrastruktur")
    else:
        result.to_csv(out_path, index=False)
    print(f"--- wrote {out_path} ({len(result)} rows) ---")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
