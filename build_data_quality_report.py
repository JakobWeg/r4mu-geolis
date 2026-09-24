"""Post-consolidation data-quality report: cross-checks the target-format
tables restructure_output.py just wrote (ev_event, ev_mapping_ev_municipality,
ev_mapping_event_location) for the kind of gaps that don't surface as a
crash - a charging event nobody ever placed, sitting quietly with no location.

Started from a manual investigation this session: 1.3% of all charging events
in the 2024 nationwide run had no location at all, ranging from ~0.1%
(home_apartment/home_detached/highway_fast - already had a synthetic-center
fallback, or don't need one) to 8.1% (retail) - traced to Gemeinden with zero
real candidate locations of that kind. Runs automatically after every
consolidation (see restructure_output.py's own call), so a regression shows
up in the very next run instead of needing another manual investigation.

Usage:
    python build_data_quality_report.py --normalized_dir results/normalized_DE_2024 \
        --out_path results/normalized_DE_2024/data_quality_report.html
"""
import argparse
import collections
import datetime
import pathlib

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import run_de


def _read_columns(path: pathlib.Path, columns: list) -> pd.DataFrame:
    return pq.read_table(path, columns=columns).to_pandas()


def _has_column(path: pathlib.Path, column: str) -> bool:
    return column in pq.ParquetFile(path).schema_arrow.names


def build_placement_gap_table(normalized_dir: pathlib.Path) -> pd.DataFrame:
    """Per charging_use_case: how many charging-event INSTANCES (not
    distinct profile events - a shared profile's event can be placed for
    some drawing vehicles and not others) are missing a location.

    expected = for every charging event_id, how many times its ev_id (pool
    profile) was drawn nationwide (ev_mapping_ev_municipality) - every draw
    needs that event placed once. actual = how many rows actually reference
    that event_id in ev_mapping_event_location.
    """
    ev = _read_columns(normalized_dir / "ev_event.parquet",
                        ["event_id", "ev_id", "nominal_charging_capacity_kW", "charging_use_case"])
    charging = ev.loc[ev["nominal_charging_capacity_kW"] > 0, ["event_id", "ev_id", "charging_use_case"]].copy()
    del ev

    muni = _read_columns(normalized_dir / "ev_mapping_ev_municipality.parquet", ["ev_id"])
    draws_per_ev = muni["ev_id"].value_counts()
    del muni

    # Streamed rather than _read_columns(...) + value_counts() on the whole
    # column: ev_mapping_event_location.parquet is the biggest table in the
    # delivery (5+ billion rows at 2037/2045 scale) - materializing its
    # event_id column in one shot before counting holds the full row count
    # in memory for a result that's bounded by distinct event_id (~72M) -
    # the difference between reading ~40GB and processing it in ~20M-row
    # slices that get discarded as they go. This is the same class of fix
    # already applied to build_evaluation_report.py's Kennzahlen table.
    actual_per_event = pd.Series(dtype=np.int64)
    parquet_file = pq.ParquetFile(normalized_dir / "ev_mapping_event_location.parquet")
    for batch in parquet_file.iter_batches(columns=["event_id"], batch_size=20_000_000):
        counts = pd.Series(batch.column("event_id").to_numpy(zero_copy_only=False)).value_counts()
        actual_per_event = actual_per_event.add(counts, fill_value=0)

    charging["expected"] = draws_per_ev.reindex(charging["ev_id"]).fillna(0).to_numpy().astype(np.int64)
    charging["actual"] = actual_per_event.reindex(charging["event_id"]).fillna(0).to_numpy().astype(np.int64)
    charging["missing"] = charging["expected"] - charging["actual"]

    summary = charging.groupby("charging_use_case", dropna=False).agg(
        expected=("expected", "sum"), actual=("actual", "sum"), missing=("missing", "sum"),
        distinct_events=("event_id", "size"),
        distinct_events_with_gap=("missing", lambda s: int((s > 0).sum())),
    )
    summary["missing_pct"] = (100 * summary["missing"] / summary["expected"]).round(4)
    return summary.sort_values("missing_pct", ascending=False).reset_index()


def build_zero_power_table(normalized_dir: pathlib.Path) -> pd.DataFrame:
    """Rows that came from the raw SimBEV profile carrying a real
    charging_use_case tag despite zero actual station_charging_capacity - a
    parked-but-not-charging stop at that kind of site, or a degenerate/
    clipped session (see restructure_output.py's own charging_use_case
    nulling for these - this table reports what got nulled, not raw rows
    still tagged that way, since by the time this report runs they no
    longer carry a charging_use_case at all).
    """
    ev = _read_columns(normalized_dir / "ev_event.parquet",
                        ["nominal_charging_capacity_kW", "location", "park_time_timesteps"])
    zero = ev.loc[(ev["nominal_charging_capacity_kW"] == 0) & (ev["park_time_timesteps"] > 0)]
    by_location = zero.groupby("location", dropna=False).size().rename("n_rows").reset_index()
    return by_location.sort_values("n_rows", ascending=False)


def build_referential_integrity_stats(normalized_dir: pathlib.Path) -> dict:
    """Both directions of location_id <-> placement referential integrity -
    nothing enforces this outside a real foreign key, so it's worth checking
    after every load rather than assuming it (see the 2024 delivery review
    that first flagged this as something to keep watching)."""
    locations = _read_columns(normalized_dir / "ev_charging_location.parquet", ["location_id"])
    all_ids = set(locations["location_id"].to_numpy())

    # Streamed for the same reason as build_placement_gap_table's event_id
    # count above: this column alone is 5+ billion rows at 2037/2045 scale,
    # but the actual answer is bounded by distinct location_id (tens of
    # millions at most) - accumulating a Python set chunk-by-chunk via
    # iter_batches never holds more than one ~20M-row batch plus the
    # (much smaller, distinct-only) running set at once.
    used_ids = set()
    parquet_file = pq.ParquetFile(normalized_dir / "ev_mapping_event_location.parquet")
    for batch in parquet_file.iter_batches(columns=["location_id"], batch_size=20_000_000):
        col = batch.column("location_id")
        if col.null_count:
            col = col.drop_null()
        used_ids.update(np.unique(col.to_numpy(zero_copy_only=False)).tolist())

    unused = all_ids - used_ids
    dangling = used_ids - all_ids
    return {
        "n_locations": len(all_ids),
        "n_unused_locations": len(unused),
        "n_dangling_location_ids": len(dangling),
        "dangling_sample": sorted(dangling)[:10],
    }


def build_energy_balance_stats(normalized_dir: pathlib.Path) -> dict:
    """Profile-level chargingdemand_battery_kWh (battery-side, into the
    battery) should sit very close to consumption_kWh (battery-side, out of
    the battery) summed nationwide - a large drift would mean the SoC-based
    sign-split in restructure_output.py's _derive_target_event_columns is no
    longer balancing. Both are battery-side by construction (see that
    function's own docstring) - this is NOT the same comparison as the
    grid-side vs battery-side energy gap documented in run_de.py's
    results_summary/energy_grid_kWh comment; don't confuse the two."""
    ev = _read_columns(normalized_dir / "ev_event.parquet", ["chargingdemand_battery_kWh", "consumption_kWh"])
    charge = float(ev["chargingdemand_battery_kWh"].sum())
    consumption = float(ev["consumption_kWh"].sum())
    diff_pct = round(100 * abs(charge - consumption) / consumption, 4) if consumption else 0.0
    return {"total_charge_kWh": charge, "total_consumption_kWh": consumption, "diff_pct": diff_pct}


def build_synthetic_location_check(normalized_dir: pathlib.Path) -> dict:
    """is_synthetic_location should be true for exactly the rows whose
    candidate_uid starts with either synthetic-center prefix (see run_de.py) -
    checked here rather than trusted, since this exact flag silently missed
    one of the two prefixes for the whole 2024 delivery until caught by
    manual review.

    candidate_uid is absent when restructure_output.py ran with
    --drop_candidate_uid (the last year in a scenario chain) - this check
    can't be done without it, so it's skipped rather than crashing the whole
    report over an intentionally-dropped column."""
    loc_path = normalized_dir / "ev_charging_location.parquet"
    if not _has_column(loc_path, "candidate_uid"):
        return {"available": False, "n_flagged_synthetic": 0, "n_expected_synthetic": 0, "n_mismatches": 0}
    loc = _read_columns(loc_path, ["candidate_uid", "is_synthetic_location"])
    uid = loc["candidate_uid"].astype(str)
    expected = (
        uid.str.startswith(run_de.HPC_HIGHWAY_CENTER_PREFIX)
        | uid.str.startswith(run_de.SYNTHETIC_CENTER_PREFIX)
    )
    mismatches = int((loc["is_synthetic_location"].astype(bool) != expected).sum())
    return {
        "available": True,
        "n_flagged_synthetic": int(loc["is_synthetic_location"].sum()),
        "n_expected_synthetic": int(expected.sum()),
        "n_mismatches": mismatches,
    }


def build_candidate_uid_integrity(normalized_dir: pathlib.Path) -> dict:
    """location_registry.parquet keys locations by (use_case, candidate_uid),
    not candidate_uid alone - the same raw candidate_uid legitimately
    appears under more than one use case (e.g. a home_apartment building's
    uid reused as a nearby public street-charging site is a real, separate
    deployment - see restructure_output.py's own registry_keys comment,
    which documents this was collapsed into one shared id by mistake before
    being fixed). So the real invariant to check is: WITHIN one use_case,
    does a candidate_uid ever map to more than one location_id - that would
    mean the same real site got two different ids, which should never
    happen. Repeats of a candidate_uid ACROSS different use cases are
    expected and not counted as a conflict here.

    candidate_uid is absent when restructure_output.py ran with
    --drop_candidate_uid (the last year in a scenario chain) - skipped rather
    than crashing the whole report over an intentionally-dropped column."""
    loc_path = normalized_dir / "ev_charging_location.parquet"
    if not _has_column(loc_path, "candidate_uid"):
        return {"available": False, "n_repeated_candidate_uid": 0, "n_candidate_uid_with_conflicting_location_id": 0}
    loc = _read_columns(loc_path, ["candidate_uid", "use_case", "location_id"])
    loc = loc.dropna(subset=["candidate_uid"])
    key = loc["use_case"].astype(str) + "::" + loc["candidate_uid"].astype(str)
    n_repeated_uid = int((loc["candidate_uid"].value_counts() > 1).sum())
    distinct_location_ids_per_key = loc.groupby(key)["location_id"].nunique()
    n_key_with_multiple_location_ids = int((distinct_location_ids_per_key > 1).sum())
    return {
        "available": True,
        "n_repeated_candidate_uid": n_repeated_uid,
        "n_candidate_uid_with_conflicting_location_id": n_key_with_multiple_location_ids,
    }


def build_event_location_category_mismatch(normalized_dir: pathlib.Path) -> pd.DataFrame:
    """Ladeevents deren eigene charging_use_case (ev_event) von der use_case
    des Standorts abweicht, auf den sie tatsächlich platziert wurden
    (ev_mapping_event_location.use_case, vom Standort übernommen) - z.B.
    urban_fast-Events auf retail-Standorten durch Multi-Use/Fallback-Logik.
    Kein Fehler, aber Berichte, die nach der Event- statt der Standort-
    Kategorie gruppieren, liefern für dieselben Daten unterschiedliche
    Summen je Use Case - hier sichtbar machen statt stillschweigend.

    Streamed in row-group batches, not a single DataFrame.merge() over the
    full table - a merge's join indexer is sized by ev_mapping_event_location's
    own row count (one entry per placement, not per distinct event), which at
    nationwide 2037 scale is 4.9 BILLION rows: confirmed via a real crash
    (numpy._core._exceptions._ArrayMemoryError: Unable to allocate 36.8 GiB)
    right after 2037's from-scratch consolidation had otherwise finished
    cleanly - this was the only casualty, the six core output tables were
    already written before this check ever ran. Batch-wise accumulation into
    a small Counter keeps peak memory bounded by batch_size regardless of how
    large the mapping table is (2024's much smaller table never hit this)."""
    ev = _read_columns(normalized_dir / "ev_event.parquet", ["event_id", "charging_use_case"])
    ev_use_case = ev.set_index("event_id")["charging_use_case"]
    del ev

    counts = collections.Counter()
    pf = pq.ParquetFile(normalized_dir / "ev_mapping_event_location.parquet")
    for batch in pf.iter_batches(batch_size=5_000_000, columns=["event_id", "use_case"]):
        df = batch.to_pandas()
        mapped_charging_use_case = df["event_id"].map(ev_use_case)
        mismatch_mask = (mapped_charging_use_case != df["use_case"]).to_numpy()
        if mismatch_mask.any():
            counts.update(zip(mapped_charging_use_case[mismatch_mask], df["use_case"][mismatch_mask]))

    return pd.DataFrame(
        [(cu, u, n) for (cu, u), n in counts.items()],
        columns=["charging_use_case", "use_case", "n_placements"],
    ).sort_values("n_placements", ascending=False).reset_index(drop=True)


def build_zero_capacity_locations(normalized_dir: pathlib.Path) -> pd.DataFrame:
    """Standorte mit charging_points > 0 aber average_charging_capacity == 0
    - liefern Punkte ohne Leistung, ein Hinweis auf fehlende Kapazitätsdaten
    an der Kandidatenquelle statt auf echte 0-kW-Ladepunkte."""
    loc = _read_columns(normalized_dir / "ev_charging_location.parquet",
                         ["location_id", "use_case", "charging_points", "average_charging_capacity"])
    zero = loc[(loc["charging_points"] > 0) & (loc["average_charging_capacity"] == 0)]
    return zero[["location_id", "use_case", "charging_points"]].sort_values("use_case")


def _style_table(df: pd.DataFrame) -> str:
    return df.to_html(index=False, border=0, classes="data-table", float_format=lambda x: f"{x:,.4f}")


def write_report(normalized_dir: pathlib.Path, out_path: pathlib.Path) -> None:
    normalized_dir = pathlib.Path(normalized_dir)
    gap_table = build_placement_gap_table(normalized_dir)
    zero_power_table = build_zero_power_table(normalized_dir)
    ref_integrity = build_referential_integrity_stats(normalized_dir)
    energy_balance = build_energy_balance_stats(normalized_dir)
    synthetic_check = build_synthetic_location_check(normalized_dir)
    candidate_uid_check = build_candidate_uid_integrity(normalized_dir)
    category_mismatch = build_event_location_category_mismatch(normalized_dir)
    zero_capacity = build_zero_capacity_locations(normalized_dir)

    total_expected = int(gap_table["expected"].sum())
    total_missing = int(gap_table["missing"].sum())
    overall_pct = round(100 * total_missing / total_expected, 4) if total_expected else 0.0
    worst_row = gap_table.iloc[0] if len(gap_table) else None

    html = f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Datenqualitätsbericht</title>
<style>
  body {{ font-family: Arial, Helvetica, sans-serif; margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; }}
  h2 {{ font-size: 1.1rem; margin-top: 2rem; border-bottom: 1px solid #ddd; padding-bottom: 0.3rem; }}
  .meta {{ color: #666; font-size: 0.85rem; margin-bottom: 1.5rem; }}
  .stat-row {{ display: flex; gap: 1.5rem; flex-wrap: wrap; margin: 1rem 0 1.5rem; }}
  .stat {{ background: #fff; border: 1px solid #e2e2e2; border-radius: 6px; padding: 0.8rem 1.2rem; min-width: 160px; }}
  .stat .value {{ font-size: 1.4rem; font-weight: 600; }}
  .stat .label {{ font-size: 0.8rem; color: #666; }}
  .stat.warn .value {{ color: #b45309; }}
  .stat.ok .value {{ color: #15803d; }}
  table.data-table {{ border-collapse: collapse; width: 100%; background: #fff; }}
  table.data-table th, table.data-table td {{ border: 1px solid #e2e2e2; padding: 0.4rem 0.7rem; text-align: right; font-size: 0.9rem; }}
  table.data-table th {{ background: #f0f0f0; text-align: right; }}
  table.data-table th:first-child, table.data-table td:first-child {{ text-align: left; }}
  .note {{ color: #666; font-size: 0.85rem; max-width: 60rem; }}
</style>
</head>
<body>
<h1>Datenqualitätsbericht</h1>
<div class="meta">
  Quelle: {normalized_dir}<br>
  Erzeugt: {datetime.datetime.now().isoformat(timespec="seconds")}
</div>

<h2>Übersicht: fehlende Standortzuordnung</h2>
<div class="stat-row">
  <div class="stat {'ok' if overall_pct < 0.5 else 'warn'}">
    <div class="value">{overall_pct}%</div>
    <div class="label">Ladeevents gesamt ohne Standort</div>
  </div>
  <div class="stat">
    <div class="value">{total_missing:,}</div>
    <div class="label">fehlende Platzierungen (von {total_expected:,})</div>
  </div>
  {"" if worst_row is None else f'''<div class="stat warn">
    <div class="value">{worst_row["missing_pct"]}%</div>
    <div class="label">größte Lücke: {worst_row["charging_use_case"]}</div>
  </div>'''}
</div>
<p class="note">
  "expected" = wie oft ein Ladeevent hätte platziert werden sollen (Anzahl gezogener Fahrzeuge für das
  jeweilige Profil). "missing" = expected minus tatsächlich in ev_mapping_event_location vorhandener
  Zuordnungen. Eine größere Lücke bei einem Use Case deutet meist auf Gemeinden hin, die für diesen Use Case
  gar keine echten Kandidatenstandorte haben (siehe run_de.py's SYNTHETIC_CENTER_USE_CASES-Fallback).
</p>
{_style_table(gap_table)}

<h2>0 kW Ladeevents (ausgeschlossen)</h2>
<p class="note">
  Zeilen, die in den SimBEV-Rohdaten einen "location"-Wert &gt;0 Parkzeit tragen, aber
  nominal_charging_capacity_kW = 0 haben - keine echte Ladenachfrage (z.B. ein reiner Parkvorgang an einem
  Standort, der auch Laden ermöglichen würde, oder eine am Rand abgeschnittene Session). Diese Zeilen
  behalten ihre übrigen Profildaten, aber restructure_output.py setzt ihre charging_use_case-Markierung auf
  leer, damit sie nicht fälschlich als unplatzierte Ladeevents erscheinen.
</p>
{_style_table(zero_power_table)}

<h2>Referenzielle Integrität: location_id &lt;-&gt; Platzierungen</h2>
<p class="note">
  Es gibt keinen Fremdschlüssel zwischen den Parquet-Dateien - beide Richtungen werden hier explizit geprüft,
  statt sie anzunehmen. "unused" = Standorte ohne jede Platzierung; "dangling" = Platzierungen, deren
  location_id in ev_charging_location gar nicht existiert (sollte immer 0 sein).
</p>
<div class="stat-row">
  <div class="stat {'ok' if ref_integrity['n_unused_locations'] == 0 else 'warn'}">
    <div class="value">{ref_integrity['n_unused_locations']:,}</div>
    <div class="label">Standorte ohne Platzierung (von {ref_integrity['n_locations']:,})</div>
  </div>
  <div class="stat {'ok' if ref_integrity['n_dangling_location_ids'] == 0 else 'warn'}">
    <div class="value">{ref_integrity['n_dangling_location_ids']:,}</div>
    <div class="label">Platzierungen auf fehlende location_id</div>
  </div>
</div>

<h2>Energiebilanz: Ladung vs. Verbrauch (batterie-seitig)</h2>
<p class="note">
  chargingdemand_battery_kWh (in die Batterie) und consumption_kWh (aus der Batterie) sind beides batterie-seitige
  Größen aus demselben Vorzeichen-Split (siehe restructure_output.py) - sollten bundesweit sehr nah beieinander
  liegen. Das ist NICHT dieselbe Größe wie die netz-seitige Energie im Auswertungs-Workbook
  (run_de.py's energy_grid_kWh) - beide unterscheiden sich bewusst um den Ladewirkungsgrad.
</p>
<div class="stat-row">
  <div class="stat">
    <div class="value">{energy_balance['total_charge_kWh']/1e6:,.1f} GWh</div>
    <div class="label">Gesamt Ladedemand (Batterie)</div>
  </div>
  <div class="stat">
    <div class="value">{energy_balance['total_consumption_kWh']/1e6:,.1f} GWh</div>
    <div class="label">Gesamt Verbrauch (Batterie)</div>
  </div>
  <div class="stat {'ok' if energy_balance['diff_pct'] < 1 else 'warn'}">
    <div class="value">{energy_balance['diff_pct']}%</div>
    <div class="label">Abweichung</div>
  </div>
</div>

<h2>is_synthetic_location: Abdeckung beider Fallback-Präfixe</h2>
<p class="note">
  is_synthetic_location sollte exakt für Standorte mit candidate_uid-Präfix HPC_HIGHWAY_CENTER_PREFIX oder
  SYNTHETIC_CENTER_PREFIX gesetzt sein (siehe run_de.py). Ein Mismatch &gt; 0 bedeutet, die Flag-Logik in
  restructure_output.py deckt nicht (mehr) alle Fallback-Arten ab.
</p>
{"<p class='note'>Nicht verfügbar - candidate_uid wurde für dieses Szenario-Jahr mit --drop_candidate_uid entfernt.</p>" if not synthetic_check['available'] else f'''<div class="stat-row">
  <div class="stat">
    <div class="value">{synthetic_check['n_flagged_synthetic']:,}</div>
    <div class="label">als synthetisch geflaggt</div>
  </div>
  <div class="stat">
    <div class="value">{synthetic_check['n_expected_synthetic']:,}</div>
    <div class="label">tatsächlich synthetisch (candidate_uid-Präfix)</div>
  </div>
  <div class="stat {"ok" if synthetic_check['n_mismatches'] == 0 else "warn"}">
    <div class="value">{synthetic_check['n_mismatches']:,}</div>
    <div class="label">Mismatches</div>
  </div>
</div>'''}

<h2>candidate_uid-Integrität</h2>
<p class="note">
  Das Registry schlüsselt Standorte nach (use_case, candidate_uid), nicht nach candidate_uid allein - derselbe
  candidate_uid darf legitim unter mehreren Use Cases auftauchen (z.B. ein Wohngebäude, dessen Adresse sowohl
  als home_apartment-Standort als auch als nahegelegener public-Straßenladeplatz dient). Kritisch ist nur, wenn
  INNERHALB desselben Use Case derselbe candidate_uid auf mehr als eine location_id zeigt.
</p>
{"<p class='note'>Nicht verfügbar - candidate_uid wurde für dieses Szenario-Jahr mit --drop_candidate_uid entfernt.</p>" if not candidate_uid_check['available'] else f'''<div class="stat-row">
  <div class="stat">
    <div class="value">{candidate_uid_check['n_repeated_candidate_uid']:,}</div>
    <div class="label">candidate_uid-Werte mit &gt;1 Zeile</div>
  </div>
  <div class="stat {"ok" if candidate_uid_check['n_candidate_uid_with_conflicting_location_id'] == 0 else "warn"}">
    <div class="value">{candidate_uid_check['n_candidate_uid_with_conflicting_location_id']:,}</div>
    <div class="label">davon mit widersprüchlicher location_id (echtes Problem)</div>
  </div>
</div>'''}

<h2>Event-Kategorie vs. Standort-Kategorie</h2>
<p class="note">
  Platzierungen, bei denen die Use-Case-Kategorie des Ladeevents von der des tatsächlich genutzten Standorts
  abweicht (z.B. urban_fast-Events auf retail-Standorten via Multi-Use/Fallback). Kein Fehler, aber Berichte,
  die nach der einen statt der anderen Kategorie gruppieren, liefern unterschiedliche Summen für denselben Datensatz.
</p>
{_style_table(category_mismatch) if len(category_mismatch) else '<p class="note">Keine Abweichungen gefunden.</p>'}

<h2>Standorte mit Ladepunkten aber 0 kW Kapazität</h2>
<p class="note">
  average_charging_capacity = 0 bei charging_points &gt; 0 - liefert Punkte ohne Leistung, meist ein Hinweis
  auf fehlende Kapazitätsangabe an der Kandidatenquelle.
</p>
{_style_table(zero_capacity) if len(zero_capacity) else '<p class="note">Keine betroffenen Standorte gefunden.</p>'}

</body>
</html>
"""
    out_path = pathlib.Path(out_path)
    out_path.write_text(html, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--normalized_dir", required=True)
    parser.add_argument("--out_path", default=None,
                         help="defaults to <normalized_dir>/data_quality_report.html")
    args = parser.parse_args()
    normalized_dir = pathlib.Path(args.normalized_dir)
    out_path = pathlib.Path(args.out_path) if args.out_path else normalized_dir / "data_quality_report.html"
    write_report(normalized_dir, out_path)
    print(f"--- wrote {out_path} ---")


if __name__ == "__main__":
    main()
