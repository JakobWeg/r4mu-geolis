"""Builds the nationwide evaluation report from a completed restructure_output.py
result (results/normalized_DE/) as one Excel workbook with three sheets:

  RS7_Zusammenfassung - Ladepunkte/Energie(Jahr+Tag)/installierte Leistung,
                         je RegioStaR7-Typ x Use-Case, + DE gesamt
                         (from the run_de.py result_summary_DE.csv)
  Kennzahlen          - Anzahl Fahrzeuge/Events je RegioStaR7-Typ + DE gesamt,
                         Anzahl Ladeevents/Standorte je Use-Case (DE gesamt)
  Angeschlossene_Leistung_kW / Auslastung_Anzahl - nationwide connected-
                         capacity curve (station_charging_capacity summed
                         over every plugged-in session, NOT the actual power
                         drawn - a session's real draw tapers as it
                         approaches full SoC, so this is an upper bound, not
                         a load measurement) and concurrent-session-count
                         curve, for one representative week, je Use-Case, as
                         data + chart. Sheet renamed from "Auslastung_kW" -
                         that name implied real load/utilization, which this
                         isn't (confirmed: ~11.5x the mean charging power the
                         same data's own annual energy implies).

Only meant to run when [analysis] run_evaluation = true in config_DE.cfg -
see run_de.py/restructure_output.py for the flag check; this module is the
implementation, callable standalone for re-runs/testing.

Usage:
    python build_evaluation_report.py --result_dir results/_DE_26-08-25_122428 \
        --normalized_dir results/normalized_DE \
        --vehicle_input_file scenario/input_vehicles_2024.xlsx \
        --out results/evaluation_report.xlsx
"""
import argparse
import pathlib

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from openpyxl.chart import LineChart, Reference

import vehicle_input as vi
from summarize_by_rs7 import RS7_CODE, build_summary, SIMULATED_DAYS

# One representative week, chosen away from year boundaries (avoids New
# Year's Day / partial-week edge effects). 96 timesteps/day (15-min).
WEEK_START_STEP = 100 * 96
WEEK_END_STEP = WEEK_START_STEP + 7 * 96
# Generous margin so events that START before the week but overlap into it
# (e.g. a multi-day depot/home charging session) aren't cut off - covers
# sessions up to 100h long, comfortably above any realistic charging event.
MARGIN_STEPS = 400


def build_kennzahlen(normalized_dir: pathlib.Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    # ev_pool is profile-level (one row per distinct EV/SimBEV profile, see
    # restructure_output.py's module docstring) - the actual sampled vehicle
    # count comes from ev_mapping_ev_municipality (one row per instance),
    # joined back to ev_pool just for rs7_id.
    pool = pd.read_parquet(normalized_dir / "ev_pool.parquet", columns=["ev_id", "rs7_id"])
    mapping_muni = pd.read_parquet(normalized_dir / "ev_mapping_ev_municipality.parquet", columns=["id", "ev_id"])
    rs7_id_to_label = {v[0]: k for k, v in RS7_CODE.items()}
    rs7_id_to_name = {v[0]: v[1] for v in RS7_CODE.values()}

    instances = mapping_muni.merge(pool, on="ev_id", how="left")
    vehicles_per_rs7 = instances.groupby("rs7_id").size().rename("Anzahl_Fahrzeuge")

    # Each sampled vehicle instance's own event count = however many rows
    # its (shared, only stored once) EV/pool profile has in ev_event - far
    # cheaper than the old fully-duplicated design, since ev_event holds
    # each distinct profile's events exactly once.
    ev_id_col = pd.read_parquet(normalized_dir / "ev_event.parquet", columns=["ev_id"])["ev_id"]
    events_per_profile = ev_id_col.value_counts()
    del ev_id_col
    events_per_instance = instances["ev_id"].map(events_per_profile)
    events_per_rs7 = events_per_instance.groupby(instances["rs7_id"]).sum().rename("Anzahl_Events_gesamt")

    table_a = pd.concat([vehicles_per_rs7, events_per_rs7], axis=1).reset_index().rename(columns={"index": "rs7_id"})
    table_a["RegioStaR7"] = table_a["rs7_id"].map(rs7_id_to_label)
    table_a["RegioStaR7_Bezeichnung"] = table_a["rs7_id"].map(rs7_id_to_name)
    de_total_a = pd.DataFrame([{
        "rs7_id": 0, "RegioStaR7": "DE_gesamt", "RegioStaR7_Bezeichnung": "Gesamt Deutschland",
        "Anzahl_Fahrzeuge": table_a["Anzahl_Fahrzeuge"].sum(),
        "Anzahl_Events_gesamt": table_a["Anzahl_Events_gesamt"].sum(),
    }])
    table_a = pd.concat([table_a, de_total_a], ignore_index=True)
    table_a = table_a[["rs7_id", "RegioStaR7", "RegioStaR7_Bezeichnung", "Anzahl_Fahrzeuge", "Anzahl_Events_gesamt"]]
    table_a = table_a.sort_values("rs7_id").reset_index(drop=True)

    # Ladeevents/Standorte je Use-Case - DE gesamt only (not split by RS7):
    # attributing a location back to a RegioStaR7-Typ would need tracing
    # location -> vehicle across the full mapping table; not needed for what
    # was asked ("Anzahl der Events gesamt" as the guiding example). Each
    # mapping-table row is already one distinct real placement (one vehicle
    # instance's one charging event at one location), and carries its own
    # use_case directly (denormalized from ev_charging_location), so a plain
    # row count/groupby is "Anzahl Ladeevents" directly - no join needed.
    #
    # Read/aggregated in row-group batches rather than via a single
    # groupby(...).agg(nunique=...) call: pandas' nunique implementation
    # combines (use_case, location_id) into one int64 "group index" array
    # sized to the FULL row count (get_group_index in pandas/core/sorting.py)
    # to compute it - for the 2045 nationwide mapping table (~5.05 billion
    # rows) that one array alone needs ~37.6 GiB and OOM-crashes here. Row
    # count scales with vehicle instances, not with the actual number of
    # distinct (use_case, location_id) pairs (a few 10s of millions at most,
    # bounded by distinct locations x use_cases), so accumulating a running
    # deduplicated set across batches - never materializing a full-row-count
    # array - stays within a few GB regardless of table size.
    mapping_path = normalized_dir / "ev_mapping_event_location.parquet"
    parquet_file = pq.ParquetFile(mapping_path)
    ladeevents_by_use_case = pd.Series(dtype="int64")
    distinct_pairs_parts = []
    for batch in parquet_file.iter_batches(columns=["location_id", "use_case"], batch_size=20_000_000):
        chunk = batch.to_pandas()
        ladeevents_by_use_case = ladeevents_by_use_case.add(
            chunk["use_case"].value_counts(), fill_value=0)
        distinct_pairs_parts.append(chunk.drop_duplicates())
        # Re-dedupe the accumulator itself once it grows past a threshold, so
        # a long run of batches can't let it creep back up toward the full
        # row count before the final dedupe at the end.
        if len(distinct_pairs_parts) > 1 and sum(len(p) for p in distinct_pairs_parts) > 20_000_000:
            distinct_pairs_parts = [pd.concat(distinct_pairs_parts, ignore_index=True).drop_duplicates()]
    distinct_pairs = pd.concat(distinct_pairs_parts, ignore_index=True).drop_duplicates()
    standorte_by_use_case = distinct_pairs.groupby("use_case").size()

    table_b = pd.DataFrame({
        "Use_Case": ladeevents_by_use_case.index,
        "Anzahl_Ladeevents": ladeevents_by_use_case.to_numpy(dtype="int64"),
    })
    table_b["Anzahl_Standorte"] = table_b["Use_Case"].map(standorte_by_use_case).fillna(0).astype("int64")

    return table_a, table_b


def build_load_curve(normalized_dir: pathlib.Path) -> pd.DataFrame:
    # ev_event holds each distinct EV/pool profile's events only once - a
    # given event_id can be "used" by many sampled vehicle instances (each
    # potentially placed at a different real-world location/use_case), so
    # the actual per-instance charging sessions for the load curve come from
    # expanding through ev_mapping_event_location below, not from this table
    # directly. park_start_timesteps/park_time_timesteps (not event_start/
    # event_time - restructure_output.py drops those as redundant with the
    # park_*/drive_* split) are 0 for driving rows and equal to the original
    # event_start/event_time for charging rows, which is exactly what's
    # needed here since we only care about charging (nominal_charging_
    # capacity_kW > 0) events anyway.
    event_cols = pd.read_parquet(
        normalized_dir / "ev_event.parquet",
        columns=["event_id", "park_start_timesteps", "park_time_timesteps", "nominal_charging_capacity_kW"],
        filters=[("park_start_timesteps", "<", WEEK_END_STEP), ("park_start_timesteps", ">", WEEK_START_STEP - MARGIN_STEPS)],
    )
    event_cols = event_cols[event_cols["nominal_charging_capacity_kW"] > 0]
    event_cols = event_cols.rename(columns={
        "park_start_timesteps": "event_start", "park_time_timesteps": "event_time",
        "nominal_charging_capacity_kW": "station_charging_capacity",
    })

    # Streamed rather than read_parquet(...) + isin() on the whole table:
    # ev_mapping_event_location.parquet is the biggest table in the delivery
    # (5+ billion rows at 2037/2045 scale), but event_cols above is already
    # narrowed to one week's worth of events - reading the whole mapping
    # table into pandas just to immediately throw away all but that narrow
    # slice held the full table in memory for a filter result that's tiny
    # by comparison. iter_batches + pc.is_in() per ~20M-row batch keeps only
    # matching rows, so peak memory is one batch plus the (small) result.
    wanted_ids = pa.array(event_cols["event_id"].to_numpy())
    matched_batches = []
    parquet_file = pq.ParquetFile(normalized_dir / "ev_mapping_event_location.parquet")
    for batch in parquet_file.iter_batches(columns=["event_id", "use_case"], batch_size=20_000_000):
        mask = pc.is_in(batch.column("event_id"), value_set=wanted_ids)
        if pc.any(mask).as_py():
            matched_batches.append(batch.filter(mask))
    mapping = (pa.Table.from_batches(matched_batches).to_pandas() if matched_batches
               else pd.DataFrame({"event_id": pd.array([], dtype="int64"), "use_case": pd.array([], dtype="object")}))

    # One row per real vehicle-instance placement (same event_id can appear
    # multiple times here, once per instance sharing that profile) - exactly
    # what the load curve needs to reflect actual vehicle count. use_case is
    # already on the mapping table (denormalized from ev_charging_location),
    # so no further join is needed.
    week_events = mapping.merge(event_cols, on="event_id", how="inner")

    n_steps = WEEK_END_STEP - WEEK_START_STEP
    use_cases = sorted(week_events["use_case"].dropna().unique())
    power_curve = pd.DataFrame({"timestep": np.arange(n_steps)})
    count_curve = pd.DataFrame({"timestep": np.arange(n_steps)})

    for uc in use_cases:
        sub = week_events[week_events["use_case"] == uc]
        # True (unclipped) relative start/end - events starting before the
        # window (rel_start < 0, kept via MARGIN_STEPS above) are already
        # mid-session at t=0. Clipping start to 0 before computing end (as an
        # earlier version of this did) would discard how much of the event
        # is already over, and worse, made every carried-over session LOOK
        # like it started fresh at t=0 - a spurious pile-up-then-decay spike
        # at the very start of the week that isn't real charging behaviour.
        # Fix: sessions already active at t=0 add straight to a baseline
        # (no synthetic "start" event), and only get an "end" diff when they
        # actually finish within the window.
        rel_start = sub["event_start"].to_numpy() - WEEK_START_STEP
        rel_end = rel_start + sub["event_time"].to_numpy()
        power = sub["station_charging_capacity"].to_numpy()

        overlaps = rel_end > 0  # already-filtered via MARGIN_STEPS, but be safe
        rel_start, rel_end, power = rel_start[overlaps], rel_end[overlaps], power[overlaps]

        already_active = rel_start < 0
        baseline_power = power[already_active].sum()
        baseline_count = int(already_active.sum())

        power_diff = np.zeros(n_steps + 1)
        count_diff = np.zeros(n_steps + 1)
        # Already-active sessions: only the (clipped) end diff, no start diff.
        end_clip_active = np.clip(rel_end[already_active], 0, n_steps)
        np.add.at(power_diff, end_clip_active, -power[already_active])
        np.add.at(count_diff, end_clip_active, -1)
        # Sessions that genuinely start inside the window.
        fresh = ~already_active & (rel_start < n_steps)
        starts_f = rel_start[fresh]
        ends_f = np.clip(rel_end[fresh], 0, n_steps)
        power_f = power[fresh]
        np.add.at(power_diff, starts_f, power_f)
        np.add.at(power_diff, ends_f, -power_f)
        np.add.at(count_diff, starts_f, 1)
        np.add.at(count_diff, ends_f, -1)
        power_diff[0] += baseline_power
        count_diff[0] += baseline_count

        power_curve[uc] = np.cumsum(power_diff[:-1])
        count_curve[uc] = np.cumsum(count_diff[:-1])

    power_curve["timestamp_in_week"] = pd.to_timedelta(power_curve["timestep"] * 15, unit="m").astype(str)
    return power_curve, count_curve


def write_report(result_dir: pathlib.Path, normalized_dir: pathlib.Path, vehicle_input_file: str, out_path: pathlib.Path):
    rs7_summary = build_summary(result_dir, vehicle_input_file)
    kennzahlen_a, kennzahlen_b = build_kennzahlen(normalized_dir)
    power_curve, count_curve = build_load_curve(normalized_dir)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        rs7_summary.to_excel(writer, index=False, sheet_name="RS7_Zusammenfassung")

        kennzahlen_a.to_excel(writer, index=False, sheet_name="Kennzahlen", startrow=0)
        start_b = len(kennzahlen_a) + 3
        kennzahlen_b.to_excel(writer, index=False, sheet_name="Kennzahlen", startrow=start_b)

        # "Angeschlossene_Leistung_kW", not "Auslastung_kW" - the curve sums
        # station_charging_capacity (nominal/rated) for every plugged-in
        # session, not the actual power drawn, so "Auslastung" (utilization/
        # load) overstated what this shows (see module docstring).
        power_curve.to_excel(writer, index=False, sheet_name="Angeschlossene_Leistung_kW")
        count_curve.to_excel(writer, index=False, sheet_name="Auslastung_Anzahl")

        ws = writer.sheets["Angeschlossene_Leistung_kW"]
        n_rows = len(power_curve)
        n_use_cases = power_curve.shape[1] - 2  # minus timestep, timestamp_in_week
        chart = LineChart()
        chart.title = "Angeschlossene Leistung je Use-Case (Beispielwoche, kW, Nennleistung - keine Lastmessung)"
        chart.y_axis.title = "kW"
        chart.x_axis.title = "Zeitschritt (15 min)"
        data = Reference(ws, min_col=2, max_col=1 + n_use_cases, min_row=1, max_row=n_rows + 1)
        cats = Reference(ws, min_col=1, min_row=2, max_row=n_rows + 1)
        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)
        chart.width, chart.height = 30, 15
        ws.add_chart(chart, f"A{n_rows + 3}")

    print(f"--- wrote {out_path} ---")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--result_dir", required=True)
    parser.add_argument("--normalized_dir", required=True)
    parser.add_argument("--vehicle_input_file", default="scenario/input_vehicles_2024.xlsx")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    write_report(pathlib.Path(args.result_dir), pathlib.Path(args.normalized_dir),
                 args.vehicle_input_file, pathlib.Path(args.out))


if __name__ == "__main__":
    main()
