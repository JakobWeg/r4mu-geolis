"""Self-contained HTML "dataset structure" report for one consolidated
scenario output directory - styled after the reGon M1_N1 team's
V1.1_data_structure.html reference report, adapted to this pipeline's own
five/six-table schema (ev_pool, ev_mapping_ev_municipality, ev_event,
ev_mapping_event_location, ev_charging_location, evaluation_report.xlsx).

Design goal carried over from build_data_quality_report.py: never load a
whole big file into memory just to describe it. Schema/row-count/size facts
come from the Parquet footer alone; the two genuinely large tables
(ev_event, ev_mapping_event_location - hundreds of millions of rows) are
only ever touched column-by-column via pyarrow, and aggregated with
pyarrow.compute before anything is converted to pandas.

Usage:
    python build_dataset_report.py --normalized_dir results/normalized_DE_2024 \
        --out_path results/normalized_DE_2024/dataset_report.html
"""
import argparse
import datetime
import pathlib

import numpy as np
import openpyxl
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

RS7_ID_TO_LABEL = {
    71: "SR_Metro", 72: "SR_Gross", 73: "SR_Mitte", 74: "SR_Klein",
    75: "LR_Zentr", 76: "LR_Mitte", 77: "LR_Klein",
}

BUNDESLAND_BY_PREFIX = {
    "01": "Schleswig-Holstein", "02": "Hamburg", "03": "Niedersachsen", "04": "Bremen",
    "05": "Nordrhein-Westfalen", "06": "Hessen", "07": "Rheinland-Pfalz", "08": "Baden-Württemberg",
    "09": "Bayern", "10": "Saarland", "11": "Berlin", "12": "Brandenburg",
    "13": "Mecklenburg-Vorpommern", "14": "Sachsen", "15": "Sachsen-Anhalt", "16": "Thüringen",
}

# One swatch per use case, drawn from the reference report's --s1..--s8 series
# so a use case keeps the same colour across every chart in this report.
USE_CASE_COLORS = {
    "home_apartment": "var(--s1)", "home_detached": "var(--s2)", "work": "var(--s3)",
    "retail": "var(--s4)", "street": "var(--s5)", "urban_fast": "var(--s6)",
    "highway_fast": "var(--s7)", "depot": "var(--s8)",
}

FILE_DESCRIPTIONS = {
    "ev_pool": "One row per distinct simulated vehicle profile (region type x car type).",
    "ev_mapping_ev_municipality": "Which municipality drew which pool vehicle - one row per drawn vehicle instance.",
    "ev_event": "One row per charging event a pool vehicle profile can produce, with capacity/SoC/timing.",
    "ev_mapping_event_location": "Fact table: every placed charging-event instance, tied to a location.",
    "ev_charging_location": "One row per opened charging location (candidate that received >=1 event).",
    "ev_count_municipality": "Per-municipality vehicle counts by car type, as sampled for this scenario year.",
}


def _fmt_bytes(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


def _parquet_facts(path: pathlib.Path) -> dict:
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    return {
        "name": path.stem,
        "rows": pf.metadata.num_rows,
        "row_groups": pf.metadata.num_row_groups,
        "size_bytes": path.stat().st_size,
        "columns": [(f.name, str(f.type)) for f in schema],
    }


def build_file_facts(normalized_dir: pathlib.Path) -> list:
    facts = []
    for name in ["ev_pool", "ev_mapping_ev_municipality", "ev_event",
                 "ev_mapping_event_location", "ev_charging_location", "ev_count_municipality"]:
        path = normalized_dir / f"{name}.parquet"
        if path.is_file():
            f = _parquet_facts(path)
            f["description"] = FILE_DESCRIPTIONS.get(name, "")
            facts.append(f)
    xlsx_path = normalized_dir / "evaluation_report.xlsx"
    if xlsx_path.is_file():
        wb = openpyxl.load_workbook(xlsx_path, read_only=True)
        facts.append({
            "name": "evaluation_report.xlsx", "rows": None, "row_groups": None,
            "size_bytes": xlsx_path.stat().st_size,
            "columns": [(s, "sheet") for s in wb.sheetnames],
            "description": "Derived aggregates: one weekly load profile per use case, plus summary sheets.",
        })
        wb.close()
    return facts


def build_location_summary(normalized_dir: pathlib.Path) -> pd.DataFrame:
    loc = pd.read_parquet(normalized_dir / "ev_charging_location.parquet",
                           columns=["use_case", "charging_points", "average_charging_capacity"])
    summary = loc.groupby("use_case").agg(
        locations=("use_case", "size"),
        charging_points=("charging_points", "sum"),
        avg_points_per_location=("charging_points", "mean"),
        median_capacity_kw=("average_charging_capacity", "median"),
    ).reset_index()
    return summary.sort_values("locations", ascending=False)


def build_event_location_stats(normalized_dir: pathlib.Path) -> dict:
    """Streams location_id/use_case from the big mapping table via pyarrow
    compute only - never materialises the 400M+ row (5+ billion at 2037/
    2045 scale) table as pandas, or even as a single pyarrow Table: reading
    it whole (as an earlier version of this function did with a single
    pq.read_table call) still holds the full row count in memory before
    pc.value_counts() ever runs. iter_batches + a running per-use_case/
    per-location Series (each bounded by distinct count - 8 use cases,
    tens of millions of locations - not by the mapping table's own row
    count) keeps peak memory to one ~20M-row batch at a time instead.
    """
    parquet_file = pq.ParquetFile(normalized_dir / "ev_mapping_event_location.parquet")
    total_mappings = parquet_file.metadata.num_rows
    uc_counts = pd.Series(dtype=np.int64)
    loc_counts = pd.Series(dtype=np.int64)
    for batch in parquet_file.iter_batches(columns=["location_id", "use_case"], batch_size=20_000_000):
        batch_uc = pc.value_counts(batch.column("use_case"))
        uc_counts = uc_counts.add(
            pd.Series(batch_uc.field("counts").to_numpy(), index=batch_uc.field("values").to_pylist()),
            fill_value=0)
        batch_loc = pc.value_counts(batch.column("location_id"))
        loc_counts = loc_counts.add(
            pd.Series(batch_loc.field("counts").to_numpy(), index=batch_loc.field("values").to_numpy()),
            fill_value=0)
    uc_counts = uc_counts.sort_values(ascending=False)

    events_per_location = loc_counts.to_numpy()

    bins = [0, 1, 10, 50, 100, 250, 500, 1000, 5000, np.inf]
    labels = ["1", "2-10", "11-50", "51-100", "101-250", "251-500", "501-1000", "1001-5000", "5000+"]
    banded = pd.cut(events_per_location, bins=bins, labels=labels)
    band_counts = banded.value_counts().reindex(labels).fillna(0).astype(int)

    return {
        "total_mappings": total_mappings,
        "distinct_locations": len(events_per_location),
        "uc_counts": uc_counts,
        "band_counts": band_counts,
        "min_per_location": int(events_per_location.min()) if len(events_per_location) else 0,
        "max_per_location": int(events_per_location.max()) if len(events_per_location) else 0,
        "mean_per_location": float(events_per_location.mean()) if len(events_per_location) else 0.0,
    }


def build_fleet_table(normalized_dir: pathlib.Path) -> pd.DataFrame:
    pool = pd.read_parquet(normalized_dir / "ev_pool.parquet", columns=["ev_id", "rs7_id", "type"])
    muni = pd.read_parquet(normalized_dir / "ev_mapping_ev_municipality.parquet", columns=["ev_id"])
    draws = muni["ev_id"].value_counts().rename("instances")
    pool = pool.join(draws, on="ev_id").fillna({"instances": 0})
    summary = pool.groupby(["rs7_id", "type"]).agg(
        pool_profiles=("ev_id", "size"), instances_drawn=("instances", "sum"),
    ).reset_index()
    summary["instances_drawn"] = summary["instances_drawn"].astype(np.int64)
    summary["rs7_id"] = summary["rs7_id"].map(RS7_ID_TO_LABEL).fillna(summary["rs7_id"].astype(str))
    return summary.sort_values("instances_drawn", ascending=False)


def build_geo_table(normalized_dir: pathlib.Path) -> pd.DataFrame:
    muni = pd.read_parquet(normalized_dir / "ev_mapping_ev_municipality.parquet", columns=["ags"])
    prefix = muni["ags"].astype(str).str.zfill(8).str[:2]
    bundesland = prefix.map(BUNDESLAND_BY_PREFIX).fillna("unbekannt")
    counts = bundesland.value_counts().rename("vehicle_instances").reset_index()
    counts.columns = ["Bundesland", "vehicle_instances"]
    return counts.sort_values("vehicle_instances", ascending=False)


def build_weekly_load(normalized_dir: pathlib.Path) -> pd.DataFrame:
    xlsx_path = normalized_dir / "evaluation_report.xlsx"
    if not xlsx_path.is_file():
        return pd.DataFrame()
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    # Sheet renamed to "Angeschlossene_Leistung_kW" (see build_evaluation_
    # report.py - "Auslastung" overstated what this curve shows, connected
    # nominal capacity, not actual load) - fall back to the old name for
    # workbooks generated before the rename.
    ws = wb["Angeschlossene_Leistung_kW"] if "Angeschlossene_Leistung_kW" in wb.sheetnames else wb["Auslastung_kW"]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    header, data = rows[0], rows[1:]
    df = pd.DataFrame(data, columns=header)
    return df


def _svg_bar_chart(labels, values, width=860, height=220, color="var(--accent)") -> str:
    if not len(values) or max(values) == 0:
        return "<div class='prose'>keine Daten</div>"
    pad_left, pad_bottom, pad_top = 40, 26, 10
    plot_w, plot_h = width - pad_left - 10, height - pad_bottom - pad_top
    n = len(values)
    bar_w = plot_w / n * 0.7
    gap = plot_w / n
    vmax = max(values)
    bars, ticks = [], []
    for i, (lab, v) in enumerate(zip(labels, values)):
        x = pad_left + i * gap + (gap - bar_w) / 2
        h = (v / vmax) * plot_h
        y = pad_top + plot_h - h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="{color}" rx="2">'
                     f'<title>{lab}: {v:,.0f}</title></rect>')
        ticks.append(f'<text class="ax" x="{x + bar_w / 2:.1f}" y="{height - 6}" text-anchor="middle">{lab}</text>')
    axis = f'<line class="gridline" x1="{pad_left}" y1="{pad_top + plot_h}" x2="{width - 10}" y2="{pad_top + plot_h}"/>'
    return (f'<svg viewBox="0 0 {width} {height}" role="img">'
            + axis + "".join(bars) + "".join(ticks) + "</svg>")


def _svg_line_chart(df: pd.DataFrame, use_cases: list, width=900, height=280) -> str:
    if df.empty:
        return "<div class='prose'>keine Daten</div>"
    pad_left, pad_bottom, pad_top, pad_right = 46, 24, 12, 10
    plot_w, plot_h = width - pad_left - pad_right, height - pad_bottom - pad_top
    n = len(df)
    vmax = max(1.0, df[use_cases].to_numpy().max())
    paths, legend = [], []
    for uc in use_cases:
        if uc not in df.columns:
            continue
        color = USE_CASE_COLORS.get(uc, "var(--accent)")
        ys = df[uc].to_numpy()
        pts = [f"{pad_left + i / (n - 1) * plot_w:.1f},{pad_top + plot_h - (y / vmax) * plot_h:.1f}"
               for i, y in enumerate(ys)]
        paths.append(f'<polyline class="jm-l" style="stroke:{color}" points="{" ".join(pts)}"/>')
        legend.append(f'<div><i style="background:{color}"></i>{uc}</div>')
    gridlines = "".join(
        f'<line class="gridline" x1="{pad_left}" y1="{pad_top + plot_h * f:.1f}" '
        f'x2="{width - pad_right}" y2="{pad_top + plot_h * f:.1f}"/>' for f in (0, 0.25, 0.5, 0.75, 1.0)
    )
    day_ticks = "".join(
        f'<text class="ax" x="{pad_left + d / 7 * plot_w:.1f}" y="{height - 6}">Tag {d + 1}</text>'
        for d in range(7)
    )
    svg = (f'<svg viewBox="0 0 {width} {height}" role="img">' + gridlines + "".join(paths) + day_ticks + "</svg>")
    return svg + f'<div class="legend">{"".join(legend)}</div>'


def build_checks(normalized_dir: pathlib.Path) -> list:
    """Reuses build_data_quality_report's own gap/zero-power computations so
    this report and data_quality_report.html never disagree with each other.
    """
    import build_data_quality_report as bdq
    checks = []
    try:
        gap = bdq.build_placement_gap_table(normalized_dir)
        total_expected = int(gap["expected"].sum())
        total_missing = int(gap["missing"].sum())
        pct = round(100 * total_missing / total_expected, 4) if total_expected else 0.0
        level = "ok" if pct < 0.5 else "warn"
        checks.append((level, f"{pct}% ohne Standort",
                       f"{total_missing:,} von {total_expected:,} erwarteten Ladeevent-Platzierungen fehlen "
                       f"eine <code>location_id</code>. Details je Use Case in data_quality_report.html."))
    except Exception as e:
        checks.append(("info", "Placement-Check nicht verfügbar", str(e)))
    try:
        zero = bdq.build_zero_power_table(normalized_dir)
        n = int(zero["n_rows"].sum()) if len(zero) else 0
        checks.append(("info", f"{n:,} 0-kW-Zeilen ausgeschlossen",
                        "Zeilen mit Parkzeit &gt;0 aber nominal_charging_capacity_kW=0 wurden nicht als "
                        "Ladeevent markiert (siehe restructure_output.py)."))
    except Exception as e:
        checks.append(("info", "0-kW-Check nicht verfügbar", str(e)))
    return checks


CSS = """
:root{
  color-scheme: light;
  --bg:#eef1f0; --surface:#ffffff; --surface2:#f6f9f8; --inset:#e9eeed;
  --ink:#0f1918; --ink2:#435754; --muted:#758884; --rule:#d7e0de; --rule2:#c3cfcc;
  --accent:#0a5a63; --accent-soft:#0a5a6318; --accent2:#a9552b;
  --good:#1a7346; --warn:#96660a; --info:#1f5fa8;
  --s1:#2a78d6; --s2:#eb6834; --s3:#1baf7a; --s4:#eda100;
  --s5:#e87ba4; --s6:#008300; --s7:#4a3aa7; --s8:#e34948;
  --shadow:0 1px 2px rgba(15,25,24,.05), 0 8px 24px -16px rgba(15,25,24,.25);
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    color-scheme: dark;
    --bg:#0b1110; --surface:#121a19; --surface2:#172120; --inset:#1c2726;
    --ink:#e7efed; --ink2:#a8bcb8; --muted:#7d908c; --rule:#243230; --rule2:#31423f;
    --accent:#4bb2ae; --accent-soft:#4bb2ae1f; --accent2:#df8c59;
    --good:#3fa96f; --warn:#d3a021; --info:#5fa2e8;
    --s1:#3987e5; --s2:#d95926; --s3:#199e70; --s4:#c98500;
    --s5:#d55181; --s6:#2fbf7a; --s7:#9085e9; --s8:#e66767;
    --shadow:0 1px 2px rgba(0,0,0,.4), 0 10px 30px -18px rgba(0,0,0,.8);
  }
}
:root[data-theme="dark"]{
  color-scheme: dark;
  --bg:#0b1110; --surface:#121a19; --surface2:#172120; --inset:#1c2726;
  --ink:#e7efed; --ink2:#a8bcb8; --muted:#7d908c; --rule:#243230; --rule2:#31423f;
  --accent:#4bb2ae; --accent-soft:#4bb2ae1f; --accent2:#df8c59;
  --good:#3fa96f; --warn:#d3a021; --info:#5fa2e8;
  --s1:#3987e5; --s2:#d95926; --s3:#199e70; --s4:#c98500;
  --s5:#d55181; --s6:#2fbf7a; --s7:#9085e9; --s8:#e66767;
  --shadow:0 1px 2px rgba(0,0,0,.4), 0 10px 30px -18px rgba(0,0,0,.8);
}
*{box-sizing:border-box}
body{margin:0; background:var(--bg); color:var(--ink); font-family:"Source Sans 3", ui-sans-serif, system-ui, sans-serif; font-size:16px; line-height:1.55}
.wrap{max-width:1140px; margin:0 auto; padding:0 24px 96px}
h1,h2,h3{font-family:"Zilla Slab", Georgia, serif; margin:0}
h1{font-size:clamp(2.1rem,4.6vw,3.1rem); font-weight:700; letter-spacing:-.015em; line-height:1.05}
h2{font-size:1.4rem; font-weight:600}
h3{font-size:1.02rem; font-weight:600}
p{margin:0}
.mono{font-family:"IBM Plex Mono", ui-monospace, monospace; font-variant-numeric:tabular-nums}
.eyebrow{font-family:"IBM Plex Mono", monospace; font-size:.7rem; letter-spacing:.16em; text-transform:uppercase; color:var(--muted)}
header.masthead{padding:48px 0 24px; border-bottom:1px solid var(--rule)}
.masthead .path{display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:16px}
.masthead .path span{font-family:"IBM Plex Mono",monospace; font-size:.72rem; color:var(--muted); letter-spacing:.08em}
.lede{margin-top:14px; max-width:70ch; color:var(--ink2); font-size:1.02rem}
.tiles{display:grid; grid-template-columns:repeat(auto-fit,minmax(168px,1fr)); gap:1px; background:var(--rule); border:1px solid var(--rule); border-radius:3px; overflow:hidden; margin:24px 0 0}
.tile{background:var(--surface); padding:14px 16px 12px}
.tile .v{font-family:"IBM Plex Mono",monospace; font-size:1.3rem; font-weight:500}
.tile .k{font-size:.84rem; color:var(--ink2)}
section{padding-top:44px}
.sec-head{display:flex; align-items:baseline; gap:14px; border-bottom:1px solid var(--rule); padding-bottom:10px; margin-bottom:20px; flex-wrap:wrap}
.sec-head .n{font-family:"IBM Plex Mono",monospace; font-size:.72rem; color:var(--accent); letter-spacing:.1em}
.sec-head .note{margin-left:auto; font-size:.8rem; color:var(--muted)}
.prose{max-width:70ch; color:var(--ink2); margin-bottom:16px}
.card{background:var(--surface); border:1px solid var(--rule); border-radius:4px; box-shadow:var(--shadow)}
.card-pad{padding:18px 20px}
.grid2{display:grid; grid-template-columns:repeat(auto-fit,minmax(330px,1fr)); gap:18px}
.tw{overflow-x:auto; border:1px solid var(--rule); border-radius:4px; background:var(--surface)}
table{border-collapse:collapse; width:100%; font-size:.85rem}
th,td{text-align:right; padding:6px 11px; border-bottom:1px solid var(--rule); white-space:nowrap}
th{font-family:"IBM Plex Mono",monospace; font-size:.68rem; letter-spacing:.05em; text-transform:uppercase; color:var(--muted); font-weight:400; background:var(--surface2)}
td:first-child,th:first-child{text-align:left}
tbody tr:last-child td{border-bottom:0}
tbody tr:hover td{background:var(--surface2)}
.file{margin-bottom:18px; overflow:hidden; border:1px solid var(--rule); border-radius:4px; background:var(--surface)}
.file > .head{display:flex; gap:14px; align-items:center; flex-wrap:wrap; padding:14px 18px; border-bottom:1px solid var(--rule); background:var(--surface2)}
.file > .head h3{font-family:"IBM Plex Mono",monospace; font-size:.92rem; font-weight:500}
.file .facts{margin-left:auto; display:flex; gap:18px; flex-wrap:wrap}
.file .facts div{font-size:.74rem; color:var(--muted)}
.file .facts b{display:block; font-family:"IBM Plex Mono",monospace; font-size:.9rem; color:var(--ink); font-weight:500}
.file .body{padding:12px 18px; font-size:.78rem; color:var(--muted)}
.file .body code{font-family:"IBM Plex Mono",monospace; color:var(--ink2)}
.file .desc{padding:0 18px 12px; font-size:.85rem; color:var(--ink2)}
.checks{display:flex; flex-direction:column; gap:0}
.check{display:grid; grid-template-columns:150px 1fr; gap:14px; padding:12px 16px; border-bottom:1px solid var(--rule); align-items:start}
.check:last-child{border-bottom:0}
.pill{font-family:"IBM Plex Mono",monospace; font-size:.66rem; letter-spacing:.06em; text-transform:uppercase; padding:2px 8px; border-radius:2px; display:inline-flex; border:1px solid}
.pill.ok{color:var(--good); border-color:var(--good); background:color-mix(in srgb, var(--good) 8%, transparent)}
.pill.warn{color:var(--warn); border-color:var(--warn); background:color-mix(in srgb, var(--warn) 10%, transparent)}
.pill.info{color:var(--info); border-color:var(--info); background:color-mix(in srgb, var(--info) 8%, transparent)}
.check .txt{color:var(--ink2); font-size:.88rem}
.check .txt strong{color:var(--ink); font-weight:600}
.legend{display:flex; flex-wrap:wrap; gap:6px 16px; margin:10px 0 0}
.legend div{display:flex; align-items:center; gap:6px; font-size:.76rem; color:var(--ink2); font-family:"IBM Plex Mono",monospace}
.legend i{width:11px; height:11px; border-radius:2px; display:block}
.plot svg{width:100%; height:auto; display:block}
.ax{font-family:"IBM Plex Mono",monospace; font-size:10px; fill:var(--muted)}
.gridline{stroke:var(--rule); stroke-width:1}
.joinmap svg{width:100%; height:auto; display:block; max-width:940px; margin:0 auto}
.jm-box{fill:var(--surface2); stroke:var(--rule2)}
.jm-t{font-family:"IBM Plex Mono",monospace; font-size:11px; fill:var(--ink)}
.jm-s{font-family:"Source Sans 3",sans-serif; font-size:10px; fill:var(--muted)}
.jm-l{stroke:var(--rule2); stroke-width:1.5; fill:none}
.jm-lab{font-family:"IBM Plex Mono",monospace; font-size:9.5px; fill:var(--muted)}
footer{margin-top:48px; padding-top:18px; border-top:1px solid var(--rule); color:var(--muted); font-size:.78rem}
"""


def write_report(normalized_dir: pathlib.Path, out_path: pathlib.Path) -> None:
    normalized_dir = pathlib.Path(normalized_dir)
    files = build_file_facts(normalized_dir)
    loc_summary = build_location_summary(normalized_dir)
    ev_stats = build_event_location_stats(normalized_dir)
    fleet = build_fleet_table(normalized_dir)
    geo = build_geo_table(normalized_dir)
    weekly = build_weekly_load(normalized_dir)
    checks = build_checks(normalized_dir)

    total_vehicles = int(fleet["instances_drawn"].sum())
    total_locations = int(loc_summary["locations"].sum())
    total_points = int(loc_summary["charging_points"].sum())
    total_mappings = ev_stats["total_mappings"]

    tiles = [
        (f"{total_vehicles:,}", "gezogene Fahrzeuginstanzen"),
        (f"{total_locations:,}", "Ladestandorte"),
        (f"{total_points:,}", "Ladepunkte"),
        (f"{total_mappings:,}", "platzierte Ladeevents"),
    ]
    tiles_html = "".join(f'<div class="tile"><div class="v mono">{v}</div><div class="k">{k}</div></div>'
                          for v, k in tiles)

    file_cards = []
    for f in files:
        cols_preview = ", ".join(f'<code>{c}: {t}</code>' for c, t in f["columns"][:8])
        more = f" … +{len(f['columns']) - 8} weitere" if len(f["columns"]) > 8 else ""
        rows_str = f'{f["rows"]:,}' if f["rows"] is not None else "-"
        file_cards.append(f"""<div class="file">
  <div class="head"><h3>{f['name']}</h3>
    <div class="facts">
      <div>Zeilen<b>{rows_str}</b></div>
      <div>Größe<b>{_fmt_bytes(f['size_bytes'])}</b></div>
    </div>
  </div>
  <div class="desc">{f['description']}</div>
  <div class="body">{cols_preview}{more}</div>
</div>""")

    loc_table_rows = "".join(
        f"<tr><td>{r.use_case}</td><td class='n mono'>{r.locations:,}</td>"
        f"<td class='n mono'>{r.charging_points:,}</td>"
        f"<td class='n mono'>{r.avg_points_per_location:.2f}</td>"
        f"<td class='n mono'>{r.median_capacity_kw:.0f}</td></tr>"
        for r in loc_summary.itertuples()
    )

    uc_chart = _svg_bar_chart(loc_summary["use_case"].tolist(), loc_summary["locations"].tolist())
    band_chart = _svg_bar_chart(ev_stats["band_counts"].index.tolist(), ev_stats["band_counts"].to_numpy())

    fleet_rows = "".join(
        f"<tr><td>{r.rs7_id}</td><td>{r.type}</td><td class='n mono'>{r.pool_profiles:,}</td>"
        f"<td class='n mono'>{r.instances_drawn:,}</td></tr>"
        for r in fleet.itertuples()
    )
    geo_rows = "".join(
        f"<tr><td>{r.Bundesland}</td><td class='n mono'>{r.vehicle_instances:,}</td></tr>"
        for r in geo.itertuples()
    )

    use_cases_present = [c for c in USE_CASE_COLORS if c in weekly.columns]
    load_chart = _svg_line_chart(weekly, use_cases_present) if not weekly.empty else "<div class='prose'>evaluation_report.xlsx nicht gefunden</div>"

    checks_html = "".join(
        f'<div class="check"><span class="pill {level}">{level}</span>'
        f'<div class="txt"><strong>{title}</strong><br>{detail}</div></div>'
        for level, title, detail in checks
    )

    html = f"""<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Datensatzstruktur</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
<header class="masthead">
  <div class="path"><span>r4mu-geolis</span><span>·</span><span>bundesweite Verortung</span><span>·</span>
    <span>{normalized_dir.name}</span></div>
  <h1>Struktur des Ergebnisdatensatzes</h1>
  <p class="lede">Sechs Tabellen beschreiben eine synthetische Flotte von <em>{total_vehicles:,}</em> Fahrzeugen,
  <em>{total_locations:,}</em> Ladestandorten und den <em>{total_mappings:,}</em> Zuordnungen zwischen
  Ladeevent und Standort, die beide Seiten verbinden.</p>
  <div class="tiles">{tiles_html}</div>
</header>

<section id="model">
  <div class="sec-head"><span class="n">Datenmodell</span><h2>Fünf Tabellen, ein Fakten-Join</h2></div>
  <div class="prose">Die Fahrzeugseite (<code class="mono">ev_pool</code> →
  <code class="mono">ev_mapping_ev_municipality</code>) und die Standortseite
  (<code class="mono">ev_charging_location</code>) sind nur über
  <code class="mono">ev_mapping_event_location</code> verbunden - die Faktentabelle referenziert
  <code class="mono">ev_event.event_id</code> auf der einen und <code class="mono">location_id</code> auf der
  anderen Seite. <code class="mono">evaluation_report.xlsx</code> ist eine abgeleitete Aggregation aller vier.</div>
  <div class="card joinmap" style="padding:16px">
    <svg viewBox="0 0 900 260" role="img" aria-label="Join-Diagramm der Ausgabetabellen">
      <rect class="jm-box" x="14" y="16" width="190" height="76" rx="3"/>
      <text class="jm-t" x="26" y="36">ev_pool</text>
      <text class="jm-s" x="26" y="51">je simuliertem Profil</text>
      <text class="jm-s" x="26" y="66">◦ ev_id, rs7_id, type</text>

      <rect class="jm-box" x="14" y="160" width="190" height="76" rx="3"/>
      <text class="jm-t" x="26" y="180">ev_mapping_ev_municipality</text>
      <text class="jm-s" x="26" y="195">Fahrzeug → Gemeinde</text>
      <text class="jm-s" x="26" y="210">◦ ev_id, ags</text>
      <path class="jm-l" d="M108 92 L108 160"/>

      <rect class="jm-box" x="340" y="16" width="200" height="76" rx="3"/>
      <text class="jm-t" x="352" y="36">ev_event</text>
      <text class="jm-s" x="352" y="51">je möglichem Ladeevent</text>
      <text class="jm-s" x="352" y="66">◦ event_id, ev_id, kW</text>

      <rect class="jm-box" x="340" y="140" width="200" height="96" rx="3"/>
      <text class="jm-t" x="352" y="160">ev_mapping_event_location</text>
      <text class="jm-s" x="352" y="175">Faktentabelle</text>
      <text class="jm-s" x="352" y="190">◦ event_id → ev_event</text>
      <text class="jm-s" x="352" y="205">◦ location_id → locations</text>
      <path class="jm-l" d="M440 92 L440 140"/>
      <path class="jm-l" d="M204 198 C270 198 300 198 340 188" stroke-dasharray="4 3"/>

      <rect class="jm-box" x="686" y="140" width="200" height="96" rx="3"/>
      <text class="jm-t" x="698" y="160">ev_charging_location</text>
      <text class="jm-s" x="698" y="175">je geöffnetem Standort</text>
      <text class="jm-s" x="698" y="190">◦ location_id, use_case</text>
      <text class="jm-s" x="698" y="205">◦ geometry (EPSG:3035)</text>
      <path class="jm-l" d="M540 188 L686 188"/>

      <rect class="jm-box" x="686" y="16" width="200" height="76" rx="3" stroke-dasharray="4 3"/>
      <text class="jm-t" x="698" y="36">evaluation_report.xlsx</text>
      <text class="jm-s" x="698" y="51">abgeleitet</text>
      <text class="jm-s" x="698" y="66">◦ Wochenlastgang je Use Case</text>
      <path class="jm-l" d="M786 92 L786 16" stroke-dasharray="4 3"/>
    </svg>
  </div>
</section>

<section id="files">
  <div class="sec-head"><span class="n">Dateien</span><h2>Schema und Umfang</h2>
    <span class="note">Metadaten aus den Parquet-Footern, keine vollständigen Table-Scans</span></div>
  {"".join(file_cards)}
</section>

<section id="locations">
  <div class="sec-head"><span class="n">Ladestandorte</span><h2>Use Cases, Kapazität</h2></div>
  <div class="grid2">
    <div class="tw"><table>
      <thead><tr><th>Use Case</th><th>Standorte</th><th>Ladepunkte</th><th>⌀ Punkte/Standort</th><th>Median kW</th></tr></thead>
      <tbody>{loc_table_rows}</tbody>
    </table></div>
    <div class="card card-pad"><h3>Standorte je Use Case</h3><div class="plot">{uc_chart}</div></div>
  </div>
</section>

<section id="events">
  <div class="sec-head"><span class="n">Events</span><h2>Wie sich {total_mappings:,} Zuordnungen verteilen</h2></div>
  <div class="grid2">
    <div class="card card-pad">
      <h3>Events pro Standort</h3>
      <div class="plot">{band_chart}</div>
    </div>
    <div class="card card-pad">
      <h3>Kennzahlen</h3>
      <dl class="tw" style="padding:12px 16px; display:grid; grid-template-columns:auto 1fr; gap:6px 16px; font-size:.86rem; border:none">
        <dt class="mono" style="color:var(--muted)">min/⌀/max je Standort</dt>
        <dd>{ev_stats['min_per_location']:,} / {ev_stats['mean_per_location']:.1f} / {ev_stats['max_per_location']:,}</dd>
        <dt class="mono" style="color:var(--muted)">distinct locations</dt>
        <dd>{ev_stats['distinct_locations']:,}</dd>
      </dl>
    </div>
  </div>
</section>

<section id="fleet">
  <div class="sec-head"><span class="n">Flotte</span><h2>{total_vehicles:,} Fahrzeuge nach Region und Typ</h2></div>
  <div class="tw"><table>
    <thead><tr><th>RegioStaR7</th><th>Typ</th><th>Pool-Profile</th><th>gezogene Instanzen</th></tr></thead>
    <tbody>{fleet_rows}</tbody>
  </table></div>
</section>

<section id="geo">
  <div class="sec-head"><span class="n">Gemeinden</span><h2>Fahrzeuge je Bundesland</h2></div>
  <div class="tw"><table>
    <thead><tr><th>Bundesland</th><th>Fahrzeuginstanzen</th></tr></thead>
    <tbody>{geo_rows}</tbody>
  </table></div>
</section>

<section id="report">
  <div class="sec-head"><span class="n">Excel-Report</span><h2>Wochenlastgang</h2>
    <span class="note">evaluation_report.xlsx · Angeschlossene_Leistung_kW (Nennleistung, keine Lastmessung)</span></div>
  <div class="card card-pad"><div class="plot">{load_chart}</div></div>
</section>

<section id="checks">
  <div class="sec-head"><span class="n">Integrität</span><h2>Was geprüft wurde</h2></div>
  <div class="card checks">{checks_html}</div>
</section>

<footer>
  Quelle: {normalized_dir} · Erzeugt: {datetime.datetime.now().isoformat(timespec="seconds")}
</footer>
</div>
</body>
</html>
"""
    out_path = pathlib.Path(out_path)
    out_path.write_text(html, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--normalized_dir", required=True)
    parser.add_argument("--out_path", default=None,
                         help="defaults to <normalized_dir>/dataset_report.html")
    args = parser.parse_args()
    normalized_dir = pathlib.Path(args.normalized_dir)
    out_path = pathlib.Path(args.out_path) if args.out_path else normalized_dir / "dataset_report.html"
    write_report(normalized_dir, out_path)
    print(f"--- wrote {out_path} ---")


if __name__ == "__main__":
    main()
