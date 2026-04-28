"""
Budget Summary HTML Renderer

Takes output from summary_engine.build_summary() and produces
a styled HTML file matching the app's dark theme with tab indicators.
"""

from summary_engine import TAB_COLORS, TAB_SHORT


def fmt(val, is_pct=False):
    """Format a number for display."""
    if val is None:
        return "—"
    if is_pct:
        return f"{val:.1f}%"
    if isinstance(val, str):
        return val
    if val < 0:
        return f"({abs(val):,.0f})"
    if val == 0:
        return "0"
    return f"{val:,.0f}"


def var_class(val):
    """CSS class for variance coloring."""
    if val is None or val == 0:
        return "zero"
    # For expenses, positive variance = bad (costs went up)
    # We'll let the sign speak for itself
    if val < 0:
        return "negative"
    return ""


def render_tab_chip(source_tab):
    """Render a colored tab indicator chip."""
    if not source_tab:
        return '<span class="src-tag tag-calc">Σ</span>'
    short = TAB_SHORT.get(source_tab, source_tab)
    colors = TAB_COLORS.get(source_tab, TAB_COLORS.get("Unknown"))
    css_class = f"tag-{source_tab.lower().replace(' & ', '-').replace(' ', '-').replace('&', '')}"
    return f'<span class="src-tag {css_class}">{short}</span>'


def render_html(summary_data, output_path=None):
    """
    Render a complete HTML page from summary engine output.

    Args:
        summary_data: dict from build_summary()
        output_path: optional file path to write HTML to
    Returns:
        HTML string
    """
    entity = summary_data.get("entity_code", "")
    name = summary_data.get("building_name", "")
    year = summary_data.get("budget_year", "")
    ytd_m = summary_data.get("ytd_months", 0)
    est_m = 12 - ytd_m
    rows = summary_data.get("rows", [])
    stats = summary_data.get("stats", {})

    # Determine prior year from context
    prior_year = year - 2 if year else "Prior"
    current_year = year - 1 if year else "Current"

    # Build table rows
    table_rows = []
    for row in rows:
        rt = row["row_type"]
        label = row["label"]

        if rt == "section_header":
            table_rows.append(f'<tr class="section-header"><td colspan="10">{_esc(label)}</td></tr>')
            continue

        # Determine row CSS class
        row_class = ""
        if rt == "subtotal":
            subtype = row.get("subtype", "")
            if subtype == "grand_total":
                row_class = "grand-total"
            elif subtype == "net_operating" or "net operating" in label.lower():
                row_class = "net-op"
            else:
                row_class = "subtotal"

        # Tab chip
        tab_html = render_tab_chip(row.get("source_tab"))

        # Editable marker for Col 6
        edit_marker = '<span class="editable-marker"></span>' if rt == "data" else ""

        # Format values
        c1 = fmt(row["col1"])
        c2 = fmt(row["col2"])
        c3 = fmt(row["col3"])
        c4 = fmt(row["col4"])
        c5 = fmt(row["col5"])
        c6 = fmt(row["col6"])
        c7 = fmt(row["col7"])
        c8 = fmt(row["col8"], is_pct=True)

        # CSS classes for values
        def val_cls(v):
            if v is None:
                return "zero"
            if isinstance(v, str):
                return ""
            if v < 0:
                return "negative"
            if v == 0:
                return "zero"
            return ""

        c1_cls = val_cls(row["col1"])
        c2_cls = val_cls(row["col2"])
        c3_cls = val_cls(row["col3"])
        c7_cls = val_cls(row["col7"])
        c8_cls = val_cls(row["col8"])

        # Footnote marker
        fn = f' <span class="fn">({row["footnote_marker"]})</span>' if row.get("footnote_marker") else ""

        table_rows.append(f'''<tr class="{row_class}">
  <td>{_esc(label)}{fn}</td>
  <td>{tab_html}</td>
  <td class="{c1_cls}">{c1}</td>
  <td class="{c2_cls}">{c2}</td>
  <td class="{c3_cls}">{c3}</td>
  <td>{c4}</td>
  <td>{c5}</td>
  <td>{c6}{edit_marker}</td>
  <td class="{c7_cls}">{c7}</td>
  <td class="{c8_cls}">{c8}</td>
</tr>''')

    table_html = "\n".join(table_rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Budget Summary — {_esc(name)} ({entity})</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0f1923; color: #e0e0e0; padding: 20px; }}

  .header {{ margin-bottom: 20px; }}
  .header h1 {{ font-size: 20px; color: #4fc3f7; margin-bottom: 4px; }}
  .header h2 {{ font-size: 14px; color: #90a4ae; font-weight: 400; }}
  .badge {{ display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; margin-left: 8px; }}
  .badge-green {{ background: #1b5e20; color: #a5d6a7; }}

  .tab-legend {{ display: flex; gap: 6px; margin: 14px 0; flex-wrap: wrap; }}
  .tab-chip {{ display: inline-flex; align-items: center; gap: 5px; padding: 4px 10px; border-radius: 14px; font-size: 11px; font-weight: 600; border: 1px solid rgba(255,255,255,0.1); }}

  .table-wrapper {{ overflow-x: auto; border-radius: 8px; border: 1px solid #263238; }}
  table {{ border-collapse: collapse; width: 100%; min-width: 1100px; font-size: 12px; }}

  th {{ background: #1a2a3a; color: #b0bec5; font-weight: 600; padding: 8px 10px; text-align: right; border-bottom: 2px solid #37474f; white-space: nowrap; position: sticky; top: 0; z-index: 2; }}
  th:first-child {{ text-align: left; min-width: 260px; position: sticky; left: 0; z-index: 3; }}
  th:nth-child(2) {{ min-width: 80px; text-align: center; }}
  th .sub {{ display: block; font-size: 9px; color: #78909c; font-weight: 400; padding-top: 2px; }}

  td {{ padding: 6px 10px; text-align: right; border-bottom: 1px solid #1e2d3a; white-space: nowrap; }}
  td:first-child {{ text-align: left; font-weight: 500; position: sticky; left: 0; background: inherit; z-index: 1; }}
  td:nth-child(2) {{ text-align: center; }}

  tr {{ background: #141f2b; }}
  tr:hover {{ background: #1a2836; }}

  .section-header td {{ background: #1a2a3a !important; color: #4fc3f7; font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid #37474f; padding-top: 14px; }}
  .subtotal td {{ background: #1e3040 !important; font-weight: 700; border-top: 1px solid #37474f; }}
  .subtotal td:first-child {{ color: #81d4fa; }}
  .grand-total td {{ background: #1b5e20 !important; color: #e8f5e9; font-weight: 700; font-size: 13px; border-top: 2px solid #4caf50; }}
  .net-op td {{ background: #263238 !important; font-weight: 700; border-top: 2px solid #546e7a; border-bottom: 2px solid #546e7a; }}
  .net-op td:first-child {{ color: #fff; }}

  .zero {{ color: #546e7a; }}
  .negative {{ color: #ef9a9a; }}

  .fn {{ font-size: 9px; color: #78909c; }}
  .editable-marker {{ display: inline-block; width: 5px; height: 5px; background: #ffd54f; border-radius: 50%; margin-left: 3px; vertical-align: middle; }}

  .src-tag {{ display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 9px; font-weight: 600; }}
  .tag-income {{ background: rgba(76,175,80,0.18); color: #81c784; }}
  .tag-payroll {{ background: rgba(33,150,243,0.18); color: #64b5f6; }}
  .tag-energy {{ background: rgba(255,152,0,0.18); color: #ffb74d; }}
  .tag-water--sewer {{ background: rgba(0,188,212,0.18); color: #4dd0e1; }}
  .tag-repairs--supplies {{ background: rgba(121,85,72,0.18); color: #a1887f; }}
  .tag-gen--admin {{ background: rgba(156,39,176,0.18); color: #ba68c8; }}
  .tag-re-taxes {{ background: rgba(244,67,54,0.18); color: #e57373; }}
  .tag-manual {{ background: rgba(255,213,79,0.18); color: #ffd54f; }}
  .tag-unknown {{ background: rgba(158,158,158,0.18); color: #bdbdbd; }}
  .tag-calc {{ background: rgba(158,158,158,0.18); color: #bdbdbd; }}

  .stats {{ margin-top: 16px; display: flex; gap: 16px; }}
  .stat-card {{ padding: 12px 16px; background: #1a2a3a; border-radius: 8px; border: 1px solid #263238; }}
  .stat-card .num {{ font-size: 20px; font-weight: 700; color: #e0e0e0; }}
  .stat-card .lbl {{ font-size: 10px; color: #78909c; margin-top: 2px; }}
</style>
</head>
<body>

<div class="header">
  <h1>{_esc(name)} (Entity {entity}) <span class="badge badge-green">Option A Engine</span></h1>
  <h2>Budget Year {year} &bull; {stats.get('total_rows', 0)} rows &bull; {ytd_m} months actual / {est_m} months estimate</h2>
</div>

<div class="tab-legend">
  <div class="tab-chip" style="background:rgba(76,175,80,0.12);color:#81c784;border-color:rgba(76,175,80,0.3)">Income</div>
  <div class="tab-chip" style="background:rgba(33,150,243,0.12);color:#64b5f6;border-color:rgba(33,150,243,0.3)">Payroll</div>
  <div class="tab-chip" style="background:rgba(255,152,0,0.12);color:#ffb74d;border-color:rgba(255,152,0,0.3)">Energy</div>
  <div class="tab-chip" style="background:rgba(0,188,212,0.12);color:#4dd0e1;border-color:rgba(0,188,212,0.3)">Water</div>
  <div class="tab-chip" style="background:rgba(121,85,72,0.12);color:#a1887f;border-color:rgba(121,85,72,0.3)">R&amp;S</div>
  <div class="tab-chip" style="background:rgba(156,39,176,0.12);color:#ba68c8;border-color:rgba(156,39,176,0.3)">Gen&amp;Admin</div>
  <div class="tab-chip" style="background:rgba(244,67,54,0.12);color:#e57373;border-color:rgba(244,67,54,0.3)">RE Taxes</div>
  <div class="tab-chip" style="background:rgba(255,213,79,0.12);color:#ffd54f;border-color:rgba(255,213,79,0.3)">Manual / FA</div>
</div>

<div class="table-wrapper">
<table>
<thead>
<tr>
  <th>Line Item</th>
  <th>Tab</th>
  <th>{prior_year} Actual*<span class="sub">Col 1 · Excel import</span></th>
  <th>{current_year} {ytd_m} Mo. Actual<span class="sub">Col 2 · budget_lines</span></th>
  <th>{current_year} {est_m} Mo. Est**<span class="sub">Col 3 · calculated</span></th>
  <th>{current_year} 12 Mo. Forecast<span class="sub">Col 4 · GL prefix SUM</span></th>
  <th>{current_year} Budget<span class="sub">Col 5 · budget_lines</span></th>
  <th>{year} Budget<span class="sub">Col 6 · proposed (FA editable)</span></th>
  <th>$ Variance<span class="sub">Col 7 · Budget − Forecast</span></th>
  <th>% Variance<span class="sub">Col 8</span></th>
</tr>
</thead>
<tbody>
{table_html}
</tbody>
</table>
</div>

<div class="stats">
  <div class="stat-card"><div class="num">{stats.get('data_rows', 0)}</div><div class="lbl">Data rows</div></div>
  <div class="stat-card"><div class="num">{stats.get('subtotal_rows', 0)}</div><div class="lbl">Subtotals</div></div>
  <div class="stat-card"><div class="num">{stats.get('total_rows', 0)}</div><div class="lbl">Total rows</div></div>
</div>

</body>
</html>"""

    if output_path:
        with open(output_path, "w") as f:
            f.write(html)

    return html


def _esc(text):
    """HTML-escape a string."""
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Standalone: render both 204 and 302 ──────────────────────────────────

if __name__ == "__main__":
    from budget_summary_parser import parse_yrlycomp
    from summary_engine import build_summary

    buildings = [
        {
            "path": "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_app/204 -  444 East 86th Street 2026 Operating Budget  - Approved.xlsx",
            "ytd_months": 8,
            "output": "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_summary_engine_204.html",
        },
        {
            "path": "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/2025 budget approved budgets only/302 - 205 Water - 2025 Operating Budget - Approved.xlsx",
            "ytd_months": 9,
            "output": "/sessions/ecstatic-sleepy-thompson/mnt/Budgets/budget_summary_engine_302.html",
        },
    ]

    for b in buildings:
        parsed = parse_yrlycomp(b["path"])
        if "error" in parsed:
            print(f"ERROR: {parsed['error']}")
            continue

        summary = build_summary(parsed, [], ytd_months=b["ytd_months"])
        html = render_html(summary, b["output"])
        print(f"✅ Rendered {summary['building_name']} ({summary['entity_code']}) → {b['output']}")
        print(f"   {summary['stats']}")
