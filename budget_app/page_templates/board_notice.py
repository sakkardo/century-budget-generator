# Extracted from workflow.py 2026-07-05 (clean-architecture tranche 1).
# BYTE-IDENTICAL constant — template edits happen HERE now. Keep the
# string style unchanged (raw vs non-raw matters for JS escapes; see
# the wizard-template-js-escapes memory / check_template_js gate).

BOARD_NOTICE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ snapshot.budget.building_name }} — {{ snapshot.budget.year }} Budget Notice</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Serif:wght@400;500&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --canvas: #eef0f2; --card: #ffffff; --ink: #14181c; --muted: #5f6b76; --faint: #8b959e;
  --navy: #001721; --line: #e2e6ea; --line-2: #cdd4da;
  --red: #b42324; --green: #1e6f4c; --gold: #b98a4a; --chipbg-up: #faecec; --chipbg-dn: #e9f4ee;
}
* { box-sizing: border-box; margin: 0; }
body { background: var(--canvas); color: var(--ink); font-family: 'IBM Plex Sans', sans-serif; font-size: 14.5px; line-height: 1.6; }

.mast { background: var(--navy); color: #fff; }
.mast-in { max-width: 1180px; margin: 0 auto; padding: 26px 32px 30px; display: flex; justify-content: space-between; align-items: flex-end; gap: 20px; flex-wrap: wrap; }
.mast .firm { font-size: 11px; font-weight: 700; letter-spacing: 0.3em; text-transform: uppercase; color: var(--gold); }
.mast h1 { font-family: 'IBM Plex Serif', serif; font-weight: 500; font-size: 27px; margin-top: 8px; }
.mast .sub { color: rgba(255,255,255,0.62); font-size: 13px; margin-top: 3px; }
.mast .right { text-align: right; font-size: 12px; color: rgba(255,255,255,0.62); }
.mast .right b { display: block; color: #fff; font-weight: 600; font-size: 13px; }
.statusline { border-top: 1px solid rgba(255,255,255,0.14); }
.statusline .in { max-width: 1180px; margin: 0 auto; padding: 10px 32px; display: flex; gap: 26px; align-items: center; font-size: 12px; color: rgba(255,255,255,0.72); flex-wrap: wrap; }
.statusline b { color: #fff; font-weight: 600; }
.badge { display: inline-block; background: var(--gold); color: var(--navy); font-size: 10.5px; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase; border-radius: 3px; padding: 2px 8px; }
.printbtn { margin-left: auto; background: none; border: 1px solid rgba(255,255,255,0.35); color: rgba(255,255,255,0.85); font-family: inherit; font-size: 11.5px; padding: 5px 12px; border-radius: 4px; cursor: pointer; }
.printbtn:hover, .printbtn:focus-visible { border-color: #fff; color: #fff; }

.wrap { max-width: 1180px; margin: 26px auto 90px; padding: 0 32px; display: grid; grid-template-columns: 208px 1fr; gap: 30px; align-items: start; }
@media (max-width: 900px) { .wrap { grid-template-columns: 1fr; } .rail { display: none; } }
.rail { position: sticky; top: 22px; }
.rail a { display: block; font-size: 13px; color: var(--muted); text-decoration: none; padding: 9px 14px; border-left: 2px solid var(--line-2); }
.rail a:hover { color: var(--ink); }
.rail a.on { color: var(--navy); font-weight: 600; border-left-color: var(--navy); background: #fff; }
.rail .cap { font-size: 10.5px; font-weight: 700; letter-spacing: 0.16em; text-transform: uppercase; color: var(--faint); padding: 0 14px 10px; }

.main { min-width: 0; }
.card { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 26px 30px; margin-bottom: 22px; box-shadow: 0 1px 2px rgba(20,24,28,0.04); }
.card h2 { font-family: 'IBM Plex Serif', serif; font-weight: 500; font-size: 20px; color: var(--navy); }
.card .sub { font-size: 12.5px; color: var(--muted); margin: 4px 0 0; }
.card-head { display: flex; justify-content: space-between; align-items: baseline; gap: 16px; padding-bottom: 16px; border-bottom: 1px solid var(--line); margin-bottom: 20px; flex-wrap: wrap; }

.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(168px, 1fr)); gap: 14px; margin-bottom: 22px; }
.kpi { background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 18px 20px; box-shadow: 0 1px 2px rgba(20,24,28,0.04); }
.kpi .k { font-size: 11px; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; color: var(--faint); }
.kpi .v { font-family: 'IBM Plex Serif', serif; font-size: 27px; color: var(--navy); margin-top: 6px; font-variant-numeric: tabular-nums; }
.kpi .s { font-size: 12px; color: var(--muted); margin-top: 3px; }
.kpi.hero { background: var(--navy); border-color: var(--navy); }
.kpi.hero .k { color: var(--gold); }
.kpi.hero .v { color: #fff; font-size: 33px; }
.kpi.hero .s { color: rgba(255,255,255,0.65); }

.chip { display: inline-block; font-size: 12px; font-weight: 600; padding: 2px 9px; border-radius: 100px; font-variant-numeric: tabular-nums; white-space: nowrap; }
.chip.up { background: var(--chipbg-up); color: var(--red); }
.chip.dn { background: var(--chipbg-dn); color: var(--green); }
.chip.flat { background: #f0f2f4; color: var(--faint); font-weight: 500; }

table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
th { font-size: 10.5px; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase; color: var(--faint); text-align: left; padding: 9px 12px; border-bottom: 1.5px solid var(--line-2); white-space: nowrap; }
th.n, td.n { text-align: right; }
td { padding: 10px 12px; border-bottom: 1px solid var(--line); vertical-align: middle; }
td.n { font-family: 'IBM Plex Mono', monospace; font-size: 12.8px; }
tr:hover td { background: #fafbfc; }
tr.total td { font-weight: 700; background: #f5f7f8; border-top: 1.5px solid var(--line-2); color: var(--navy); }
.tbl-note { font-size: 11.5px; color: var(--faint); margin-top: 10px; }

.mixgrid { display: grid; grid-template-columns: 240px 1fr; gap: 34px; align-items: center; }
@media (max-width: 700px) { .mixgrid { grid-template-columns: 1fr; } }
.donut { width: 220px; height: 220px; border-radius: 50%; position: relative; margin: 0 auto; }
.donut .hole { position: absolute; inset: 36px; background: var(--card); border-radius: 50%; display: flex; flex-direction: column; align-items: center; justify-content: center; }
.donut .hole .t { font-family: 'IBM Plex Serif', serif; font-size: 21px; color: var(--navy); font-variant-numeric: tabular-nums; }
.donut .hole .c { font-size: 9.5px; letter-spacing: 0.15em; color: var(--faint); margin-top: 3px; }
.leg { font-size: 13px; }
.leg-r { display: grid; grid-template-columns: 12px 1fr 56px 100px 1fr; gap: 12px; align-items: center; padding: 8px 0; border-bottom: 1px solid var(--line); }
.leg-r .sw { width: 10px; height: 10px; border-radius: 2px; }
.leg-r .pc { text-align: right; color: var(--muted); font-variant-numeric: tabular-nums; }
.leg-r .am { text-align: right; font-family: 'IBM Plex Mono', monospace; font-size: 12.5px; }
.leg-r .bar { height: 6px; background: #f0f2f4; border-radius: 3px; overflow: hidden; }
.leg-r .bar i { display: block; height: 100%; border-radius: 3px; }

.mv { display: grid; grid-template-columns: minmax(170px, 230px) 1fr 110px; gap: 16px; align-items: center; padding: 10px 0; border-bottom: 1px solid var(--line); font-size: 13.5px; }
.mv .tr2 { height: 16px; background: #f0f2f4; border-radius: 3px; overflow: hidden; }
.mv .tr2 i { display: block; height: 100%; }
.mv .v { text-align: right; font-family: 'IBM Plex Mono', monospace; font-size: 12.8px; font-weight: 500; }

.pills { display: flex; flex-wrap: wrap; gap: 7px; margin-bottom: 16px; }
.pill { font-family: inherit; font-size: 12.5px; font-weight: 600; padding: 7px 14px; border-radius: 7px; border: 1px solid var(--line-2); background: #fff; color: var(--muted); cursor: pointer; }
.pill:hover { border-color: var(--navy); color: var(--navy); }
.pill.on { background: var(--navy); border-color: var(--navy); color: #fff; }
.pill:focus-visible { outline: 2px solid var(--navy); outline-offset: 2px; }
.tpanel { display: none; } .tpanel.on { display: block; }
.tpanel-name { display: none; font-size: 11px; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: var(--faint); margin: 0 0 8px; }
.tbl-scroll { overflow-x: auto; }

.daterow { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; }
.datebox { border: 1px solid var(--line); border-left: 3px solid var(--gold); border-radius: 8px; padding: 14px 18px; }
.datebox .d { font-family: 'IBM Plex Serif', serif; font-size: 18px; color: var(--navy); }
.datebox .l { font-size: 11px; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; color: var(--faint); margin-top: 3px; }

.acc { border: 1px solid var(--line); border-radius: 8px; margin-bottom: 10px; overflow: hidden; }
.acc summary { list-style: none; cursor: pointer; padding: 14px 18px; font-weight: 600; font-size: 14px; display: flex; justify-content: space-between; align-items: center; gap: 12px; }
.acc summary::-webkit-details-marker { display: none; }
.acc summary::after { content: "+"; font-size: 18px; color: var(--faint); flex-shrink: 0; }
.acc[open] summary::after { content: "–"; }
.acc[open] summary { border-bottom: 1px solid var(--line); }
.acc .a { padding: 14px 18px; color: var(--muted); font-size: 13.5px; white-space: pre-wrap; }

.sig { text-align: center; padding: 34px 0 8px; color: var(--muted); font-size: 12.5px; }
.sig b { display: block; font-family: 'IBM Plex Serif', serif; font-size: 16px; color: var(--navy); font-weight: 500; margin-bottom: 3px; }

/* ── What-if workshop (interactive, lives only in the reader's browser) ── */
.wif-band { position: sticky; top: 12px; z-index: 30; background: var(--navy); color: #f2ede4; border-radius: 10px;
  padding: 14px 20px; display: flex; gap: 26px; align-items: center; flex-wrap: wrap;
  box-shadow: 0 8px 24px rgba(0,23,33,.28); margin-bottom: 16px; }
.wk { min-width: 148px; }
.wk-l { font-family: 'IBM Plex Mono', monospace; font-size: 10px; letter-spacing: .13em; text-transform: uppercase; color: #8fa3ac; }
.wk-v { font-size: 21px; font-weight: 600; font-variant-numeric: tabular-nums; margin-top: 2px; }
.wk.hero .wk-v { font-size: 30px; color: #b98a4a; }
.wk-s { font-size: 11px; color: #8fa3ac; font-variant-numeric: tabular-nums; margin-top: 1px; }
.wif-reset { margin-left: auto; font: inherit; font-size: 12.5px; font-weight: 600; border: 1px solid rgba(242,237,228,.35);
  background: transparent; color: #f2ede4; border-radius: 6px; padding: 7px 14px; cursor: pointer; }
.wif-reset:hover { background: rgba(242,237,228,.1); }
.wif-cat { display: flex; justify-content: space-between; align-items: baseline; font-family: 'IBM Plex Serif', serif;
  font-size: 15.5px; font-weight: 600; color: var(--navy); border-bottom: 2px solid var(--line); padding: 16px 4px 6px; }
.wif-cat span:last-child { font-family: 'IBM Plex Sans', sans-serif; font-size: 13.5px; font-variant-numeric: tabular-nums; color: var(--muted); }
.wif-row { display: flex; align-items: center; gap: 14px; padding: 8px 4px; border-bottom: 1px solid #f0ece3; }
.wif-d { flex: 1 1 240px; font-size: 13.5px; min-width: 200px; }
.wif-cent { display: block; font-size: 11px; color: var(--faint); font-variant-numeric: tabular-nums; }
.wif-lever { display: flex; align-items: center; gap: 8px; flex: 0 0 300px; }
.wif-lever input[type=range] { flex: 1; accent-color: #b98a4a; height: 24px; min-width: 120px; }
.wif-fine { width: 26px; height: 26px; border-radius: 6px; border: 1px solid var(--line); background: #fbfaf6;
  font-size: 15px; font-weight: 700; color: var(--navy); cursor: pointer; line-height: 1; }
.wif-fine:hover { background: #efe9dd; }
.wif-pct { min-width: 52px; text-align: right; font-weight: 700; font-size: 12.5px; font-variant-numeric: tabular-nums; color: var(--faint); }
.wif-pct.up { color: var(--red); } .wif-pct.dn { color: var(--green); }
.wif-val { flex: 0 0 108px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums; font-size: 14px; }
.wif-delta { flex: 0 0 96px; text-align: right; font-family: 'IBM Plex Mono', monospace; font-size: 11.5px; color: #b3a88f; font-variant-numeric: tabular-nums; }
.wif-delta.up { color: var(--red); } .wif-delta.dn { color: var(--green); }
.wif-row.wif-fixed { color: var(--muted); background: #faf8f3; }
@media (max-width: 760px) { .wif-lever { flex: 1 1 100%; } .wif-row { flex-wrap: wrap; } }

@media print {
  body { background: #fff; }
  .rail, .pills, .printbtn { display: none !important; }
  .wif { display: none !important; }
  .wrap { display: block; margin: 0; padding: 0; max-width: none; }
  .card, .kpi { box-shadow: none; break-inside: avoid; }
  .tpanel { display: block !important; margin-bottom: 18px; }
  .tpanel-name { display: block !important; }
  .acc .a { display: block; }
  .mast, .statusline, .kpi.hero, .chip, .donut, .leg-r .bar i, .mv .tr2 i, .datebox { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
}
</style>
</head>
<body>
{% set words = snapshot.narrative.building_type_words or {} %}
{% set hd = snapshot.narrative.headline %}
{% set tl = snapshot.narrative.timeline or {} %}
{% set Y = snapshot.budget.year %}

<div class="mast">
  <div class="mast-in">
    <div>
      <div class="firm">Century Management</div>
      <h1>{{ snapshot.budget.building_name }}</h1>
      <div class="sub">Proposed Operating Budget · Fiscal Year {{ Y }}</div>
    </div>
    <div class="right">Prepared for<b>The Board of {{ 'Managers' if words.is_condo else 'Directors' }}</b></div>
  </div>
  <div class="statusline"><div class="in">
    <span><span class="badge">Proposed budget</span></span>
    {% if tl.board_vote_by %}<span>Board vote by <b>{{ tl.board_vote_by }}</b></span>{% endif %}
    <span>Prepared by <b>Century Management Finance Team</b></span>
    <button class="printbtn" onclick="window.print()">Print / Save as PDF</button>
  </div></div>
</div>

<div class="wrap">
  <nav class="rail" id="rail" aria-label="Report sections">
    <div class="cap">This report</div>
    <a href="#s-sum" class="on">Summary</a>
    {% if snapshot.detail_tabs and snapshot.detail_tabs.chart_data.donut %}<a href="#s-mix">Expense composition</a>{% endif %}
    <a href="#s-drv">Category changes</a>
    {% if snapshot.detail_tabs and snapshot.detail_tabs.chart_data.movers %}<a href="#s-mov">Largest movements</a>{% endif %}
    {% if snapshot.detail_tabs and snapshot.detail_tabs.tabs %}<a href="#s-det">Full budget detail</a>{% endif %}
    {% if snapshot.detail_tabs and snapshot.detail_tabs.tabs %}<a href="#s-wif">What-if workshop</a>{% endif %}
    {% if tl.board_review_through or tl.board_vote_by or tl.effective_date %}<a href="#s-dates">Key dates</a>{% endif %}
    {% if snapshot.narrative.faq %}<a href="#s-faq">Questions &amp; answers</a>{% endif %}
  </nav>

  <div class="main">

    <div class="kpis" id="s-sum">
      <div class="kpi hero"><div class="k">Proposed change</div><div class="v">{{ '%+.1f'|format(hd.pct_change) }}%</div><div class="s">in operating expense vs. {{ Y - 1 }}</div></div>
      <div class="kpi"><div class="k">{{ Y }} Operating expense</div><div class="v">{{ fmt(hd.exp_proposed) }}</div><div class="s">from {{ fmt(hd.exp_current) }} in {{ Y - 1 }}</div></div>
      {% if snapshot.narrative.income %}
      <div class="kpi"><div class="k">{{ Y }} Total income</div><div class="v">{{ fmt(snapshot.narrative.income.proposed) }}</div><div class="s">from {{ fmt(snapshot.narrative.income.current) }} in {{ Y - 1 }}</div></div>
      {% endif %}
      <div class="kpi"><div class="k">Net change</div><div class="v">{{ '+' if hd.net_change >= 0 else '' }}{{ fmt(hd.net_change) }}</div><div class="s">to be funded by {{ words.charge_word or 'common charge' }}s</div></div>
    </div>

    <div class="card">
      <p style="font-size:15.5px; color:var(--ink);">{{ snapshot.narrative.opening }}</p>
      <p style="font-size:14px; color:var(--muted); margin-top:12px;">{{ snapshot.narrative.driver_summary }}</p>
      {% if snapshot.narrative.additional_notes %}<p style="font-size:14px; color:var(--muted); margin-top:12px;">{{ snapshot.narrative.additional_notes }}</p>{% endif %}
    </div>

    {% if snapshot.detail_tabs and snapshot.detail_tabs.chart_data.donut %}
    {% set donut = snapshot.detail_tabs.chart_data.donut %}
    {% set bars = snapshot.detail_tabs.chart_data.bars %}
    <div class="card" id="s-mix">
      <div class="card-head"><div><h2>Expense composition</h2><div class="sub">Every dollar of the proposed {{ Y }} budget, by category</div></div></div>
      <div class="mixgrid">
        <div class="donut" style="background: conic-gradient({% for s in donut %}{{ s.color }} {{ s.start }}% {{ s.start + s.pct }}%{% if not loop.last %}, {% endif %}{% endfor %});">
          <div class="hole"><div class="t">{{ fmt(hd.exp_proposed) }}</div><div class="c">{{ Y }} EXPENSE</div></div>
        </div>
        <div class="leg">
          {% set maxprop = bars | map(attribute='proposed') | max if bars else 1 %}
          {% for s in donut %}
          {% set b = bars[loop.index0] if bars and loop.index0 < (bars | length) else None %}
          <div class="leg-r"><span class="sw" style="background:{{ s.color }}"></span><span>{{ s.name }}</span><span class="pc">{{ s.pct }}%</span><span class="am">{{ fmt(b.proposed) if b else '' }}</span><span class="bar"><i style="width:{{ (b.proposed / maxprop * 100) | round(1) if b and maxprop else 0 }}%; background:{{ s.color }}"></i></span></div>
          {% endfor %}
        </div>
      </div>
    </div>
    {% endif %}

    <div class="card" id="s-drv">
      <div class="card-head"><div><h2>Category changes</h2><div class="sub">{{ Y - 1 }} budget vs. {{ Y }} proposed</div></div></div>
      <div class="tbl-scroll"><table>
        <thead><tr><th>Category</th><th class="n">{{ Y - 1 }} Budget</th><th class="n">{{ Y }} Proposed</th><th class="n">Change</th><th class="n">%</th></tr></thead>
        <tbody>
          {% for c in snapshot.narrative.categories %}
          {% set d = c.proposed - c.current %}
          <tr><td>{{ c.sheet }}</td><td class="n">{{ fmt(c.current) }}</td><td class="n">{{ fmt(c.proposed) }}</td>
            <td class="n">{% if d == 0 %}<span class="chip flat">no change</span>{% else %}<span class="chip {{ 'dn' if d < 0 else 'up' }}">{{ '+' if d > 0 else '−' }}{{ fmt(d) | replace('-$', '$') | replace('−$', '$') }}</span>{% endif %}</td>
            <td class="n" style="color:var(--muted)">{% if d == 0 or not c.current %}—{% else %}{{ '%+.1f'|format(d / c.current * 100) }}%{% endif %}</td></tr>
          {% endfor %}
          {% set td = hd.exp_proposed - hd.exp_current %}
          <tr class="total"><td>Total operating expense</td><td class="n">{{ fmt(hd.exp_current) }}</td><td class="n">{{ fmt(hd.exp_proposed) }}</td>
            <td class="n">{% if td == 0 %}<span class="chip flat">no change</span>{% else %}<span class="chip {{ 'dn' if td < 0 else 'up' }}">{{ '+' if td > 0 else '−' }}{{ fmt(td) | replace('-$', '$') | replace('−$', '$') }}</span>{% endif %}</td>
            <td class="n">{{ '%+.1f'|format(hd.pct_change) }}%</td></tr>
        </tbody>
      </table></div>
    </div>

    {% if snapshot.detail_tabs and snapshot.detail_tabs.chart_data.movers %}
    <div class="card" id="s-mov">
      <div class="card-head"><div><h2>Largest movements</h2><div class="sub">The individual budget lines driving the change, ranked by dollar impact</div></div></div>
      {% for mv in snapshot.detail_tabs.chart_data.movers %}
      <div class="mv"><div>{{ mv.label }}</div>
        <div class="tr2"><i style="width:{{ mv.bar_pct }}%; background:{{ 'var(--green)' if mv.favorable else 'var(--red)' }}"></i></div>
        <div class="v" style="color:{{ 'var(--green)' if mv.favorable else 'var(--red)' }}">{{ '+' if mv.change > 0 else '−' }}{{ fmt(mv.change) | replace('-$', '$') | replace('−$', '$') }}</div></div>
      {% endfor %}
    </div>
    {% endif %}

    {% if snapshot.detail_tabs and snapshot.detail_tabs.tabs %}
    {% set ytdm = snapshot.detail_tabs.meta.ytd_months if snapshot.detail_tabs.meta else None %}
    <div class="card" id="s-det">
      <div class="card-head"><div><h2>Full budget detail</h2><div class="sub">Every line of the proposed budget — the same numbers behind the summary above</div></div></div>
      <div class="pills" role="tablist">
        {% for t in snapshot.detail_tabs.tabs %}
        <button class="pill{% if loop.first %} on{% endif %}" onclick="showTab({{ loop.index0 }})" data-i="{{ loop.index0 }}">{{ t.name }}</button>
        {% endfor %}
      </div>
      {% for t in snapshot.detail_tabs.tabs %}
      <div class="tpanel{% if loop.first %} on{% endif %}" data-i="{{ loop.index0 }}">
        <div class="tpanel-name">{{ t.name }}</div>
        {% if t.lines is defined %}
        {% set ns = namespace(pa=0, yt=0, cu=0, fc=0, pr=0, gaps=false) %}
        <div class="tbl-scroll"><table>
          <thead><tr><th>Description</th><th class="n">{{ Y - 2 }} Actual</th><th class="n">{{ Y - 1 }} YTD</th><th class="n">{{ Y - 1 }} Budget</th><th class="n">{{ Y - 1 }} Forecast</th><th class="n">{{ Y }} Proposed</th><th class="n">Change</th><th class="n">%</th></tr></thead>
          <tbody>
            {% for l in t.lines %}
            {% if l.prior_actual is none %}{% set ns.gaps = true %}{% else %}{% set ns.pa = ns.pa + l.prior_actual %}{% set ns.yt = ns.yt + (l.ytd_actual or 0) %}{% set ns.fc = ns.fc + (l.forecast or 0) %}{% endif %}
            {% set ns.cu = ns.cu + l.current %}{% set ns.pr = ns.pr + l.proposed %}
            <tr><td>{{ l.description }}</td>
              <td class="n">{{ fmt(l.prior_actual) if l.prior_actual is not none else '—' }}</td>
              <td class="n">{{ fmt(l.ytd_actual) if l.ytd_actual is not none else '—' }}</td>
              <td class="n">{{ fmt(l.current) }}</td>
              <td class="n">{{ fmt(l.forecast) if l.forecast is not none else '—' }}</td>
              <td class="n">{{ fmt(l.proposed) }}</td>
              <td class="n">{% if l.variance == 0 %}<span class="chip flat">no change</span>{% else %}<span class="chip {{ 'dn' if l.variance < 0 else 'up' }}">{{ '+' if l.variance > 0 else '−' }}{{ fmt(l.variance) | replace('-$', '$') | replace('−$', '$') }}</span>{% endif %}</td>
              <td class="n" style="color:var(--faint)">{% if l.variance == 0 %}—{% else %}{{ '%+.1f'|format(l.variance_pct) }}%{% endif %}</td></tr>
            {% endfor %}
            {% set dt = ns.pr - ns.cu %}
            <tr class="total"><td>Total {{ t.name | lower }}</td>
              <td class="n">{{ fmt(ns.pa) if not ns.gaps else '—' }}</td>
              <td class="n">{{ fmt(ns.yt) if not ns.gaps else '—' }}</td>
              <td class="n">{{ fmt(ns.cu) }}</td>
              <td class="n">{{ fmt(ns.fc) if not ns.gaps else '—' }}</td>
              <td class="n">{{ fmt(ns.pr) }}</td>
              <td class="n">{% if dt == 0 %}<span class="chip flat">no change</span>{% else %}<span class="chip {{ 'dn' if dt < 0 else 'up' }}">{{ '+' if dt > 0 else '−' }}{{ fmt(dt) | replace('-$', '$') | replace('−$', '$') }}</span>{% endif %}</td>
              <td class="n">{% if dt == 0 or not ns.cu %}—{% else %}{{ '%+.1f'|format(dt / ns.cu * 100) }}%{% endif %}</td></tr>
          </tbody>
        </table></div>
        {% elif t.rows is defined %}
        <div class="tbl-scroll"><table>
          <thead><tr><th>Line</th><th class="n">{{ Y - 2 }} Actual</th><th class="n">{{ Y - 1 }} Budget</th><th class="n">{{ Y }} Proposed</th></tr></thead>
          <tbody>
            {% for r in t.rows %}
            <tr><td>{{ r.label }}</td><td class="n">{{ fmt(r.col1_prior_actual) if r.col1_prior_actual is not none else '—' }}</td><td class="n">{{ fmt(r.col6_approved_budget) }}</td><td class="n">{{ fmt(r.col7_proposed_budget) }}</td></tr>
            {% endfor %}
          </tbody>
        </table></div>
        {% if snapshot.detail_tabs.meta and snapshot.detail_tabs.meta.recon %}{% set rc = snapshot.detail_tabs.meta.recon %}
        <p class="tbl-note">{% if rc.diff_exp %}The expense detail tabs total {{ fmt(rc.detail_exp) }}; this summary reflects Century&rsquo;s computed operating budget of {{ fmt(rc.summary_exp) }} — the {{ fmt(rc.diff_exp) }} difference comes from summary-level adjustments and lines not yet individually budgeted.{% endif %}{% if rc.diff_inc %} Income detail totals {{ fmt(rc.detail_inc) }} against a computed income total of {{ fmt(rc.summary_inc) }} ({{ fmt(rc.diff_inc) }} difference).{% endif %}</p>
        {% endif %}
        {% elif t.re_taxes is defined %}
        <div class="tbl-scroll"><table>
          <tbody>
            <tr><td>Gross real estate tax</td><td class="n">{{ fmt(t.re_taxes.gross_tax) }}</td></tr>
            <tr><td>First-half installment</td><td class="n">{{ fmt(t.re_taxes.first_half_tax) }}</td></tr>
            <tr><td>Second-half installment</td><td class="n">{{ fmt(t.re_taxes.second_half_tax) }}</td></tr>
            <tr class="total"><td>Net real estate tax</td><td class="n">{{ fmt(t.re_taxes.net_tax) }}</td></tr>
          </tbody>
        </table></div>
        {% endif %}
      </div>
      {% endfor %}
      <p class="tbl-note">{% if ytdm %}YTD reflects {{ ytdm }} month{{ 's' if ytdm != 1 else '' }} of {{ Y - 1 }} actuals · {% endif %}Forecast = YTD actuals + projected remaining months · Change compares the {{ Y }} proposal to the {{ Y - 1 }} budget.</p>
    </div>
    {% endif %}

    {% if snapshot.detail_tabs and snapshot.detail_tabs.tabs %}
    <div class="card wif" id="s-wif" style="display:none">
      <div class="card-head"><div><h2>What-if workshop</h2><div class="sub">Test the budget live: slide any line and watch the {{ words.charge_word or 'common charge' }} increase recalculate. For discussion only — nothing you change here is saved or sent to Century.</div></div></div>
      <div class="wif-band">
        <div class="wk hero"><div class="wk-l" id="wifWordLab">Increase needed to balance</div><div class="wk-v" id="wifPct">—</div><div class="wk-s" id="wifBase"></div></div>
        <div class="wk"><div class="wk-l">Total operating expenses</div><div class="wk-v" id="wifExp">—</div><div class="wk-s" id="wifMod">At Century proposal</div></div>
        <div class="wk"><div class="wk-l">Less: all other income</div><div class="wk-v" id="wifOther">—</div><div class="wk-s">held at Century proposal</div></div>
        <div class="wk"><div class="wk-l">Required to balance</div><div class="wk-v" id="wifReq">—</div><div class="wk-s" id="wifCur"></div></div>
        <button class="wif-reset" onclick="wifResetAll()">Reset to Century proposal</button>
      </div>
      <p class="tbl-note" id="wifCaution" style="display:none; color:#92400e; font-weight:600;">This draft shows an unusually large gap against current {{ words.charge_word or 'common charge' }}s — the budget build may still be in progress. Treat the number above as directional until Century confirms the draft is complete.</p>
      <div id="wifBody"></div>
      <p class="tbl-note">A balanced budget collects exactly what it spends: {{ words.charge_word or 'common charge' }}s must cover total operating expenses minus all other income. Lines the board cannot adjust here (and any schedule outside the detail tabs) are held at the Century proposal.</p>
    </div>
    {% endif %}

    {% if tl.board_review_through or tl.board_vote_by or tl.effective_date %}
    <div class="card" id="s-dates">
      <div class="card-head"><div><h2>Key dates</h2></div></div>
      <div class="daterow">
        {% if tl.board_review_through %}<div class="datebox"><div class="d">{{ tl.board_review_through }}</div><div class="l">Review by</div></div>{% endif %}
        {% if tl.board_vote_by %}<div class="datebox"><div class="d">{{ tl.board_vote_by }}</div><div class="l">Board vote by</div></div>{% endif %}
        {% if tl.effective_date %}<div class="datebox"><div class="d">{{ tl.effective_date }}</div><div class="l">New {{ words.charge_word or 'common charge' }}s effective</div></div>{% endif %}
      </div>
    </div>
    {% endif %}

    {% if snapshot.narrative.faq %}
    <div class="card" id="s-faq">
      <div class="card-head"><div><h2>Questions &amp; answers</h2><div class="sub">Prepared answers to the questions boards ask most</div></div></div>
      {% for item in snapshot.narrative.faq %}
      <details class="acc"{% if loop.first %} open{% endif %}><summary>{{ item.q }}</summary><div class="a">{{ item.a }}</div></details>
      {% endfor %}
    </div>
    {% endif %}

    <div class="sig"><b>Century Management Finance Team</b>On behalf of {{ snapshot.budget.building_name }} · Confidential — prepared for the Board</div>
  </div>
</div>

<script>
function showTab(i) {
  document.querySelectorAll('.pill').forEach(b => b.classList.toggle('on', b.dataset.i == i));
  document.querySelectorAll('.tpanel').forEach(p => p.classList.toggle('on', p.dataset.i == i));
}
var WIF_TABS = {{ (snapshot.detail_tabs.tabs if snapshot.detail_tabs and snapshot.detail_tabs.tabs else []) | tojson }};
var WIF_WORD = {{ (words.charge_word or 'common charge') | tojson }};
(function () {
  var card = document.getElementById('s-wif');
  var railLink = document.querySelector('#rail a[href="#s-wif"]');
  function bail() {
    if (card && card.parentNode) card.parentNode.removeChild(card);
    if (railLink && railLink.parentNode) railLink.parentNode.removeChild(railLink);
  }
  if (!card || !WIF_TABS || !WIF_TABS.length) { bail(); return; }
  var incomeTab = null, sumTab = null, expTabs = [];
  WIF_TABS.forEach(function (t) {
    if (t.lines && t.name === 'Income') incomeTab = t;
    else if (t.rows) sumTab = t;
    else if (t.lines && t.name !== 'Commercial' && t.name !== 'Capital') expTabs.push(t);
  });
  if (!incomeTab || !incomeTab.lines || !incomeTab.lines.length || !expTabs.length) { bail(); return; }
  var charge = null;
  incomeTab.lines.forEach(function (l) { if (!charge || (l.proposed || 0) > (charge.proposed || 0)) charge = l; });
  if (!charge || !(charge.current > 1) || !(charge.proposed > 0)) { bail(); return; }
  var incomeLinesSum = 0;
  incomeTab.lines.forEach(function (l) { incomeLinesSum += (l.proposed || 0); });
  var incomeProp = incomeLinesSum;
  if (sumTab && sumTab.rows) {
    sumTab.rows.forEach(function (r) {
      var v = r.col7_proposed_budget;
      // Sanity band (2026-07-05 sweep): partially built budgets can compute
      // absurd summary totals (829 income total = 1,500 against 3.16M of
      // visible income lines). Trust the Summary total only when it covers at
      // least the charge line and stays within twice the visible lines;
      // otherwise stay consistent with the numbers the reader can see.
      if (r.label === 'Total Income' && typeof v === 'number'
          && v >= (charge.proposed || 0) && v <= incomeLinesSum * 2) incomeProp = v;
    });
  }
  var otherIncome = incomeProp - (charge.proposed || 0);
  var lines = [];
  var negSum = 0;
  expTabs.forEach(function (t) {
    (t.lines || []).forEach(function (l) {
      var p = l.proposed || 0;
      if (p > 0.5) lines.push({ cat: t.name, d: l.description || '', prop: p, pct: 0 });
      else if (p < -0.5) negSum += p;
    });
  });
  if (!lines.length) { bail(); return; }
  var leverBase = 0;
  lines.forEach(function (l) { leverBase += l.prop; });
  var baseAll = leverBase + negSum;
  var fullExp = baseAll;
  if (sumTab && sumTab.rows && baseAll > 0) {
    sumTab.rows.forEach(function (r) {
      var v = r.col7_proposed_budget;
      // Same sweep on the expense side: computed totals can come back
      // negative (872), zero (829) or doubled (710) on unfinished builds.
      // Accept only a positive total within 0.9x to 2x of the visible lines.
      if (r.label === 'Total Expenses' && typeof v === 'number'
          && v > 0 && v >= baseAll * 0.9 && v <= baseAll * 2) fullExp = v;
    });
  }
  var fixedOther = fullExp - leverBase;
  function money(n) {
    var neg = n < 0; n = Math.round(Math.abs(n));
    return (neg ? '-$' : '$') + n.toLocaleString('en-US');
  }
  function adjVal(l) { return Math.round(l.prop * (1 + l.pct / 100)); }
  function balance() {
    var t = fixedOther;
    lines.forEach(function (l) { t += adjVal(l); });
    var req = t - otherIncome;
    var d = req - charge.current;
    return { exp: t, req: req, delta: d, pct: d / charge.current * 100 };
  }
  var BASE = balance();
  var cats = [];
  lines.forEach(function (l, i) { l.i = i; if (cats.indexOf(l.cat) < 0) cats.push(l.cat); });
  var h = '';
  cats.forEach(function (c, ci) {
    h += '<div class="wif-cat"><span>' + c + '</span><span id="wcs_' + ci + '"></span></div>';
    lines.forEach(function (l) {
      if (l.cat !== c) return;
      h += '<div class="wif-row">' +
        '<div class="wif-d">' + l.d + '<span class="wif-cent">Century: ' + money(l.prop) + '</span></div>' +
        '<div class="wif-lever">' +
          '<button class="wif-fine" onclick="wifFine(' + l.i + ',-0.5)" aria-label="decrease">−</button>' +
          '<input type="range" min="-15" max="15" step="0.5" value="0" id="ws_' + l.i + '" oninput="wifSlide(' + l.i + ', this.value)">' +
          '<button class="wif-fine" onclick="wifFine(' + l.i + ',0.5)" aria-label="increase">+</button>' +
          '<span class="wif-pct" id="wp_' + l.i + '">0.0%</span>' +
        '</div>' +
        '<div class="wif-val" id="wv_' + l.i + '">' + money(l.prop) + '</div>' +
        '<div class="wif-delta" id="wd_' + l.i + '">—</div>' +
      '</div>';
    });
  });
  if (Math.abs(fixedOther) > 0.5) {
    var foLabel = fixedOther >= 0
      ? 'Schedules outside the adjustable detail — held at the Century proposal'
      : 'Credits and adjustments (not adjustable) — held at the Century proposal';
    h += '<div class="wif-cat"><span>Everything else in this budget</span><span>' + money(fixedOther) + '</span></div>' +
      '<div class="wif-row wif-fixed"><div class="wif-d">' + foLabel + '</div>' +
      '<div class="wif-lever"></div><div class="wif-val">' + money(fixedOther) + '</div><div class="wif-delta">—</div></div>';
  }
  document.getElementById('wifBody').innerHTML = h;
  window.wifFine = function (i, step) {
    var l = lines[i];
    l.pct = Math.max(-15, Math.min(15, Math.round((l.pct + step) * 2) / 2));
    document.getElementById('ws_' + i).value = l.pct;
    wifPaint();
  };
  window.wifSlide = function (i, v) { lines[i].pct = parseFloat(v) || 0; wifPaint(); };
  window.wifResetAll = function () {
    lines.forEach(function (l) { l.pct = 0; document.getElementById('ws_' + l.i).value = 0; });
    wifPaint();
  };
  function wifPaint() {
    var mod = 0;
    lines.forEach(function (l) {
      var v = adjVal(l), d = v - l.prop;
      if (l.pct !== 0) mod++;
      document.getElementById('wv_' + l.i).textContent = money(v);
      var pe = document.getElementById('wp_' + l.i);
      pe.textContent = (l.pct > 0 ? '+' : '') + l.pct.toFixed(1) + '%';
      pe.className = 'wif-pct' + (l.pct > 0 ? ' up' : l.pct < 0 ? ' dn' : '');
      var de = document.getElementById('wd_' + l.i);
      var dr = l.pct === 0 ? 0 : Math.round(d);
      de.textContent = Math.abs(dr) < 1 ? '—' : (dr > 0 ? '+' : '−') + '$' + Math.abs(dr).toLocaleString('en-US');
      de.className = 'wif-delta' + (dr >= 1 ? ' up' : dr <= -1 ? ' dn' : '');
    });
    cats.forEach(function (c, ci) {
      var t = 0;
      lines.forEach(function (l) { if (l.cat === c) t += adjVal(l); });
      document.getElementById('wcs_' + ci).textContent = money(t);
    });
    var b = balance();
    var pe2 = document.getElementById('wifPct');
    pe2.textContent = (b.pct >= 0 ? '+' : '−') + Math.abs(b.pct).toFixed(1) + '%';
    pe2.style.color = b.pct <= 0 ? '#7fd1a8' : b.pct <= BASE.pct ? '#e8cf9e' : '#f0a0a0';
    document.getElementById('wifExp').textContent = money(b.exp);
    document.getElementById('wifReq').textContent = money(b.req);
    document.getElementById('wifMod').textContent = mod === 0 ? 'At Century proposal'
      : mod + ' line' + (mod === 1 ? '' : 's') + ' adjusted';
    var warn = document.getElementById('wifCaution');
    if (warn) warn.style.display = Math.abs(b.pct) > 25 ? '' : 'none';
  }
  document.getElementById('wifOther').textContent = '−$' + Math.round(otherIncome).toLocaleString('en-US');
  document.getElementById('wifCur').textContent = 'current: ' + money(charge.current);
  document.getElementById('wifBase').textContent = 'Century proposal: ' + (BASE.pct >= 0 ? '+' : '−') + Math.abs(BASE.pct).toFixed(1) + '%';
  document.getElementById('wifWordLab').textContent = WIF_WORD + ' increase needed to balance';
  card.style.display = '';
  wifPaint();
})();
(function () {
  const links = Array.from(document.querySelectorAll('#rail a'));
  if (!links.length) return;
  const secs = links.map(l => document.querySelector(l.getAttribute('href'))).filter(Boolean);
  const byId = {};
  links.forEach(l => byId[l.getAttribute('href').slice(1)] = l);
  let animId = null;
  function animateTo(top, ms) {
    const se = document.scrollingElement;
    const from = se.scrollTop, dist = top - from, t0 = performance.now();
    if (animId) cancelAnimationFrame(animId);
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) { se.scrollTop = top; return; }
    (function step(now) {
      const p = Math.min(1, (now - t0) / ms);
      const e2 = p < 0.5 ? 4 * p * p * p : 1 - Math.pow(-2 * p + 2, 3) / 2;
      se.scrollTop = from + dist * e2;
      if (p < 1) animId = requestAnimationFrame(step);
    })(t0);
  }
  const obs = new IntersectionObserver(es => es.forEach(e => {
    if (e.isIntersecting) { links.forEach(l => l.classList.remove('on')); (byId[e.target.id] || links[0]).classList.add('on'); }
  }), { rootMargin: '-20% 0px -65% 0px' });
  secs.forEach(s => obs.observe(s));
  links.forEach(l => l.addEventListener('click', e => {
    e.preventDefault();
    const el = document.querySelector(l.getAttribute('href'));
    animateTo(el.getBoundingClientRect().top + document.scrollingElement.scrollTop - 16, 450);
  }));
})();
</script>
</body>
</html>
"""
