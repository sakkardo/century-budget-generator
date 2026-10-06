"""Monthly Financial Snapshot: draft commentary and PDF rendering.

render_pdf(snapshot, commentary, board_note, signoff) -> PDF bytes. Pure reportlab, no Flask.
"""
import io
import os
from xml.sax.saxutils import escape as xesc

from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.shapes import Drawing
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Frame, Image, KeepTogether, PageBreak, PageTemplate,
                                Paragraph, Spacer, Table, TableStyle)

try:
    from snapshot_parser import account_kind
except ImportError:
    from budget_app.snapshot_parser import account_kind

RED = colors.HexColor("#A4262C")
INK = colors.HexColor("#221F1F")
MUTE = colors.HexColor("#6D6564")
LINE = colors.HexColor("#E4DEDC")
GOOD = colors.HexColor("#2F6B4F")
SOFT = colors.HexColor("#F7E3E2")
LOGO = os.path.join(os.path.dirname(__file__), "brand", "century_logo_dark.png")
MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
               "October", "November", "December"]


def money(n, sign=False):
    if n is None:
        return ""
    s = "{:,}".format(abs(int(n)))
    if n < 0:
        return "-$" + s if not sign else "-" + s
    return "$" + s if not sign else s


def vtxt(v):
    return ("%s %s" % (money(abs(v)), "ahead" if v > 0 else "behind")) if v else "on budget"


def num(n):
    return "{:,}".format(int(n)) if n >= 0 else "-{:,}".format(abs(int(n)))


def _fav(v):
    """Variance as the statement states it: positive is favorable to budget."""
    return v


# ---------------------------------------------------------------- commentary
# A note is a dict: key (stable id across months), title, label (item name), facts (system sentence, refreshed every
# month), text (the FA's explanation), variance (this month's YTD variance for the item), settled (the whole year's
# budget is already booked and nothing posted this month: insurance, tax instalments), status (new / worse /
# continuing), since (month first explained), basis (variance when the explanation was written).
WORSE_PCT = 0.10    # Jacob 2026-10-03: a known item is "worse" when it moved unfavorably by more than 10%
WORSE_ABS = 5000    # ...and by more than $5,000 since it was explained
CARRIED = ("income", "cat:", "unposted:", "saving:")  # 'overall' is always written fresh
FLAG_YTD_PCT = 0.10  # Jacob 2026-10-03: an expense line gets a note only when it is more than 10% over its YTD budget
FLAG_MTD_PCT = 0.15  # ...or more than 15% over this month's budget


SCOPES = (("ytd", "Year to date"), ("month", "This month only"))
SCOPE_HINT = {"ytd": "Over budget for the year so far.",
              "month": "Over budget this month but within budget for the year, usually timing."}
SCOPE_COLORS = {"ytd": ("#E6EEFB", "#1F3F8F"), "month": ("#FCEFD2", "#7A4A00")}  # band fill, band text


def note_scope(n):
    """'ytd' (judged on the year, the board's focus), 'month' (over this month only, usually timing) or None
    (the overall headline). Notes made before 2026-10-05 have no scope field, so read it from the title."""
    if n.get("scope"):
        return n["scope"]
    if n.get("key") == "overall" or n.get("title") == "Overall":
        return None
    t = n.get("title") or ""
    if "year to date" not in t and any(" in %s" % m in t for m in MONTH_NAMES):
        return "month"
    return "ytd"


def _over_pct(var, budget):
    """How far over budget, as a fraction (0 when favorable or there is no budget to measure against)."""
    return -var / float(budget) if var < 0 and budget > 0 else 0.0


def flagged(c):
    """(ytd_pct, mtd_pct) when the expense line crosses either threshold, else None."""
    y, m = _over_pct(c["ytd_var"], c["ytd_budget"]), _over_pct(c["month_var"], c["month_budget"])
    return (y, m) if y > FLAG_YTD_PCT or m > FLAG_MTD_PCT else None


def income_flagged(d):
    """Same thresholds for income, in either direction: a big shortfall or a big windfall both get a note."""
    y = abs(d["ytd_var"]) / float(d["ytd_budget"]) if d["ytd_budget"] > 0 else 0.0
    m = abs(d["month_var"]) / float(d["month_budget"]) if d["month_budget"] > 0 else 0.0
    return (y, m) if y > FLAG_YTD_PCT or m > FLAG_MTD_PCT else None


def _note(key, title, facts, variance=0, label="", settled=False, draft=True, scope="ytd"):
    return {"key": key, "title": title, "label": label or title, "facts": facts, "text": "", "variance": variance,
            "settled": settled, "status": "new", "draft": draft, "scope": scope}


def note_body(n):
    """What prints for a note: the refreshed facts, then the FA's explanation."""
    return " ".join(x for x in ((n.get("facts") or "").strip(), (n.get("text") or "").strip()) if x)


def classify_notes(notes, prior, prior_month):
    """Compare this month's notes with the building's confirmed notes from the previous month.

    prior: {key: note} confirmed last month. Returns (notes, resolved_labels). Continuing and worse notes carry the
    previous explanation; continuing keeps the original basis so slow creep is still measured from what was explained.
    """
    keys = {n["key"] for n in notes}
    for n in notes:
        p = prior.get(n["key"]) if n["key"].startswith(CARRIED) else None
        if not p:
            n["status"] = "new"
            continue
        basis = p.get("basis", p.get("variance", 0))
        n.update({"text": p.get("text", ""), "prior_text": p.get("text", ""), "since": p.get("since") or prior_month,
                  "basis": basis, "draft": False})
        drop = basis - n["variance"]  # positive = moved unfavorably (more over budget, or a smaller saving)
        if n.get("settled") or not (drop > WORSE_ABS and drop > WORSE_PCT * abs(basis)):
            n["status"] = "continuing"
        else:
            n["status"], n["moved"] = "worse", drop
    resolved = [p.get("label") or p.get("title") for k, p in prior.items()
                if k not in keys and k.startswith(("income", "cat:", "unposted:"))]
    return notes, resolved


def draft_commentary(s):
    """Rule-based first draft of the 'what changed' notes. The FA adds the reason and confirms before sign-off."""
    out = []
    ytd_noi, ytd_net = s["noi"]["ytd_actual"], s["net_income"]["ytd_actual"]
    bud_noi = s["noi"]["ytd_budget"]
    over = [c for c in s["categories"] if flagged(c)]
    # year-to-date problems first (the board's focus), then lines over this month only
    over.sort(key=lambda c: (flagged(c)[0] <= FLAG_YTD_PCT, c["ytd_var"] if flagged(c)[0] > FLAG_YTD_PCT else c["month_var"]))
    under = sorted([c for c in s["categories"] if c["ytd_var"] > 0 and c["ytd_actual"] > 0], key=lambda c: -c["ytd_var"])
    unposted = [c for c in s["categories"] if c["ytd_actual"] == 0 and c["ytd_budget"] > 0]

    head = "Year to date, net operating income is %s against a budget of %s." % (money(ytd_noi), ("a deficit of " + money(abs(bud_noi))) if bud_noi < 0 else money(bud_noi))
    nonop = s["nonop_expense"]["ytd_actual"] if s["nonop_expense"] else 0
    nonop_inc = s["nonop_income"]["ytd_actual"] if s["nonop_income"] else 0
    if nonop or nonop_inc:
        parts = []
        if nonop:
            parts.append("%s of capital and other non-operating expense" % money(nonop))
        if nonop_inc:
            parts.append("%s of non-operating income" % money(nonop_inc))
        head += " After %s, net income is %s." % (" and ".join(parts), money(ytd_net))
    out.append(_note("overall", "Overall", head, draft=False, scope=None))

    inc = s["income"]
    inc_fl = income_flagged(inc)
    if inc_fl:
        total_var, ab = inc["ytd_var"], lambda v: "above" if v > 0 else "below"
        mname = MONTH_NAMES[s["meta"]["month"] - 1]
        ytd_scope = inc_fl[0] > FLAG_YTD_PCT
        if ytd_scope:
            title = "Income, %s %s budget year to date" % (money(abs(total_var)), ab(total_var))
            lead = "%.0f%% %s budget year to date (%s)." % (100 * inc_fl[0], ab(total_var), money(abs(total_var)))
            if inc_fl[1] > FLAG_MTD_PCT:
                lead += " %s alone was %s %s (%.0f%%)." % (mname, money(abs(inc["month_var"])), ab(inc["month_var"]), 100 * inc_fl[1])
        else:  # flagged on the month alone
            title = "Income, %s %s budget in %s" % (money(abs(inc["month_var"])), ab(inc["month_var"]), mname)
            lead = "%s %s budget in %s (%.0f%%). Year to date it is %s %s budget." % (
                money(abs(inc["month_var"])), ab(inc["month_var"]), mname, 100 * inc_fl[1], money(abs(total_var)), ab(total_var))
        drv = s["income_drivers"]
        if drv and total_var and abs(drv[0]["ytd_var"]) >= abs(total_var) * 0.5:
            d, rest = drv[0], total_var - drv[0]["ytd_var"]
            facts = "%s accounts for %s of the year-to-date difference. Without it, income is %s %s budget." % (
                d["name"], money(abs(d["ytd_var"])), money(abs(rest)), ab(rest))
        else:
            facts = ("Largest differences year to date: %s." % ", ".join("%s (%s)" % (d["name"], money(d["ytd_var"])) for d in drv[:3])) if drv else ""
        out.append(_note("income", title, (lead + " " + facts).strip(), variance=total_var, label="Income", draft=False,
                         scope="ytd" if ytd_scope else "month"))
    for c in over[:3]:
        items = ", ".join("%s (%s)" % (i["name"], money(i["ytd_var"])) for i in c["worst_items"][:3])
        mname = MONTH_NAMES[s["meta"]["month"] - 1]
        ypct, mpct = flagged(c)
        settled = c.get("annual_budget", 0) > 0 and c["ytd_budget"] >= c["annual_budget"] - 1 and c["month_actual"] == 0
        if ypct > FLAG_YTD_PCT:  # judged on the year: lead with the year, the month is context
            scope = "ytd"
            title = "%s, %s over budget year to date" % (c["name"], money(abs(c["ytd_var"])))
            facts = "%.0f%% over budget year to date (%s)." % (100 * ypct, money(abs(c["ytd_var"])))
            if c["month_var"] < 0:
                facts += " %s alone was %s over" % (mname, money(abs(c["month_var"]))) + (" (%.0f%%)." % (100 * mpct) if mpct else ".")
            if items:
                facts += " Largest lines year to date: %s." % items
        else:  # over this month only; the year is still within budget
            scope = "month"
            title = "%s, %s over budget in %s" % (c["name"], money(abs(c["month_var"])), mname)
            facts = "%s over budget in %s (%.0f%%). Year to date it is still %s %s budget." % (
                money(abs(c["month_var"])), mname, 100 * mpct, money(abs(c["ytd_var"])), "under" if c["ytd_var"] >= 0 else "over")
        out.append(_note("cat:" + c["name"], title, facts, variance=c["ytd_var"], label=c["name"], settled=settled, scope=scope))
    for c in unposted:
        out.append(_note("unposted:" + c["name"], "%s, nothing recorded year to date against a %s budget" % (c["name"], money(c["ytd_budget"])),
                         "No expense has been recorded on this line so far this year.", variance=c["ytd_var"], label=c["name"]))
    if under:
        u = under[0]
        out.append(_note("saving:" + u["name"], "Largest saving: %s, %s under budget" % (u["name"], money(u["ytd_var"])),
                         "This is %.0f%% of its year-to-date budget." % (100.0 * u["ytd_var"] / u["ytd_budget"] if u["ytd_budget"] else 0),
                         variance=u["ytd_var"], label=u["name"]))
    return out


# ---------------------------------------------------------------- charts
def _cash_chart(s, width, height):
    hist, months = s["cash"].get("history"), s["cash"].get("history_months")
    if not hist:
        return None
    d = Drawing(width, height)
    lp = LinePlot()
    lp.x, lp.y, lp.width, lp.height = 48, 22, width - 60, height - 34
    lp.data = [list(enumerate(hist))]
    lp.lines[0].strokeColor = RED
    lp.lines[0].strokeWidth = 1.8
    lo, hi = min(hist), max(hist)
    pad = (hi - lo) * 0.15 or hi * 0.1
    lp.yValueAxis.valueMin, lp.yValueAxis.valueMax = max(0, lo - pad), hi + pad
    lp.yValueAxis.labelTextFormat = lambda v: "$%.1fM" % (v / 1e6) if hi > 2e6 else "$%dK" % (v / 1e3)
    lp.yValueAxis.labels.fontSize = 7
    lp.yValueAxis.labels.fontName = "Helvetica"
    lp.xValueAxis.labels.fontName = "Helvetica"
    lp.yValueAxis.gridStrokeColor = LINE
    lp.yValueAxis.visibleGrid = True
    lp.yValueAxis.strokeColor = colors.white
    lp.xValueAxis.valueMin, lp.xValueAxis.valueMax = 0, len(hist) - 1
    lp.xValueAxis.valueSteps = list(range(0, len(hist), max(1, len(hist) // 6)))
    labs = [m.split()[0] for m in (months or [str(i) for i in range(len(hist))])]
    lp.xValueAxis.labelTextFormat = lambda i: labs[int(i)] if 0 <= int(i) < len(labs) else ""
    lp.xValueAxis.labels.fontSize = 7
    d.add(lp)
    return d


# ---------------------------------------------------------------- pdf
def render_pdf(s, commentary=None, board_note="", signoff=None, status_label="DRAFT", reviewed=None, resolved=None):
    """reviewed: {"by", "at"} once the FA has confirmed every note; printed under the notes.
    resolved: labels of items explained last month that are no longer over budget."""
    buf = io.BytesIO()
    W, H = letter
    m = s["meta"]
    month_name = MONTH_NAMES[m["month"] - 1]
    title = "%s %s" % (month_name, m["year"])
    building = m.get("building") or ""
    commentary = draft_commentary(s) if commentary is None else commentary
    gutter = 0.7 * inch
    cw = W - 2 * gutter

    st = lambda name, **kw: ParagraphStyle(name, fontName=kw.pop("fn", "Helvetica"), **kw)
    h1 = st("h1", fn="Helvetica-Bold", fontSize=19, leading=22, textColor=INK)
    h2 = st("h2", fn="Helvetica-Bold", fontSize=11.5, leading=14, textColor=INK, spaceBefore=12, spaceAfter=5)
    body = st("b", fontSize=9, leading=12.5, textColor=INK)
    small = st("s", fontSize=7.5, leading=10, textColor=MUTE)
    cell = st("c", fontSize=8, leading=10, textColor=INK)
    cellr = st("cr", fontSize=8, leading=10, textColor=INK, alignment=2)

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(RED)
        canvas.setLineWidth(2.5)
        canvas.line(0, H - 4, W, H - 4)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(MUTE)
        canvas.drawString(gutter, 0.45 * inch, "%s  |  Monthly Financial Snapshot, %s" % (building, title))
        canvas.drawRightString(W - gutter, 0.45 * inch, "Page %d" % doc.page)
        if status_label != "APPROVED":
            canvas.setFont("Helvetica-Bold", 8)
            canvas.setFillColor(RED)
            canvas.drawRightString(W - gutter, H - 0.4 * inch, status_label)
        canvas.restoreState()

    doc = BaseDocTemplate(buf, pagesize=letter, leftMargin=gutter, rightMargin=gutter, topMargin=0.55 * inch,
                          bottomMargin=0.7 * inch, title="%s Monthly Financial Snapshot %s" % (building, title),
                          author="Century Management")
    doc.addPageTemplates([PageTemplate(id="p", frames=[Frame(gutter, 0.7 * inch, cw, H - 1.25 * inch, 0, 0, 0, 0)], onPage=footer)])
    f = []

    # ---- page 1
    logo = Image(LOGO, width=1.6 * inch, height=1.6 * inch * 77 / 429)
    head = Table([[logo, Paragraph("<b>Monthly Financial Snapshot</b><br/>%s" % title, st("hr", fontSize=10, leading=13, textColor=MUTE, alignment=2))]],
                 colWidths=[cw * 0.5, cw * 0.5])
    head.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0)]))
    f += [head, Spacer(1, 6), Paragraph(building, h1), Spacer(1, 8)]

    inc, exp, noi, net = s["income"], s["expenses"], s["noi"], s["net_income"]

    def kpi(label, value, sub):
        return [Paragraph(label.upper(), st("kl", fontSize=6.5, leading=8, textColor=MUTE)),
                Paragraph("<b>%s</b>" % value, st("kv", fontSize=15, leading=18, textColor=INK)),
                Paragraph(sub, st("kd", fontSize=7, leading=9, textColor=MUTE))]

    def signed(v, show_plus=True):  # green when positive (favorable), red when negative (Jacob 2026-10-06)
        txt = ("+" if v > 0 and show_plus else "") + money(v)
        return '<font color="%s">%s</font>' % ("#2F6B4F" if v > 0 else ("#A4262C" if v < 0 else "#221F1F"), txt)
    k = [kpi("Income, YTD variance", signed(inc["ytd_var"]), "Actual %s, budget %s" % (money(inc["ytd_actual"]), money(inc["ytd_budget"]))),
         kpi("Expenses, YTD variance", signed(exp["ytd_var"]), "Actual %s, budget %s" % (money(exp["ytd_actual"]), money(exp["ytd_budget"]))),
         kpi("Net operating income, YTD", signed(noi["ytd_actual"], False), "Budget %s (%s)" % (money(noi["ytd_budget"]), vtxt(noi["ytd_var"])))]
    kt = Table([k], colWidths=[cw / 3.0] * 3)
    kt.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 0.5, LINE), ("INNERGRID", (0, 0), (-1, -1), 0.5, LINE),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 7),
                            ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))
    f += [kt, Paragraph("Income and expenses against budget", h2)]

    hdr = ["", "%s actual" % month_name[:3], "Budget", "Variance", "YTD actual", "YTD budget", "YTD variance", ""]
    rows = [hdr]
    flags = []

    def vcell(v, bold=False):
        c = GOOD if v > 0 else (RED if v < 0 else INK)
        return Paragraph('<font color="%s">%s</font>' % (c.hexval().replace("0x", "#"), ("+" if v > 0 else "") + num(v)), cellr)

    def line(label, d, bold=False, flag=""):
        lab = Paragraph("<b>%s</b>" % label if bold else label, cell)
        return [lab, num(d["month_actual"]), num(d["month_budget"]), vcell(d["month_var"]),
                num(d["ytd_actual"]), num(d["ytd_budget"]), vcell(d["ytd_var"]), flag]

    rows.append(line("Total income", inc, True))
    tot_rows = [1]
    for c in s["categories"]:
        fl = ""
        fg = flagged(c)
        if fg:
            fl = "%.0f%% over" % (100 * fg[0]) if fg[0] > FLAG_YTD_PCT else "%.0f%% over in %s" % (100 * fg[1], month_name[:3])
            flags.append(len(rows))
        rows.append(line(c["name"], c, False, fl))
    rows.append(line("Total expenses", exp, True)); tot_rows.append(len(rows) - 1)
    rows.append(line("Net operating income", noi, True)); tot_rows.append(len(rows) - 1)
    if s["nonop_income"] and s["nonop_income"]["ytd_actual"]:
        rows.append(line("Non-operating income", s["nonop_income"]))
    if s["nonop_expense"] and (s["nonop_expense"]["ytd_actual"] or s["nonop_expense"]["ytd_budget"]):
        rows.append(line("Non-operating and capital expense", s["nonop_expense"]))
    rows.append(line("Net income", net, True)); tot_rows.append(len(rows) - 1)

    tbl = Table(rows, colWidths=[cw * 0.25, cw * 0.1, cw * 0.1, cw * 0.1, cw * 0.12, cw * 0.12, cw * 0.12, cw * 0.09], repeatRows=1)
    ts = [("FONT", (0, 0), (-1, -1), "Helvetica", 8), ("FONT", (0, 0), (-1, 0), "Helvetica", 6.5),
          ("TEXTCOLOR", (0, 0), (-1, 0), MUTE), ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
          ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, 1), (-1, -2), 0.3, LINE),
          ("TOPPADDING", (0, 0), (-1, -1), 2.2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.2),
          ("LEFTPADDING", (0, 0), (0, -1), 0), ("TEXTCOLOR", (-1, 1), (-1, -1), RED), ("FONT", (-1, 1), (-1, -1), "Helvetica-Bold", 7)]
    for r in tot_rows:
        ts += [("FONT", (1, r), (-1, r), "Helvetica-Bold", 8), ("LINEABOVE", (0, r), (-1, r), 0.8, INK)]
    tbl.setStyle(TableStyle(ts))
    f += [tbl, Spacer(1, 4),
          Paragraph("Green is favorable to budget and red is unfavorable. A line is flagged only when it is more than 10 percent over its year-to-date budget or more than 15 percent over this month's budget. Figures are whole dollars from the monthly financial statement.", small)]

    # ---- page 2
    # New and worse items in full; known items that have not moved in one compact table (they don't repeat each month)
    items = [c for c in commentary if not (c.get("key") == "overall" or c["title"] == "Overall")]
    fresh = [c for c in items if c.get("status") != "continuing"]
    ongoing = [c for c in items if c.get("status") == "continuing"]
    f.append(Paragraph("New notes", h2))
    if not fresh:
        f.append(Paragraph("No new notes this month. Earlier explanations are listed under Previous notes.", body))
    sub = st("sub", fn="Helvetica-Bold", fontSize=8, leading=10, textColor=MUTE)
    for code, label in SCOPES:
        group = [c for c in fresh if note_scope(c) == code]
        if not group:
            continue
        hint = SCOPE_HINT.get(code)
        fill, ink = SCOPE_COLORS[code]
        band = Paragraph('<font color="%s"><b>%s</b>&nbsp;&nbsp;<font name="Helvetica" size="7.5">%s</font></font>' % (
            ink, label.upper(), xesc(hint or "")), sub)
        f.append(Table([[band]], colWidths=[cw], style=[
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(fill)), ("LINEBEFORE", (0, 0), (0, -1), 3, colors.HexColor(ink)),
            ("LEFTPADDING", (0, 0), (-1, -1), 7), ("TOPPADDING", (0, 0), (-1, -1), 1.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5)]))
        f.append(Spacer(1, 2))
        for c in group:
            tag = ""
            if c.get("status") == "worse":
                tag = ' <font color="#A4262C" size="7.5">MOVED: explained in %s, %s more since</font>' % (
                    xesc(c.get("since") or "an earlier month"), money(c.get("moved", 0)))
            f.append(KeepTogether([Paragraph("<b>%s</b>%s" % (xesc(c["title"]), tag), body), Paragraph(xesc(note_body(c)), body), Spacer(1, 3)]))
    if ongoing:
        f.append(Paragraph("Previous notes", h2))
        orow = [["Item", "YTD variance", "Explained", "Explanation"]]
        for c in ongoing:
            orow.append([Paragraph(xesc(c.get("label") or c["title"]) + (" (this month)" if note_scope(c) == "month" else ""), cell),
                         num(c.get("variance", 0)),
                         Paragraph(xesc(c.get("since") or ""), cell),
                         Paragraph(xesc((c.get("text") or "").strip() or (c.get("facts") or "")), cell)])
        ot = Table(orow, colWidths=[cw * 0.2, cw * 0.13, cw * 0.13, cw * 0.54])
        ot.setStyle(TableStyle([("FONT", (0, 0), (-1, -1), "Helvetica", 8), ("FONT", (0, 0), (-1, 0), "Helvetica", 6.5),
                                ("TEXTCOLOR", (0, 0), (-1, 0), MUTE), ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                                ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, 1), (-1, -1), 0.3, LINE),
                                ("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (0, -1), 0),
                                ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]))
        f.append(ot)
    if resolved:
        f.append(Spacer(1, 4))
        f.append(Paragraph("No longer flagged since last month: %s." % xesc(", ".join(resolved)), small))
    if reviewed:
        f.append(Paragraph("Commentary reviewed and confirmed by %s (Financial Analyst), %s." % (
            xesc(reviewed["by"]), xesc(reviewed["at"].split(" ")[0] + " " + " ".join(reviewed["at"].split(" ")[1:3]).rstrip(","))), small))
        f.append(Spacer(1, 6))
    if board_note:
        f.append(Table([[Paragraph("<b>Note to the board.</b> %s" % xesc(board_note), body)]], colWidths=[cw],
                       style=[("BACKGROUND", (0, 0), (-1, -1), SOFT), ("BOX", (0, 0), (-1, -1), 0.5, RED), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

    f.append(CondPageBreak(2.2 * inch))  # new page only when capital would not fit; no half-empty pages
    f.append(Paragraph("Capital expenditures", h2))
    cap = [c for c in s["capital"]]
    if cap:
        crow = [["Project", "%s" % month_name[:3], "YTD", "YTD budget", "Annual budget"]]
        for c in cap:
            crow.append([c["project"], num(c["month"]), num(c["ytd"]), num(c["budget_ytd"]), num(c["annual_budget"])])
        ne = s["nonop_expense"] or {}
        crow.append(["Total", num(ne.get("month_actual", 0)), num(ne.get("ytd_actual", 0)), num(ne.get("ytd_budget", 0)), num(sum(c["annual_budget"] for c in cap))])
        ct = Table(crow, colWidths=[cw * 0.4, cw * 0.14, cw * 0.15, cw * 0.15, cw * 0.16])
        ct.setStyle(TableStyle([("FONT", (0, 0), (-1, -1), "Helvetica", 8), ("FONT", (0, 0), (-1, 0), "Helvetica", 6.5),
                                ("TEXTCOLOR", (0, 0), (-1, 0), MUTE), ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                                ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, 1), (-1, -2), 0.3, LINE),
                                ("FONT", (0, -1), (-1, -1), "Helvetica-Bold", 8), ("LINEABOVE", (0, -1), (-1, -1), 0.8, INK),
                                ("LEFTPADDING", (0, 0), (0, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 1.8), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.8)]))
        f.append(ct)
    else:
        f.append(Paragraph("No capital or other non-operating expenses are recorded this year to date.", body))
    ca = s["capital_assessment"]
    if ca and (ca["ytd_actual"] or ca["ytd_budget"]):
        f.append(Paragraph("Capital assessment collected year to date: %s (budget %s)." % (money(ca["ytd_actual"]), money(ca["ytd_budget"])), body))

    # cash, arrears and payables: three sections (Jacob 2026-10-06)
    def ledger(rows, widths, total=False, bold_row=None):
        t = Table(rows, colWidths=widths, hAlign="LEFT")
        ts = [("FONT", (0, 0), (-1, -1), "Helvetica", 8), ("FONT", (0, 0), (-1, 0), "Helvetica", 6.5),
              ("TEXTCOLOR", (0, 0), (-1, 0), MUTE), ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
              ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, 1), (-1, -2 if total else -1), 0.3, LINE),
              ("LEFTPADDING", (0, 0), (0, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 1.8), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.8)]
        if total:
            ts += [("FONT", (0, -1), (-1, -1), "Helvetica-Bold", 8), ("LINEABOVE", (0, -1), (-1, -1), 0.8, INK)]
        if bold_row is not None:
            ts += [("FONT", (0, bold_row), (-1, bold_row), "Helvetica-Bold", 8)]
        t.setStyle(TableStyle(ts))
        return t

    cash = s["cash"]
    mlab = lambda off: MONTH_NAMES[(m["month"] - 1 - off) % 12][:3]
    months3 = ["", mlab(2), mlab(1), mlab(0)]
    if cash["accounts"]:
        f.append(Paragraph("Cash", h2))
        arow = [["Account", "Start of month", "Month end"]]
        for a in cash["accounts"]:
            if a["end"] or a["begin"]:
                arow.append([Paragraph(a["name"], cell), num(a["begin"]), num(a["end"])])
        arow.append(["Total", num(cash["total"]["begin"]), num(cash["total"]["end"])])
        f.append(ledger(arow, [cw * 0.4, cw * 0.15, cw * 0.15], total=True))
        kinds = {}
        for a in cash["accounts"]:
            kinds[account_kind(a["name"])] = kinds.get(account_kind(a["name"]), 0) + a["end"]
        order = [("operating", "Operating"), ("reserve", "Reserves"), ("money market", "Money market"),
                 ("other", "Other"), ("security", "Security deposits")]
        parts = ["%s %s" % (lab, money(kinds[k])) for k, lab in order if kinds.get(k)]
        f += [Spacer(1, 3), Paragraph(" &nbsp;|&nbsp; ".join(parts), small)]
        cc = _cash_chart(s, cw, 74)
        if cc:
            f += [Spacer(1, 6), Paragraph("Cash at month end, last twelve months (excluding security deposits)", small), cc]
    if len(cash["arrears"]) == 3:
        ar = cash["arrears"]
        f.append(Paragraph("Arrears", h2))
        f.append(ledger([months3,
                         ["Receivable"] + [num(x) for x in ar["Accounts Receivable"]],
                         ["Prepaid"] + [num(x) for x in ar["Prepaid"]],
                         ["Net arrears"] + [num(x) for x in ar["Total Arrears"]]],
                        [cw * 0.4, cw * 0.15, cw * 0.15, cw * 0.15], bold_row=3))
    if cash["ap"]:
        f.append(Paragraph("Payables", h2))
        f.append(ledger([months3, ["Accounts payable"] + [num(x) for x in cash["ap"]]],
                        [cw * 0.4, cw * 0.15, cw * 0.15, cw * 0.15]))

    # sign-off: one panel per signer, name over a signature line, then the date signed (Jacob 2026-10-06)
    so = signoff or {}
    lab = st("sl", fn="Helvetica-Bold", fontSize=6.5, leading=8, textColor=MUTE)
    nm = st("sn", fn="Helvetica-Bold", fontSize=10.5, leading=13, textColor=INK)
    when = st("sw", fontSize=8, leading=10, textColor=INK)

    def panel(role, who):
        who = who or {}
        name = xesc(who.get("name") or "Not assigned")
        if who.get("signed_at"):
            status = '<font color="#2F6B4F"><b>Signed</b></font> %s' % xesc(who["signed_at"])
        else:
            status = '<font color="#6D6564">Awaiting sign-off</font>'
        return [Paragraph(role.upper(), lab), Paragraph(name, nm), Paragraph(status, when)]
    fa_p, pm_p = panel("Financial Analyst", so.get("fa")), panel("Property Manager", so.get("pm"))
    gap = cw * 0.08
    sign = Table([[fa_p[0], "", pm_p[0]], [fa_p[1], "", pm_p[1]], [fa_p[2], "", pm_p[2]]],
                 colWidths=[(cw - gap) / 2, gap, (cw - gap) / 2],
                 style=[("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                        ("TOPPADDING", (0, 0), (-1, -1), 1), ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                        ("BOTTOMPADDING", (0, 1), (-1, 1), 5), ("TOPPADDING", (0, 2), (-1, 2), 4),
                        ("LINEBELOW", (0, 1), (0, 1), 0.8, INK), ("LINEBELOW", (2, 1), (2, 1), 0.8, INK)])
    f.append(KeepTogether([Paragraph("Sign-off", h2), Spacer(1, 6), sign, Spacer(1, 8),
                           Paragraph("Prepared and reviewed by Century Management.", small)]))
    doc.build(f)
    return buf.getvalue()
