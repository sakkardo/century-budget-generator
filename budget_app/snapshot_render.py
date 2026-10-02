"""Monthly Financial Snapshot: draft commentary and PDF rendering.

render_pdf(snapshot, commentary, board_note, signoff) -> PDF bytes. Pure reportlab, no Flask.
"""
import io
import os
from xml.sax.saxutils import escape as xesc

from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (BaseDocTemplate, Frame, Image, KeepTogether, PageBreak, PageTemplate,
                                Paragraph, Spacer, Table, TableStyle)

try:
    from snapshot_parser import account_kind, is_security_account
except ImportError:
    from budget_app.snapshot_parser import account_kind, is_security_account

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
def draft_commentary(s):
    """Rule-based first draft of the 'what changed' notes. The FA edits before sign-off."""
    out = []
    ytd_noi, ytd_net = s["noi"]["ytd_actual"], s["net_income"]["ytd_actual"]
    bud_noi = s["noi"]["ytd_budget"]
    over = [c for c in s["categories"] if c["ytd_var"] <= -5000 or
            (c["ytd_var"] < 0 and c["ytd_budget"] and -c["ytd_var"] / c["ytd_budget"] > 0.05)]
    over.sort(key=lambda c: c["ytd_var"])
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
    out.append({"title": "Overall", "text": head, "draft": False})

    if s["income_drivers"]:
        d = s["income_drivers"][0]
        total_var = s["income"]["ytd_var"]
        if total_var and abs(d["ytd_var"]) >= abs(total_var) * 0.5 and abs(total_var) > 5000:
            rest = total_var - d["ytd_var"]
            out.append({"title": "Income, %s %s budget year to date" % (
                money(abs(total_var)), "above" if total_var > 0 else "below"),
                "text": "%s accounts for %s of the difference. Without it, income is %s %s budget." % (
                    d["name"], money(abs(d["ytd_var"])), money(abs(rest)), "above" if rest > 0 else "below"),
                "draft": False})
    for c in over[:3]:
        items = ", ".join("%s (%s)" % (i["name"], money(i["ytd_var"])) for i in c["worst_items"][:3])
        m = "Over budget by %s in %s. " % (money(abs(c["month_var"])), MONTH_NAMES[s["meta"]["month"] - 1]) if c["month_var"] < 0 else ""
        out.append({"title": "%s, %s over budget year to date" % (c["name"], money(abs(c["ytd_var"]))),
                    "text": (m + ("Largest lines: %s." % items if items else "")).strip(), "draft": True})
    for c in unposted:
        out.append({"title": "%s, nothing recorded year to date against a %s budget" % (c["name"], money(c["ytd_budget"])),
                    "text": "No expense has been recorded on this line so far this year.", "draft": True})
    if under:
        u = under[0]
        out.append({"title": "Largest saving: %s, %s under budget" % (u["name"], money(u["ytd_var"])),
                    "text": "This is %.0f%% of its year-to-date budget." % (
                        100.0 * u["ytd_var"] / u["ytd_budget"] if u["ytd_budget"] else 0), "draft": True})
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


def _capital_chart(items, width):
    items = [i for i in items if i["ytd"]]
    if not items:
        return None
    items = sorted(items, key=lambda i: -i["ytd"])[:6]
    rowh = 16
    d = Drawing(width, rowh * len(items) + 4)
    top = max(i["ytd"] for i in items)
    barmax = width - 190
    for k, it in enumerate(items):
        y = rowh * (len(items) - 1 - k) + 4
        d.add(String(0, y + 3, it["project"][:28], fontSize=8, fontName="Helvetica", fillColor=INK))
        w = max(2, barmax * it["ytd"] / top)
        d.add(Rect(120, y, w, 12, fillColor=RED, strokeColor=None))
        d.add(String(124 + w, y + 3, money(it["ytd"]), fontSize=8, fontName="Helvetica", fillColor=MUTE))
    return d


# ---------------------------------------------------------------- pdf
def render_pdf(s, commentary=None, board_note="", signoff=None, status_label="DRAFT", reviewed=None):
    """reviewed: {"by", "at"} once the FA has confirmed every note; printed under the notes."""
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
    read = st("read", fn="Times-Roman", fontSize=12, leading=15.5, textColor=INK)
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
        blocked = [c for c in s["checks"] if c["status"] != "tied"]
        tie = "All %d statement checks tied" % len(s["checks"]) if not blocked else "%d of %d checks need attention" % (len(blocked), len(s["checks"]))
        canvas.drawString(gutter, 0.45 * inch, "%s  |  Monthly Financial Snapshot, %s  |  %s" % (building, title, tie))
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
    f += [head, Spacer(1, 6), Paragraph(building, h1), Spacer(1, 4)]

    overall = next((c["text"] for c in commentary if c["title"] == "Overall"), "")
    if overall:
        f += [Paragraph(xesc(overall), read), Spacer(1, 6)]

    sec_cash = sum(a["end"] for a in s["cash"]["accounts"] if not is_security_account(a["name"])) if s["cash"]["accounts"] else None
    inc, exp, noi, net = s["income"], s["expenses"], s["noi"], s["net_income"]

    def kpi(label, value, sub):
        return [Paragraph(label.upper(), st("kl", fontSize=6.5, leading=8, textColor=MUTE)),
                Paragraph("<b>%s</b>" % value, st("kv", fontSize=15, leading=18, textColor=INK)),
                Paragraph(sub, st("kd", fontSize=7, leading=9, textColor=MUTE))]
    k = [kpi("Net operating income, YTD", money(noi["ytd_actual"]), "Budget %s (%s)" % (money(noi["ytd_budget"]), vtxt(noi["ytd_var"]))),
         kpi("Net income, YTD", money(net["ytd_actual"]), "Budget %s (%s)" % (money(net["ytd_budget"]), vtxt(net["ytd_var"]))),
         kpi("%s net operating income" % month_name, money(noi["month_actual"]), "Budget %s (%s)" % (money(noi["month_budget"]), vtxt(noi["month_var"]))),
         kpi("Cash, excluding security deposits", money(sec_cash) if sec_cash is not None else "n/a", "At month end")]
    kt = Table([k], colWidths=[cw / 4.0] * 4)
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
        if c["ytd_var"] < 0 and c["ytd_budget"] and -c["ytd_var"] / c["ytd_budget"] > 0.05:
            fl = "%.0f%% over" % (100.0 * -c["ytd_var"] / c["ytd_budget"])
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
          Paragraph("Green is favorable to budget and red is unfavorable. A line is flagged only when it is more than 5 percent over its year-to-date budget. Figures are whole dollars from the monthly financial statement.", small)]

    # ---- page 2
    f.append(Paragraph("What changed", h2))
    for c in commentary:
        if c["title"] == "Overall":
            continue
        f.append(KeepTogether([Paragraph("<b>%s</b>" % xesc(c["title"]), body), Paragraph(xesc(c["text"]), body), Spacer(1, 3)]))
    if reviewed:
        f.append(Paragraph("Commentary reviewed and confirmed by %s (Financial Analyst), %s." % (
            xesc(reviewed["by"]), xesc(reviewed["at"].split(" ")[0] + " " + " ".join(reviewed["at"].split(" ")[1:3]).rstrip(","))), small))
        f.append(Spacer(1, 6))
    if board_note:
        f.append(Table([[Paragraph("<b>Note to the board.</b> %s" % xesc(board_note), body)]], colWidths=[cw],
                       style=[("BACKGROUND", (0, 0), (-1, -1), SOFT), ("BOX", (0, 0), (-1, -1), 0.5, RED), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))

    f.append(PageBreak())
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
                                ("LEFTPADDING", (0, 0), (0, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        f.append(ct)
        ch = _capital_chart(cap, cw)
        if ch:
            f += [Spacer(1, 6), ch]
    else:
        f.append(Paragraph("No capital or other non-operating expenses are recorded this year to date.", body))
    ca = s["capital_assessment"]
    if ca and (ca["ytd_actual"] or ca["ytd_budget"]):
        f.append(Paragraph("Capital assessment collected year to date: %s (budget %s)." % (money(ca["ytd_actual"]), money(ca["ytd_budget"])), body))

    # cash + arrears
    cash = s["cash"]
    f.append(Paragraph("Cash, arrears and payables", h2))
    left = []
    if cash["accounts"]:
        arow = [["Account", "Start of month", "Month end"]]
        for a in cash["accounts"]:
            if a["end"] or a["begin"]:
                arow.append([Paragraph(a["name"], cell), num(a["begin"]), num(a["end"])])
        arow.append(["Total", num(cash["total"]["begin"]), num(cash["total"]["end"])])
        at = Table(arow, colWidths=[cw * 0.26, cw * 0.14, cw * 0.14])
        at.setStyle(TableStyle([("FONT", (0, 0), (-1, -1), "Helvetica", 8), ("FONT", (0, 0), (-1, 0), "Helvetica", 6.5),
                                ("TEXTCOLOR", (0, 0), (-1, 0), MUTE), ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                                ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, 1), (-1, -2), 0.3, LINE),
                                ("FONT", (0, -1), (-1, -1), "Helvetica-Bold", 8), ("LINEABOVE", (0, -1), (-1, -1), 0.8, INK),
                                ("LEFTPADDING", (0, 0), (0, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]))
        left.append(at)
        kinds = {}
        for a in cash["accounts"]:
            kinds[account_kind(a["name"])] = kinds.get(account_kind(a["name"]), 0) + a["end"]
        order = [("operating", "Operating"), ("reserve", "Reserves"), ("money market", "Money market"),
                 ("other", "Other"), ("security", "Security deposits")]
        parts = ["%s %s" % (lab, money(kinds[k])) for k, lab in order if kinds.get(k)]
        left.append(Spacer(1, 3))
        left.append(Paragraph(" &nbsp;|&nbsp; ".join(parts), small))
    right = []
    if len(cash["arrears"]) == 3:
        ar = cash["arrears"]
        mlab = lambda off: MONTH_NAMES[(m["month"] - 1 - off) % 12][:3]
        rrow = [["", mlab(2), mlab(1), mlab(0)],
                ["Receivable"] + [num(x) for x in ar["Accounts Receivable"]],
                ["Prepaid"] + [num(x) for x in ar["Prepaid"]],
                ["Net arrears"] + [num(x) for x in ar["Total Arrears"]]]
        if cash["ap"]:
            rrow.append(["Accounts payable"] + [num(x) for x in cash["ap"]])
        rt = Table(rrow, colWidths=[cw * 0.14, cw * 0.1, cw * 0.1, cw * 0.1])
        rt.setStyle(TableStyle([("FONT", (0, 0), (-1, -1), "Helvetica", 8), ("FONT", (0, 0), (-1, 0), "Helvetica", 6.5),
                                ("TEXTCOLOR", (0, 0), (-1, 0), MUTE), ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                                ("LINEBELOW", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, 1), (-1, -1), 0.3, LINE),
                                ("FONT", (0, 3), (-1, 3), "Helvetica-Bold", 8),
                                ("LEFTPADDING", (0, 0), (0, -1), 0), ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]))
        right.append(rt)
    ctab = Table([[left or "", right or ""]], colWidths=[cw * 0.56, cw * 0.44])
    ctab.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0)]))
    f.append(ctab)
    cc = _cash_chart(s, cw, 84)
    if cc:
        f += [Spacer(1, 6), Paragraph("Cash at month end, last twelve months (excluding security deposits)", small), cc]

    # tie-out checklist
    f.append(Paragraph("Ties to the statement", h2))
    labels = {"Month": "this month", "YTD": "year to date"}
    cells = []
    for c in s["checks"]:
        ok = c["status"] == "tied"
        tag = '<font color="%s"><b>%s</b></font>' % ("#2F6B4F" if ok else "#A4262C", "Tied" if ok else "Check")
        cells.append(Paragraph("%s &nbsp;%s" % (tag, c["label"]), st("ck", fontSize=6.8, leading=8.4, textColor=INK)))
    if len(cells) % 2:
        cells.append("")
    half = len(cells) // 2
    ck = Table([[cells[i], cells[i + half]] for i in range(half)], colWidths=[cw / 2.0, cw / 2.0])
    ck.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0),
                            ("TOPPADDING", (0, 0), (-1, -1), 1), ("BOTTOMPADDING", (0, 0), (-1, -1), 1)]))
    f.append(ck)

    # sign-off block
    f.append(Spacer(1, 6))
    so = signoff or {}
    def sline(role, who):
        who = who or {}
        if who.get("signed_at"):
            return "%s: %s, %s" % (role, who["name"], who["signed_at"])
        return "%s: %s, awaiting sign-off" % (role, who.get("name") or "not assigned")
    f.append(Table([[Paragraph("<b>Prepared and reviewed by Century Management</b><br/>%s<br/>%s" % (
        sline("Financial Analyst", so.get("fa")), sline("Property Manager", so.get("pm"))), body)]], colWidths=[cw],
        style=[("BOX", (0, 0), (-1, -1), 0.5, LINE), ("LEFTPADDING", (0, 0), (-1, -1), 8), ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))
    doc.build(f)
    return buf.getvalue()
