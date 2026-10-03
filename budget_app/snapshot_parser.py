"""Monthly Financial Snapshot: read a Yardi monthly financial statement PDF.

Pure functions, no Flask or DB. Input is the statement PDF bytes; output is a dict of
figures plus a list of tie-out checks. Every figure comes straight from the statement's
own text, and every check compares the statement against itself, so a mismatch points at
a specific line the FA can look at.
"""
import re

import fitz  # PyMuPDF

NUM = r"-?\d[\d,]*"
_NUM_RE = re.compile(NUM)
# label, then 8 or more numbers separated by whitespace
# exactly 8 figures per row; the label is everything before them, so a name that ends in a
# number ("Owners Reserve 2") keeps its number instead of shifting every column by one
_ROW_RE = re.compile(r"^(?P<label>.*?[A-Za-z\)].*?)\s+(?P<nums>(?:%s\s+){7}%s)\s*$" % (NUM, NUM))
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


_TOP_SECTIONS = ("INCOME", "EXPENSES", "NON-OPERATING INCOME & EXPENSES", "ADJUSTMENTS TO CASH", "CAPITAL")


def is_security_account(name):
    """Tenant security-deposit cash ('Master Security Account', 'Bank United - Security Acct').
    Loan collateral ('NCB - Collateral Security', 148) is the building's money, not deposits."""
    n = name.lower()
    return "security" in n and "collateral" not in n


def account_kind(name):
    n = name.lower()
    if is_security_account(name):
        return "security"
    if "money market" in n:
        return "money market"
    if "res" in n.replace("restricted", "") and ("(res" in n or "reserve" in n):
        return "reserve"
    if "(op" in n or "operating" in n:
        return "operating"
    return "other"


def _n(s):
    return int(s.replace(",", ""))


def _lines(page):
    out = []
    for raw in page.get_text("text", sort=True).splitlines():
        s = re.sub(r"[ \t]+", " ", raw.strip())
        if s:
            out.append(s)
    return out


def _title(page):
    ls = _lines(page)
    return ls[0] if ls else ""


def _split_row(line):
    """Return (label, [numbers]) or None."""
    m = _ROW_RE.match(line)
    if not m:
        return None
    return m.group("label").strip(), [_n(x) for x in _NUM_RE.findall(m.group("nums"))]


def _statement_meta(doc):
    meta = {"building": None, "address": None, "month_end": None, "year": None, "month": None}
    for i in range(1, min(4, len(doc))):
        t = doc[i].get_text("text", sort=True)
        m = re.search(r"Month Ending\s+(\d\d)/(\d\d)/(\d{4})", t)
        if m:
            meta["month"], meta["year"] = int(m.group(1)), int(m.group(3))
            meta["month_end"] = "%s/%s/%s" % m.groups()
            lines = [l.strip() for l in t.splitlines() if l.strip()]
            for j, l in enumerate(lines):
                if re.search(r"(Table of Contents|Executive Financial Summary)", l):
                    parts = re.split(r"\s{2,}", l)
                    if len(parts) > 1:
                        meta["building"] = parts[-1].strip()
                    break
            break
    return meta


def parse_income_statement(doc):
    """Walk the 'Income Statement Detail' pages into rows with section context."""
    rows, sections, pending = [], [], None
    for page in doc:
        if not _title(page).startswith("Income Statement Detail"):
            continue
        for line in _lines(page):
            if re.match(r"^(Income Statement Detail|Month and YTD|Month Ending|Account Description|Budget$|2026 Remaining|\d+ of \d+)", line):
                continue
            if re.match(r"^\d{4} Remaining", line):
                continue
            sr = _split_row(line)
            if sr is None:
                if line.isupper() or line.upper() == line:
                    if line.startswith("TOTAL") and line.endswith("&"):
                        pending = line  # label wrapped onto the next line
                        continue
                    if line in _TOP_SECTIONS:
                        sections = [line]
                    elif line in ("NON-OPERATING INCOME", "NON-OPERATING EXPENSES"):
                        sections = sections[:1] + [line]
                    else:
                        sections.append(line)
                elif pending is not None:
                    pass
                continue
            label, nums = sr
            if pending is not None:
                label = pending + " " + label
                pending = None
            if len(nums) < 8:
                continue
            rows.append({
                "label": label,
                "section": list(sections),
                "month_actual": nums[0], "month_budget": nums[1], "month_var": nums[2],
                "ytd_actual": nums[3], "ytd_budget": nums[4], "ytd_var": nums[5],
                "annual_budget": nums[6], "remaining": nums[7],
                "is_total": bool(re.match(r"^(TOTAL|NET)\b", label)),
            })
    return rows


def parse_budget_analysis(doc):
    """Monthly actuals Jan..current month from 'Budget Analysis Detail'."""
    out = {}
    pending = None
    for page in doc:
        if not _title(page).startswith("Budget Analysis Detail"):
            continue
        for line in _lines(page):
            if line.startswith("TOTAL") and line.endswith("&"):
                pending = line
                continue
            m = re.match(r"^(?P<label>.*?[A-Za-z\)])\s+(?P<nums>(?:%s\s+){12,}%s(?:\s+-)?)\s*$" % (NUM, NUM), line)
            if not m:
                continue
            label = m.group("label").strip()
            if pending:
                label, pending = pending + " " + label, None
            nums = [_n(x) for x in _NUM_RE.findall(m.group("nums"))]
            out.setdefault(label, nums[:8])
    return out


def parse_cash(doc):
    """Cash Journal (accounts), Summary Cash Balance (12 months), exec summary A/P and arrears."""
    cash = {"accounts": [], "total": None, "history": None, "history_months": None,
            "ap": None, "arrears": {}}
    for page in doc:
        t = _title(page)
        if t.startswith("Cash Journal"):
            for line in _lines(page):
                m = re.match(r"^(?P<a>.*?[A-Za-z\)].*?)\s+(?P<n>(?:%s\s+){3}%s)$" % (NUM, NUM), line)
                if m and not m.group("a").upper().startswith("CASH ACCOUNT TOTALS"):
                    b, d, c, e = [_n(x) for x in _NUM_RE.findall(m.group("n"))]
                    cash["accounts"].append({"name": m.group("a").strip(), "begin": b, "debit": d, "credit": c, "end": e})
                m = re.match(r"^CASH ACCOUNT TOTALS:?\s+(%s)\s+(%s)\s+(%s)\s+(%s)$" % ((NUM,) * 4), line)
                if m:
                    cash["total"] = {"begin": _n(m.group(1)), "debit": _n(m.group(2)),
                                     "credit": _n(m.group(3)), "end": _n(m.group(4))}
        elif t.startswith("Summary Cash Balance"):
            for line in _lines(page):
                mm = re.findall(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (\d{4})", line)
                if len(mm) >= 6:
                    cash["history_months"] = ["%s %s" % x for x in mm]
                m = re.match(r"^CASH ACCOUNT TOTALS:?\s+((?:%s\s*)+)$" % NUM, line)
                if m:
                    cash["history"] = [_n(x) for x in _NUM_RE.findall(m.group(1))]
        elif t.startswith("Executive Financial Summary"):
            for line in _lines(page):
                m = re.match(r"^(Accounts Receivable|Prepaid|Total Arrears)\s+(%s)\s+(%s)\s+(%s)$" % ((NUM,) * 3), line)
                if m:
                    cash["arrears"][m.group(1)] = [_n(m.group(2)), _n(m.group(3)), _n(m.group(4))]
                m = re.search(r"Balance\s+(%s)\s+(%s)\s+(%s)\s*$" % ((NUM,) * 3), line)
                if m and cash["ap"] is None:
                    cash["ap"] = [_n(m.group(1)), _n(m.group(2)), _n(m.group(3))]
            # A/P shares a visual row with cash accounts, so also search the raw text
            if cash["ap"] is None:
                m = re.search(r"Balance\s+(%s)\s+(%s)\s+(%s)" % ((NUM,) * 3), page.get_text("text", sort=True))
                if m:
                    cash["ap"] = [_n(m.group(i)) for i in (1, 2, 3)]
    return cash


def _is_parent(rows_totals, idx):
    """A total is a parent subtotal if consecutive earlier totals sum to it."""
    r = rows_totals[idx]
    keys = ("month_actual", "ytd_actual", "ytd_budget")
    acc = {k: 0 for k in keys}
    for j in range(idx - 1, -1, -1):
        for k in keys:
            acc[k] += rows_totals[j][k]
        if j < idx - 1 and all(abs(acc[k] - r[k]) <= 2 for k in keys) and any(r[k] for k in keys):
            return True
    return False


def build_snapshot(pdf_bytes):
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    meta = _statement_meta(doc)
    rows = parse_income_statement(doc)
    monthly = parse_budget_analysis(doc)
    cash = parse_cash(doc)

    def find(label, section=None):
        for r in rows:
            if r["label"].upper() == label and (section is None or section in r["section"]):
                return r
        return None

    inc = find("TOTAL INCOME")
    exp = find("TOTAL EXPENSES")
    noi = find("NET OPERATING INCOME (LOSS)")
    nonop_inc = find("TOTAL NON-OPERATING INCOME")
    nonop_exp = find("TOTAL NON-OPERATING EXPENSES")
    net = find("NET INCOME (LOSS) FOR THIS PERIOD")
    cap_assess = next((r for r in rows if r["label"].lower().startswith("capital assessment")
                       and "CAPITAL" in r["section"]), None)

    # expense categories = TOTAL rows between EXPENSES and TOTAL EXPENSES, minus parent subtotals
    exp_totals = []
    in_exp = False
    for r in rows:
        if r["label"].upper() == "TOTAL INCOME":
            in_exp = True
            continue
        if r["label"].upper() == "TOTAL EXPENSES":
            break
        if in_exp and r["is_total"] and r["label"].upper().startswith("TOTAL"):
            exp_totals.append(r)
    categories = [r for i, r in enumerate(exp_totals) if not _is_parent(exp_totals, i)]

    def line_items(cat_total):
        """Items directly under a category: rows sharing its last section header, in order."""
        sec = cat_total["section"][-1] if cat_total["section"] else None
        return [r for r in rows if not r["is_total"] and sec and r["section"] and r["section"][-1] == sec]

    cats = []
    for r in categories:
        name = re.sub(r"^TOTAL\s+", "", r["label"]).title().replace("&", "&")
        items = sorted(line_items(r), key=lambda x: x["ytd_var"])  # most unfavorable first
        cats.append({"name": name, **{k: r[k] for k in (
            "month_actual", "month_budget", "month_var", "ytd_actual", "ytd_budget", "ytd_var",
            "annual_budget", "remaining")},
            "worst_items": [{"name": i["label"], "ytd_var": i["ytd_var"], "month_var": i["month_var"],
                             "ytd_actual": i["ytd_actual"], "ytd_budget": i["ytd_budget"]}
                            for i in items[:3] if i["ytd_var"] < 0]})

    inc_items = [r for r in rows if not r["is_total"] and r["section"] and r["section"][0] == "INCOME"]
    income_drivers = sorted(inc_items, key=lambda r: -abs(r["ytd_var"]))[:3]

    nonop_items = [r for r in rows if not r["is_total"] and any("NON-OPERATING EXPENSES" == s for s in r["section"])]
    nonop_inc_items = [r for r in rows if not r["is_total"] and any("NON-OPERATING INCOME" == s for s in r["section"])]

    def clean_cap(label):
        return re.sub(r"^Cap\s*-\s*", "", label).strip()

    capital = [{"project": clean_cap(r["label"]), "is_capital": r["label"].lower().startswith("cap"),
                "month": r["month_actual"], "ytd": r["ytd_actual"], "budget_ytd": r["ytd_budget"],
                "annual_budget": r["annual_budget"]} for r in nonop_items]

    def pick(r, *ks):
        return {k: r[k] for k in ks} if r else None

    full = ("month_actual", "month_budget", "month_var", "ytd_actual", "ytd_budget", "ytd_var")
    snap = {
        "meta": meta,
        "pages": len(doc),
        "income": pick(inc, *full),
        "expenses": pick(exp, *full),
        "noi": pick(noi, *full),
        "nonop_income": pick(nonop_inc, *full),
        "nonop_expense": pick(nonop_exp, *full),
        "net_income": pick(net, *full),
        "capital_assessment": pick(cap_assess, *full),
        "categories": cats,
        "income_drivers": [{"name": r["label"], "ytd_var": r["ytd_var"], "ytd_actual": r["ytd_actual"],
                            "ytd_budget": r["ytd_budget"]} for r in income_drivers],
        "nonop_income_items": [{"name": r["label"], "ytd": r["ytd_actual"], "month": r["month_actual"]}
                               for r in nonop_inc_items],
        "capital": capital,
        "monthly": {k: monthly[k] for k in monthly
                    if k in ("TOTAL INCOME", "TOTAL EXPENSES", "NET OPERATING INCOME (LOSS)") or
                    any(k.upper() == "TOTAL " + c["name"].upper() for c in cats)},
        "cash": cash,
    }
    snap["checks"] = run_checks(snap, rows, monthly)
    return snap


def _chk(cid, label, severity, expected, actual, detail=""):
    ok = expected == actual
    return {"id": cid, "label": label, "severity": severity, "status": "tied" if ok else "mismatch",
            "expected": expected, "actual": actual, "detail": detail if not ok else ""}


def run_checks(s, rows, monthly):
    ch = []
    tol = 2  # statement rounds each line to whole dollars

    def near(cid, label, sev, a, b, detail=""):
        c = _chk(cid, label, sev, a, b, detail)
        if abs(a - b) <= tol:
            c["status"], c["detail"] = "tied", ""
        else:
            c["detail"] = detail + (" " if detail else "") + "Difference %s." % format(a - b, ",")
        ch.append(c)

    for needed in ("income", "expenses", "noi", "net_income"):
        if not s[needed]:
            ch.append({"id": "found_" + needed, "label": "Found %s on the statement" % needed,
                       "severity": "block", "status": "mismatch", "expected": "present", "actual": "missing",
                       "detail": "Could not find this total in the Income Statement Detail pages."})
    if any(c["status"] == "mismatch" for c in ch):
        return ch

    for per, pk in (("Month", "month_actual"), ("YTD", "ytd_actual")):
        near("cat_sum_" + per, "Expense categories add up to total expenses (%s)" % per, "block",
             sum(c[pk] for c in s["categories"]), s["expenses"][pk])
        near("noi_" + per, "Income minus expenses equals net operating income (%s)" % per, "block",
             s["income"][pk] - s["expenses"][pk], s["noi"][pk])
        ni = s["noi"][pk] + (s["nonop_income"][pk] if s["nonop_income"] else 0) - (s["nonop_expense"][pk] if s["nonop_expense"] else 0)
        near("net_" + per, "Net operating income plus non-operating items equals net income (%s)" % per, "block",
             ni, s["net_income"][pk])
    if s["nonop_expense"]:
        near("capital_sum", "Capital and non-operating projects add up to total non-operating expenses (YTD)", "block",
             sum(c["ytd"] for c in s["capital"]), s["nonop_expense"]["ytd_actual"])

    # Budget Analysis (monthly) must agree with the Income Statement
    m = s["meta"]["month"]
    mo = monthly.get("TOTAL INCOME")
    if mo and m:
        near("trend_income_ytd", "Monthly income detail adds up to YTD income", "block", sum(mo[:m]), s["income"]["ytd_actual"])
        near("trend_income_month", "Monthly income detail matches this month's income", "block", mo[m - 1], s["income"]["month_actual"])
    me = monthly.get("TOTAL EXPENSES")
    if me and m:
        near("trend_exp_ytd", "Monthly expense detail adds up to YTD expenses", "block", sum(me[:m]), s["expenses"]["ytd_actual"])

    # Cash
    cash = s["cash"]
    if cash["accounts"] and cash["total"]:
        near("cash_accounts", "Cash accounts add up to the Cash Journal total", "block",
             sum(a["end"] for a in cash["accounts"]), cash["total"]["end"])
        for a in cash["accounts"]:
            if abs(a["begin"] + a["debit"] + a["credit"] - a["end"]) > 2:  # whole-dollar rounding
                ch.append({"id": "cash_roll_" + a["name"], "label": "Cash roll-forward: %s" % a["name"],
                           "severity": "review", "status": "mismatch", "expected": a["end"],
                           "actual": a["begin"] + a["debit"] + a["credit"], "detail": ""})
    if cash["history"] and cash["total"]:
        # The 12-month history page leaves out security-deposit accounts in some buildings (204, 302, 206)
        # and includes them in others (148). It must match the Cash Journal one way or the other.
        excl = sum(a["end"] for a in cash["accounts"] if not is_security_account(a["name"]))
        hist = cash["history"][-1]
        target = cash["total"]["end"] if abs(hist - cash["total"]["end"]) <= 2 else excl
        near("cash_history", "Cash history page ends at the Cash Journal total", "review", target, hist,
             "The 'Summary Cash Balance' page disagrees with its own Cash Journal (with or without security deposits).")
    arr = cash["arrears"]
    if len(arr) == 3:
        near("arrears", "Receivable plus prepaid equals total arrears (current month)", "block",
             arr["Accounts Receivable"][2] + arr["Prepaid"][2], arr["Total Arrears"][2])
    return ch
