"""Monthly Financial Snapshot: suggested reasons ("why") for over-budget notes, drafted from the GL.

Jennifer Murman (FA), 2026-10-05: a note should say why a line is off budget, not only that it is. The statement's
General Ledger and Budget Analysis Detail hold the evidence (snapshot_parser.gl_evidence); this module picks the
evidence for each note and asks Claude for a short suggested reason. The FA edits or confirms it; nothing here is
ever final on its own.

Only expense accounts reach this module (receipts with resident names are dropped by the parser), and only the
evidence for flagged notes is sent to Claude.
"""
import copy
import json
import os
import re
import statistics

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December"]
MAX_ACCOUNTS = 4
MAX_LINES = 8

SYSTEM = """You write short explanations of budget variances for the board of a New York co-op or condominium.
You receive JSON: for each note, the over-budget line (title and facts) and evidence from the building's general
ledger: the accounts driving the variance, each account's actual spending by month this year, the budget for the
rest of the year, this month's largest ledger entries (vendor, amount, remark), and under "drivers" the largest
entries from the earlier months that drove the variance, read from those months' own statements.

For each note write a suggested reason in 1 or 2 plain sentences:
- Say WHY the line is over budget: which accounts, which months, which vendors or kinds of work.
- Use only facts in the evidence. Never invent causes, vendors, dates or amounts. Round to whole dollars.
- When you say "about", round to the nearest thousand ("about $85,000", never "about $85,001"); otherwise give the
  exact whole-dollar amount without "about".
- When you infer (for example a bill covering more than one month), say "appears to".
- Budgets exist only for this month ("this_month"), year to date, the full year, and the months still ahead
  ("budget_rest_of_year"). There is no budget for earlier individual months: never state or imply one, and never
  apply this month's budget to another month (not "a monthly budget of $X" for July when X is August's).
- So only the year to date and this month can be called "over budget" or "above budget". Describe earlier months by
  their actual spending compared with other months ("about $90,000 in July and August, well above the spring months",
  "a one-time $9,688 in March"), never as "over budget in July" or "over budget most months".
- Vendors and descriptions are known for this month's entries and for the months listed under "drivers". Name
  vendors and work only from those entries; describe any other month by account and amount only ("Steam was about
  $85,000 in March").
- A driver month with "complete": false shows only part of that month; don't describe it as the whole month.
- Never include people's names from remarks. "v. [party]" marks a legal case: call it a legal matter.
- Don't speculate about reversals, catch-up billing or errors unless an entry's remark says so.
- Do not repeat the variance amount from the title; the reader already sees it.
- An account with "month_complete": false has only part of its month in the evidence; don't describe its entries as
  the whole month.
- No people's names other than vendors. No apartment owners or residents.
- If the evidence does not explain the variance, return an empty reason. Never use placeholders or brackets.

Answer with JSON only: {"notes": [{"key": "<key>", "reason": "<text>"}]}"""


SPIKE_FACTOR = 1.5   # an earlier month "drove it" when it is 1.5x the account's typical month...
SPIKE_MIN = 1000     # ...and at least $1,000
MAX_DRIVERS = 3      # up to three such months per account
MAX_DRIVER_LINES = 5
_PARTY = re.compile(r"\bv\.\s+[A-Z][A-Za-z'\-]+(?:\s+[A-Z][A-Za-z'\-]+)*")  # "v. Wendy Patitucci"


def norm(label):
    return re.sub(r"[^a-z0-9]+", " ", (label or "").lower()).strip()


def spike_months(months, current):
    """Earlier months (1-based) that drove an account's year-to-date variance: well above its typical month."""
    prior = [(v, i + 1) for i, v in enumerate((months or [])[:current - 1])]
    pos = [v for v, _ in prior if v > 0]
    if not pos:
        return []
    typical = statistics.median(pos)
    return [m for v, m in sorted(prior, reverse=True) if v >= SPIKE_FACTOR * typical and v >= SPIKE_MIN][:MAX_DRIVERS]


def redact(items):
    """What goes to the AI: legal-case names in remarks become 'v. [party]'. The FA still sees the full remark."""
    items = copy.deepcopy(items)
    for it in items:
        for a in it["evidence"]["accounts"]:
            for l in a.get("this_month_entries", []) + [e for d in a.get("drivers", []) for e in d["entries"]]:
                l["remark"] = _PARTY.sub("v. [party]", l.get("remark") or "")
    return items


def _money(n):
    return ("-$" if n < 0 else "$") + "{:,.0f}".format(abs(n))


def note_evidence(s, note):
    """The GL evidence behind one note, or None. Expense category notes only."""
    gl = s.get("gl") or {}
    key = note.get("key") or ""
    if not key.startswith("cat:") or not gl.get("categories"):
        return None
    accts = gl["categories"].get(key[4:]) or []
    month = gl.get("month") or s["meta"]["month"]
    by = "month_var" if note.get("scope") == "month" else "ytd_var"
    driving = sorted([a for a in accts if a[by] < 0], key=lambda a: a[by])[:MAX_ACCOUNTS]
    if not driving:
        return None
    out = []
    for a in driving:
        months = a.get("months") or []
        out.append({
            "account": a["name"], "acct": a.get("acct"),
            "ytd_actual": a["ytd_actual"], "ytd_budget": a["ytd_budget"], "ytd_var": a["ytd_var"],
            # the statement gives a budget for THIS month only (plus year to date, full year and months ahead);
            # label it with the month so it is never read as every month's budget
            "this_month": {"month": MONTHS[month - 1], "actual": a["month_actual"], "budget": a["month_budget"],
                           "variance": a["month_var"]},
            "annual_budget": a["annual_budget"],
            "actual_by_month": {MONTHS[i][:3]: v for i, v in enumerate(months)},
            "budget_rest_of_year": {MONTHS[month + i][:3]: v for i, v in enumerate(a.get("budget_ahead") or [])},
            "month_complete": a.get("month_complete"),
            "this_month_entries": [{"date": l["date"], "vendor": l["vendor"], "amount": l["amount"], "remark": l["note"]}
                                   for l in a.get("lines", [])[:MAX_LINES]],
            "drivers": (gl.get("drivers") or {}).get(norm(a["name"])) or [],
        })
    return {"month": MONTHS[month - 1], "accounts": out}


def eligible(note):
    """New or moved notes the FA hasn't written, confirmed or removed. Carried-forward notes keep their reason."""
    return (note.get("key", "").startswith("cat:") and note.get("status") in ("new", "worse") and not (note.get("text") or "").strip()
            and not note.get("confirmed") and not note.get("removed"))


def build_request(s, notes):
    items = []
    for n in notes:
        if not eligible(n):
            continue
        ev = note_evidence(s, n)
        if ev:
            items.append({"key": n["key"], "title": n["title"], "facts": n.get("facts", ""), "evidence": ev})
    return redact(items)


def driver_requests(s, notes):
    """{month: [account names]}: the earlier months whose own statements explain each year-to-date note."""
    gl = s.get("gl") or {}
    month = gl.get("month") or s["meta"]["month"]
    need = {}
    for n in notes:
        if not eligible(n) or n.get("scope") == "month":
            continue
        ev = note_evidence(s, n)
        for a in (ev or {}).get("accounts", []):
            months = [a["actual_by_month"].get(MONTHS[i][:3], 0) for i in range(12)]
            for m in spike_months(months, month):
                need.setdefault(m, set()).add(a["account"])
    return need


def build_drivers(s, need, ledgers):
    """{account: [{month, total, complete, entries}]} from earlier months' ledgers ({month: {norm name: {net, lines}}})."""
    gl = s.get("gl") or {}
    month_vals = {norm(a["name"]): a.get("months") or [] for accts in (gl.get("categories") or {}).values() for a in accts}
    out = {}
    for m, accounts in sorted(need.items()):
        led = ledgers.get(m)
        if not led:
            continue
        for name in accounts:
            g = led.get(norm(name))
            if not g:
                continue
            total = (month_vals.get(norm(name)) or [0] * 12)[m - 1]
            out.setdefault(norm(name), []).append({
                "month": MONTHS[m - 1][:3], "total": total, "complete": abs(round(g["net"]) - total) <= 1,
                "entries": [{"date": l["date"], "vendor": l["vendor"], "amount": l["amount"], "remark": l["note"]}
                            for l in g["lines"][:MAX_DRIVER_LINES]]})
    for v in out.values():
        v.sort(key=lambda d: -d["total"])
    return out


_BAD = re.compile(r"\[|\]|\{|\}|TBD|to be confirmed", re.I)
_ABOUT = re.compile(r"\b(about|roughly|around|approximately) \$(\d{1,3}(?:,\d{3})+|\d{4,})(?:\.\d+)?", re.I)


def round_abouts(text):
    """'about $85,001' -> 'about $85,000': an approximate figure is rounded to the nearest thousand (Jacob 2026-10-06)."""
    def fix(m):
        n = int(m.group(2).replace(",", ""))
        return "%s $%s" % (m.group(1), "{:,}".format(int(round(n / 1000.0)) * 1000))
    return _ABOUT.sub(fix, text)


def parse_reply(text, keys):
    """{key: reason} from Claude's JSON. Anything malformed is dropped, never guessed."""
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        return {}
    try:
        data = json.loads(m.group(0))
    except ValueError:
        return {}
    out = {}
    for n in data.get("notes", []) if isinstance(data, dict) else []:
        if not isinstance(n, dict):
            continue
        k, r = n.get("key"), (n.get("reason") or "").strip()
        if k in keys and r and len(r) <= 700 and not _BAD.search(r):
            out[k] = round_abouts(r)
    return out


class ClaudeDrafter:
    """Calls the Anthropic API. SNAPSHOT_WHY_MODEL picks the model (default: current Sonnet)."""

    def __init__(self, api_key=None, model=None, client=None):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.model = model or os.environ.get("SNAPSHOT_WHY_MODEL", "claude-sonnet-5-5")
        self._client = client

    @property
    def available(self):
        return bool(self._client or self.api_key)

    def __call__(self, items):
        if not items:
            return {}
        client = self._client
        if client is None:
            import anthropic
            client = anthropic.Anthropic(api_key=self.api_key, timeout=90)
        msg = client.messages.create(model=self.model, max_tokens=2000, system=SYSTEM,
                                     messages=[{"role": "user", "content": json.dumps({"notes": items})}])
        text = "".join(getattr(b, "text", "") for b in msg.content)
        return parse_reply(text, {i["key"] for i in items})
