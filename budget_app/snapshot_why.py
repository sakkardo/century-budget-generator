"""Monthly Financial Snapshot: suggested reasons ("why") for over-budget notes, drafted from the GL.

Jennifer Murman (FA), 2026-10-05: a note should say why a line is off budget, not only that it is. The statement's
General Ledger and Budget Analysis Detail hold the evidence (snapshot_parser.gl_evidence); this module picks the
evidence for each note and asks Claude for a short suggested reason. The FA edits or confirms it; nothing here is
ever final on its own.

Only expense accounts reach this module (receipts with resident names are dropped by the parser), and only the
evidence for flagged notes is sent to Claude.
"""
import json
import os
import re

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December"]
MAX_ACCOUNTS = 4
MAX_LINES = 8

SYSTEM = """You write short explanations of budget variances for the board of a New York co-op or condominium.
You receive JSON: for each note, the over-budget line (title and facts) and evidence from the building's general
ledger: the accounts driving the variance, each account's actual spending by month this year, the budget for the
rest of the year, and this month's largest ledger entries (vendor, amount, remark).

For each note write a suggested reason in 1 or 2 plain sentences:
- Say WHY the line is over budget: which accounts, which months, which vendors or kinds of work.
- Use only facts in the evidence. Never invent causes, vendors, dates or amounts. Round to whole dollars.
- When you infer (for example a bill covering more than one month), say "appears to".
- Do not repeat the variance amount from the title; the reader already sees it.
- An account with "month_complete": false has only part of its month in the evidence; don't describe its entries as
  the whole month.
- No people's names other than vendors. No apartment owners or residents.
- If the evidence does not explain the variance, return an empty reason. Never use placeholders or brackets.

Answer with JSON only: {"notes": [{"key": "<key>", "reason": "<text>"}]}"""


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
            "month_actual": a["month_actual"], "month_budget": a["month_budget"], "month_var": a["month_var"],
            "annual_budget": a["annual_budget"],
            "actual_by_month": {MONTHS[i][:3]: v for i, v in enumerate(months)},
            "budget_rest_of_year": {MONTHS[month + i][:3]: v for i, v in enumerate(a.get("budget_ahead") or [])},
            "month_complete": a.get("month_complete"),
            "this_month_entries": [{"date": l["date"], "vendor": l["vendor"], "amount": l["amount"], "remark": l["note"]}
                                   for l in a.get("lines", [])[:MAX_LINES]],
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
    return items


_BAD = re.compile(r"\[|\]|\{|\}|TBD|to be confirmed", re.I)


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
            out[k] = r
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
