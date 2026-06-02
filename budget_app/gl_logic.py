"""
Canonical GL-prefix logic — the ONE source of truth for the server.

Background
----------
The budget "Summary" tab groups GL accounts into rows. Each row carries a list
of prefix tokens; a GL account "belongs" to a row when it matches one of the
row's tokens. Two questions fall out of that:

  1. matching   — does GL code X match prefix token T?            gl_matches_prefixes
  2. overlap    — would putting token A where B already lives      gl_prefixes_overlap
                  double-count the same dollars?
  3. coverage   — does a token already on a row make a new token   gl_token_covered_by
                  redundant (idempotent no-op)?

Token grammar (unchanged from the original implementations):
  - bare token  "5260"      → whole-account-family. Matches when the GL's
                              family base (suffix stripped, 4130-0010 -> 4130)
                              startswith the token.
  - dashed token "4130-0010" → exact sub-account. Matches when the full GL
                              string startswith the token. Lets sub-accounts
                              be disambiguated (Storage 4130-0010 vs
                              Bicycle 4130-0015 vs Laundry 4130-0030).
  Detection: a token containing "-" is treated as an exact sub-account.

Why this module exists
----------------------
This logic used to live in three places that had to agree by hand: the matcher
inside workflow.create_workflow_blueprint, the overlap guard inside
app.admin_append_summary_prefix, and a JavaScript mirror in the building-detail
template. They drifted once already (an equality-vs-startswith bug let a
catch-all family like '7' silently double-count '7120'). This module is now the
single server-side definition; workflow.py and app.py both import from here.

The browser mirror (_sumOrphanOverlap in the building-detail template) cannot
import Python without a build step the app does not have, so it stays a hand
mirror — but budget_app/gl_test_vectors.json pins BOTH sides to the same
expected answers, and budget_app/test_gl_logic.py fails if the Python side ever
drifts from those vectors. Keep the JS mirror and the vector file in lockstep.

This module deliberately has NO heavy imports (no Flask, no SQLAlchemy) so it
is import-safe everywhere and unit-testable with plain `python`.
"""


def gl_family(token):
    """The whole-account-family base of a token: strip the sub-account suffix.

    "4130-0010" -> "4130";  "5260" -> "5260".
    """
    token = str(token)
    return token.split("-", 1)[0] if "-" in token else token


def gl_matches_prefixes(gl_code, prefixes):
    """True if ``gl_code`` is matched by ANY token in ``prefixes``.

    - bare token  -> match on family base (suffix stripped from the GL side)
    - dashed token -> match on the full, un-stripped GL string
    """
    if not gl_code or not prefixes:
        return False
    gl_str = str(gl_code).strip()
    gl_base = gl_str.split("-")[0].strip()
    for prefix in prefixes:
        p = str(prefix).strip()
        if "-" in p:
            # Sub-account exact-prefix mode: keep the suffix on both sides.
            if gl_str.startswith(p):
                return True
        else:
            # Whole-account-family mode: strip suffix on the GL side.
            if gl_base.startswith(p):
                return True
    return False


def gl_prefixes_overlap(a, b):
    """True iff some GL code would be matched by BOTH tokens — i.e. appending
    one where the other already lives would double-count the same dollars.

    Mirrors gl_matches_prefixes: a bare token matches gl_base.startswith(token);
    a dashed token matches gl_full.startswith(token). Overlap therefore reduces
    to a mutual-prefix test on the right strings. This correctly catches short
    catch-all families like '7' (Capital Expenses) covering '7120', which a
    family-base *equality* test would miss.
    """
    a, b = str(a).strip(), str(b).strip()
    ea, eb = ("-" in a), ("-" in b)
    if not ea and not eb:          # both bare: mutual base-prefix
        return a.startswith(b) or b.startswith(a)
    if ea and eb:                  # both dashed: mutual full-prefix
        return a.startswith(b) or b.startswith(a)
    if ea and not eb:              # a dashed, b bare: a's family under b
        return gl_family(a).startswith(b)
    return gl_family(b).startswith(a)   # b dashed, a bare


def gl_token_covered_by(tok, row_prefixes):
    """True iff some token already in ``row_prefixes`` matches every GL that
    ``tok`` would — so appending ``tok`` adds nothing (idempotent no-op).

    Same startswith semantics as the matcher, but asymmetric: an existing
    token must COVER ``tok``.
    """
    tok = str(tok).strip()
    te = ("-" in tok)
    tbase = gl_family(tok)
    for t in row_prefixes:
        t = str(t or "").strip()
        if not t:
            continue
        if "-" in t:
            if te and tok.startswith(t):
                return True
        else:
            if (not te) and tok.startswith(t):
                return True
            if te and tbase.startswith(t):
                return True
    return False
