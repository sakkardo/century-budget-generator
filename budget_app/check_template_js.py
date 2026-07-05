# -*- coding: utf-8 -*-
"""Deploy gate: parse every inline <script> block in workflow.py's templates.

WHY THIS EXISTS
    The dashboard/board templates carry ~800KB of hand-written JS inside Python
    strings. A single stray backslash (e.g. ``\\'`` inside a JS string built in a
    Python string) terminates the JS string early and throws a SyntaxError that
    kills the ENTIRE inline <script> block. When that happens the page renders
    blank/dead while every backend API still returns 200 -- so the post-deploy
    health probe (which only hits endpoints) reports HEALTHY and the outage ships
    silently. This exact bug took production down twice (2026-05 wizard,
    2026-07-02 CAM). None of the other gates parse JS.

WHAT IT DOES
    Extracts each <script>...</script> block from the given workflow.py, strips
    Jinja ({{ }}, {% %}, {# #}) so the residue is plain JS, and runs `node
    --check` on it. Any SyntaxError fails the gate (exit 1), aborting the deploy
    before the dead page can reach a client. node absent -> exit 2 (can't verify,
    so don't pretend to).

USAGE
    python check_template_js.py [path/to/workflow.py]
    (defaults to the workflow.py sitting next to this script)
"""
import io
import os
import re
import subprocess
import sys
import tempfile



def _with_page_templates(wf_path, text):
    """Append page_templates/*.py so the corpus equals the pre-extraction
    workflow.py (templates moved out 2026-07-05, clean-architecture tranche 1)."""
    import glob as _glob
    _pkg = os.path.join(os.path.dirname(os.path.abspath(wf_path)), "page_templates")
    for _p in sorted(_glob.glob(os.path.join(_pkg, "*.py"))):
        try:
            with open(_p, encoding="utf-8") as _fh:
                text += "\n" + _fh.read()
        except OSError:
            pass
    return text

def main():
    here = os.path.dirname(os.path.abspath(__file__))
    wf = sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "workflow.py")
    if not os.path.exists(wf):
        sys.stderr.write("JS GATE: workflow.py not found at %s\n" % wf)
        return 2

    # node is required to parse-check; without it we cannot verify, so fail
    # loud rather than silently skip (silent skip = the outage ships).
    try:
        subprocess.run(["node", "--version"], capture_output=True, check=True)
    except Exception:
        sys.stderr.write(
            "JS GATE: `node` not found on PATH -- cannot parse-check template JS.\n"
            "Install Node.js so this gate can run (it is the only guard against the\n"
            "inline-JS-escape outage that has hit production twice).\n")
        return 2

    src = _with_page_templates(wf, io.open(wf, encoding="utf-8").read())
    blocks = re.findall(r"<script[^>]*>(.*?)</script>", src, re.S)
    blocks = [b for b in blocks if len(b.strip()) > 40]

    fails = []
    tmpdir = tempfile.mkdtemp(prefix="jsgate_")
    for i, b in enumerate(blocks):
        js = re.sub(r"\{\{.*?\}\}", "0", b, flags=re.S)   # Jinja expressions -> literal
        js = re.sub(r"\{%.*?%\}", "", js, flags=re.S)      # Jinja statements -> gone
        js = re.sub(r"\{#.*?#\}", "", js, flags=re.S)      # Jinja comments -> gone
        p = os.path.join(tmpdir, "block_%d.js" % i)
        io.open(p, "w", encoding="utf-8").write(js)
        r = subprocess.run(["node", "--check", p], capture_output=True, text=True)
        if r.returncode != 0:
            first = (r.stderr.strip().splitlines() or ["(no stderr)"])[0]
            fails.append((i, first))

    if fails:
        sys.stderr.write("JS GATE FAILED: %d inline <script> block(s) do not parse.\n" % len(fails))
        for i, err in fails:
            sys.stderr.write("  block #%d: %s\n" % (i, err))
        sys.stderr.write(
            "A JS SyntaxError kills the whole script block -> the page renders dead\n"
            "while APIs return 200. Fix the escape (usually a stray backslash) before\n"
            "deploying. Do NOT push this.\n")
        return 1

    print("JS GATE OK: %d inline <script> block(s) parse clean." % len(blocks))
    return 0


if __name__ == "__main__":
    sys.exit(main())
