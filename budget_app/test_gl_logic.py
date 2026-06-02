"""
Regression net for the canonical GL-prefix logic.

Runs the vectors in gl_test_vectors.json against budget_app/gl_logic.py and
fails loudly if any answer drifts. Pure-Python, no Flask, no DB — runs in
under a second:

    python budget_app/test_gl_logic.py

The same "overlap" vectors are the contract the JavaScript mirror
(_sumOrphanOverlap in the building-detail template) must also satisfy. If you
change the rules, update gl_test_vectors.json and BOTH implementations.

Exit code 0 = all pass; 1 = at least one drift (and the offending case prints).
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gl_logic import (
    gl_matches_prefixes,
    gl_prefixes_overlap,
    gl_token_covered_by,
)


def _load_vectors():
    with open(os.path.join(HERE, "gl_test_vectors.json"), encoding="utf-8") as fh:
        return json.load(fh)


def run():
    vectors = _load_vectors()
    failures = []

    for case in vectors.get("matches", []):
        got = gl_matches_prefixes(case["gl"], case["prefixes"])
        if bool(got) != bool(case["expected"]):
            failures.append(
                f"matches: gl={case['gl']!r} prefixes={case['prefixes']!r} "
                f"expected={case['expected']} got={got}  ({case.get('note','')})"
            )

    for case in vectors.get("overlap", []):
        got = gl_prefixes_overlap(case["a"], case["b"])
        if bool(got) != bool(case["expected"]):
            failures.append(
                f"overlap: a={case['a']!r} b={case['b']!r} "
                f"expected={case['expected']} got={got}  ({case.get('note','')})"
            )

    for case in vectors.get("covered", []):
        got = gl_token_covered_by(case["tok"], case["row_prefixes"])
        if bool(got) != bool(case["expected"]):
            failures.append(
                f"covered: tok={case['tok']!r} row_prefixes={case['row_prefixes']!r} "
                f"expected={case['expected']} got={got}  ({case.get('note','')})"
            )

    total = (
        len(vectors.get("matches", []))
        + len(vectors.get("overlap", []))
        + len(vectors.get("covered", []))
    )

    if failures:
        print(f"FAIL — {len(failures)}/{total} GL-logic vectors drifted:")
        for f in failures:
            print("  - " + f)
        return 1

    print(f"PASS — all {total} GL-logic vectors match gl_logic.py")
    return 0


if __name__ == "__main__":
    sys.exit(run())
