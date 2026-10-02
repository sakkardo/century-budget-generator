"""Release placement tests, built from real library layouts (2026-10-02 survey of 125 buildings).
Run: python budget_app/test_snapshot_release.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from snapshot_db import SharePointReleaser, month_folder_like, month_of_folder


class Tree:
    """In-memory library: a set of folder paths and a dict of file paths."""

    def __init__(self, folders, files=()):
        self.folders, self.files, self.writes = set(), {f: b"x" for f in files}, []
        for f in list(folders) + [os.path.dirname(x).replace(os.sep, "/") for x in files]:
            parts = f.split("/")
            for i in range(1, len(parts) + 1):
                self.folders.add("/".join(parts[:i]))

    def list_children(self, path):
        pre = path + "/" if path else ""
        if path and path not in self.folders:
            raise RuntimeError("Graph 404 Not Found on " + path)
        names = {}
        for f in self.folders:
            if f.startswith(pre) and f != path and "/" not in f[len(pre):]:
                names[f[len(pre):]] = True
        for f in self.files:
            if f.startswith(pre) and "/" not in f[len(pre):]:
                names[f[len(pre):]] = False
        return [{"name": n, "folder": d} for n, d in names.items()]

    def exists(self, path):
        return path in self.files

    def put_new(self, path, data):
        assert path not in self.files
        self.files[path] = data
        self.writes.append(path)


def target(tree, entity, month, year=2026, enabled=False):
    r = SharePointReleaser(tree, enabled=enabled)
    name = "%s - Client Monthly Financial Snapshot M %d.pdf" % (entity, year)
    return r, r.plan(name, entity, year, month), name


def raises(fn, text):
    try:
        fn()
    except ValueError as e:
        assert text in str(e), str(e)
        return
    raise AssertionError("expected ValueError containing %r" % text)


def run():
    # month spellings seen in the library
    for n, m in {"08 - August": 8, "08.2026": 8, "08-August 2026": 8, "08-2026": 8, "8-2026": 8, "May": 5,
                 "08 August 2026": 8, "04 - Apri": 4, "8 -2026": 8, "09- September 2026": 9,
                 "01-February 2026": 2, "Prior Management": None, "2026": None, "Mayor": None}.items():
        assert month_of_folder(n) == m, (n, month_of_folder(n), m)
    # new month folders copy the siblings' style, skipping a typo'd sibling
    assert month_folder_like(["07.2026"], 9, 2026) == "09.2026"
    assert month_folder_like(["July"], 9, 2026) == "September"
    assert month_folder_like(["7-2026"], 9, 2026) == "9-2026"
    assert month_folder_like(["01-February 2026", "03-March 2026"], 9, 2026) == "09-September 2026"
    assert month_folder_like(["01-February 2026"], 9, 2026) == "09-September 2026"
    assert month_folder_like([], 9, 2026) == "09 - September"

    C = "01 - Accounting General/Monthly Financial Snapshots"
    base = [C + "/2026/08-2026"]

    # the final copy goes into the building's month folder ONLY (Jacob 2026-10-02): one target, no central copy
    # 204: <bldg>/Monthly Financials/<yyyy>/<NN - Month>
    t = Tree(base + ["204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August"])
    _, p, name = target(t, "204", 8)
    assert p["targets"] == ["204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August/" + name], p
    # 206: <bldg>/<yyyy>/<Month>
    t = Tree(base + ["206 - 77 Bleecker Street Corp/2026/May", "206 - 77 Bleecker Street Corp/2025/May"])
    _, p, name = target(t, "206", 5)
    assert p["targets"] == ["206 - 77 Bleecker Street Corp/2026/May/" + name], p
    # 939: month folders straight under the building, each carrying its year
    t = Tree(base + ["939 - 305 Equities/07-2026", "939 - 305 Equities/08-2026"])
    _, p, name = target(t, "939", 8)
    assert p["targets"] == ["939 - 305 Equities/08-2026/" + name], p
    _, p, name = target(t, "939", 9)
    assert p["targets"] == ["939 - 305 Equities/09-2026/" + name] and p["new_month_folder"], p
    # statement folder spelled differently
    for spelled in ("Monthly financials", "Monthly FInancials", "Monthly Financial Reports", "Monthly Financial Statements"):
        t = Tree(base + ["724 - Cherokee Owners Corp/%s/2026/08-2026" % spelled, "724 - Cherokee Owners Corp/Audited Financials/2026"])
        _, p, name = target(t, "724", 8)
        assert p["targets"] == ["724 - Cherokee Owners Corp/%s/2026/08-2026/%s" % (spelled, name)], (spelled, p)
    # a new year: the statement folder exists, the year folder does not yet; style comes from last year
    t = Tree(base + ["148 - X/Monthly Financials/2025/12 - December"])
    _, p, name = target(t, "148", 1, year=2026)
    assert p["targets"] == ["148 - X/Monthly Financials/2026/01 - January/" + name], p
    # no place at all: refused with a clear message, nothing written
    t = Tree(base + ["850 - LC Lemle/Misc"])
    raises(lambda: target(t, "850", 8), "no place to put the snapshot")
    # the vendor's snapshot (and every other file) stays as it is; ours sits alongside under its own name
    vendor = "204 - 444 East 86th Street Owners Corp/Monthly Financials/2026/08 - August/444 East 86th Owners Corp Monthly FInancial Snapshot August 2026.pdf"
    t = Tree(base, files=[vendor])
    r, p, name = target(t, "204", 8, enabled=True)
    assert p["blockers"] == []
    assert r.release(b"%PDF", name, "204", "x", 2026, 8) == p["targets"] and t.writes == p["targets"]
    assert t.files[vendor] == b"x"  # untouched
    # an identical file name is never overwritten
    raises(lambda: r.release(b"%PDF", name, "204", "x", 2026, 8), "already exists")
    assert len(t.writes) == 1
    # duplicate month folders: the one already in use wins; two in use is refused
    t = Tree(base + ["204 - A/Monthly Financials/2026/03-March"], files=["204 - A/Monthly Financials/2026/03 - March/03-2026 Financials.pdf"])
    _, p, name = target(t, "204", 3)
    assert p["targets"] == ["204 - A/Monthly Financials/2026/03 - March/" + name], p
    t = Tree(base, files=["204 - A/Monthly Financials/2026/03 - March/a.pdf", "204 - A/Monthly Financials/2026/03-March/b.pdf"])
    raises(lambda: target(t, "204", 3), "More than one folder")
    # dry run writes nothing
    t = Tree(base + ["206 - B/2026/August"])
    r, p, name = target(t, "206", 8, enabled=False)
    assert r.release(b"%PDF", name, "206", "B", 2026, 8) == p["targets"] and t.writes == []
    # sandbox root for testing: same path under a test folder; the real building folder is not written
    os.environ["SNAPSHOT_RELEASE_ROOT"] = "01 - Accounting General/Snapshot Test"
    try:
        t = Tree(base + ["206 - B/2026/August"])
        r, p, name = target(t, "206", 8, enabled=True)
        assert p["targets"] == ["01 - Accounting General/Snapshot Test/206 - B/2026/August/" + name], p
        r.release(b"%PDF", name, "206", "B", 2026, 8)
        assert t.writes == p["targets"] and not any(w.startswith("206 - B/") for w in t.writes)
    finally:
        del os.environ["SNAPSHOT_RELEASE_ROOT"]
    print("snapshot release: all tests passed")


if __name__ == "__main__":
    run()
