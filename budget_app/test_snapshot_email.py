"""Tests for the snapshot mailer's safety switch and the Graph sender (no network).
Run: python budget_app/test_snapshot_email.py
"""
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import snapshot_mail as sm


class Fake:
    def __init__(self, fail=False):
        self.sent, self.fail = [], fail

    def send(self, sender, to, cc, subject, body_html, attachments):
        if self.fail:
            raise RuntimeError("Graph sendMail 403 Forbidden")
        self.sent.append((sender, list(to), list(cc), subject))


def run():
    for k in ("SNAPSHOT_EMAIL_MODE", "SNAPSHOT_EMAIL_TEST_TO"):
        os.environ.pop(k, None)
    args = ("fa@centuryny.com", ["pm@centuryny.com"], "Subj", "<p>body</p>")

    # default is OFF: nothing goes out
    f = Fake()
    r = sm.Mailer(f).send(*args, cc=["boss@centuryny.com"])
    assert r["status"] == "off" and f.sent == [] and r["intended"] == ["pm@centuryny.com", "boss@centuryny.com"]
    # unknown mode is treated as off
    os.environ["SNAPSHOT_EMAIL_MODE"] = "LIVEX"
    assert sm.Mailer(Fake()).send(*args)["status"] == "off"
    # TEST: only the test inbox, never the real recipient; subject and banner say so
    os.environ["SNAPSHOT_EMAIL_MODE"] = "test"
    assert sm.Mailer(Fake()).send(*args)["status"] == "failed"  # no test inbox configured: refuse, do not guess
    os.environ["SNAPSHOT_EMAIL_TEST_TO"] = "jacob@centuryny.com"
    f = Fake()
    m = sm.Mailer(f)
    r = m.send(*args, cc=["boss@centuryny.com"])
    assert r["status"] == "test" and f.sent == [("fa@centuryny.com", ["jacob@centuryny.com"], [], "[TEST] Subj")]
    # LIVE: the real recipients
    os.environ["SNAPSHOT_EMAIL_MODE"] = "live"
    f = Fake()
    assert sm.Mailer(f).send(*args, cc=["boss@centuryny.com"])["status"] == "sent"
    assert f.sent == [("fa@centuryny.com", ["pm@centuryny.com"], ["boss@centuryny.com"], "Subj")]
    # a transport failure is reported, not raised
    r = sm.Mailer(Fake(fail=True)).send(*args)
    assert r["status"] == "failed" and "403" in r["error"]

    # Graph sender: 202 with an empty body is success; the request is shaped right; HTTP errors become RuntimeError
    seen = {}

    class Resp:
        status = 202

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b""

    def fake_urlopen(req, timeout=None):
        seen["url"], seen["body"], seen["auth"] = req.full_url, json.loads(req.data), req.headers.get("Authorization")
        return Resp()

    real = urllib.request.urlopen
    urllib.request.urlopen = fake_urlopen
    try:
        sm.GraphTransport(lambda: "tok").send("fa@centuryny.com", ["pm@centuryny.com"], [], "S", "<b>x</b>",
                                              [{"name": "a.pdf", "bytes": b"%PDF-1"}])
        assert seen["url"] == "https://graph.microsoft.com/v1.0/users/fa%40centuryny.com/sendMail", seen["url"]
        msg = seen["body"]["message"]
        assert seen["auth"] == "Bearer tok" and msg["toRecipients"][0]["emailAddress"]["address"] == "pm@centuryny.com"
        assert msg["attachments"][0]["name"] == "a.pdf" and msg["attachments"][0]["contentBytes"] == "JVBERi0x"
        assert msg["body"]["contentType"] == "HTML" and seen["body"]["saveToSentItems"] is True

        def boom(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {}, None)
        urllib.request.urlopen = boom
        try:
            sm.GraphTransport(lambda: "tok").send("fa@x", ["pm@x"], [], "S", "x", [])
            raise AssertionError("expected RuntimeError")
        except RuntimeError as e:
            assert "403" in str(e)
    finally:
        urllib.request.urlopen = real

    # the PM email carries the link, the due date, the figures and the notes; text is escaped
    subj, body = sm.pm_request({"building": "444 East 86th", "entity": "204", "month_label": "August 2026", "fa_name": "Kristy Paxinos",
                                "pm_name": "Jacob Sirotkin", "kpis": [("NOI", "$1", "Budget $2")], "notes": [("R&M", "<5% over")],
                                "board_note": "", "link": "https://x/snapshot/confirm/a/b", "due_label": "Oct 4, 2026 9:00 AM"})
    assert "204 August 2026" in subj and 'href="https://x/snapshot/confirm/a/b"' in body and "Oct 4, 2026" in body
    assert "R&amp;M" in body and "&lt;5% over" in body
    for k in ("SNAPSHOT_EMAIL_MODE", "SNAPSHOT_EMAIL_TEST_TO"):
        os.environ.pop(k, None)
    print("snapshot email: all tests passed")


if __name__ == "__main__":
    run()
