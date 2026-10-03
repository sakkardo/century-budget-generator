"""Monthly Financial Snapshot: email.

Mailer is the only way snapshot code sends email, and it has a safety switch:
    SNAPSHOT_EMAIL_MODE = off (default) | test | live
    off  : nothing is sent; the message is only recorded (outbox + snapshot history)
    test : everything goes to SNAPSHOT_EMAIL_TEST_TO instead, with a banner naming who it was for
    live : sent to the real recipients
Transport is Microsoft Graph users/{sender}/sendMail with the app's own token (Mail.Send, application).
Graph answers 202 with an empty body, which the app's generic _graph_post would treat as an error,
so this module has its own small sender.
"""
import base64
import html
import json
import os
import urllib.error
import urllib.parse
import urllib.request

MODES = ("off", "test", "live")


class GraphTransport:
    def __init__(self, token_fn, timeout=30):
        self.token_fn, self.timeout = token_fn, timeout

    def send(self, sender, to, cc, subject, body_html, attachments, save_sent=True):
        msg = {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body_html},
            "toRecipients": [{"emailAddress": {"address": a}} for a in to],
            "ccRecipients": [{"emailAddress": {"address": a}} for a in (cc or [])],
            "attachments": [{"@odata.type": "#microsoft.graph.fileAttachment", "name": a["name"],
                             "contentType": a.get("type", "application/pdf"),
                             "contentBytes": base64.b64encode(a["bytes"]).decode("ascii")} for a in (attachments or [])],
        }
        url = "https://graph.microsoft.com/v1.0/users/%s/sendMail" % urllib.parse.quote(sender)
        req = urllib.request.Request(url, data=json.dumps({"message": msg, "saveToSentItems": bool(save_sent)}).encode("utf-8"),
                                     method="POST", headers={"Authorization": "Bearer " + self.token_fn(),
                                                             "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                if r.status not in (200, 202):
                    raise RuntimeError("Graph sendMail returned %s" % r.status)
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8")[:300]
            except Exception:
                pass
            raise RuntimeError("Graph sendMail %s %s: %s" % (e.code, e.reason, detail))


class Mailer:
    def __init__(self, transport=None, mode=None, test_to=None, keep=200):
        self.transport = transport
        self._mode, self._test_to = mode, test_to
        self.outbox, self.keep = [], keep

    @property
    def mode(self):
        m = (self._mode or os.environ.get("SNAPSHOT_EMAIL_MODE") or "off").strip().lower()
        return m if m in MODES else "off"

    @property
    def test_to(self):
        return (self._test_to or os.environ.get("SNAPSHOT_EMAIL_TEST_TO") or "").strip()

    def send(self, sender, to, subject, body_html, attachments=None, cc=None, kind=""):
        """Returns {"status": "sent"|"test"|"off"|"failed", "to": [...actually addressed], "intended": [...]}."""
        to, cc = [a for a in to if a], [a for a in (cc or []) if a]
        out = {"status": None, "to": [], "intended": list(to) + list(cc), "kind": kind, "subject": subject,
               "sender": sender, "html": body_html, "attachments": [a["name"] for a in (attachments or [])]}
        mode = self.mode
        if mode == "off" or not self.transport:
            out["status"] = "off"
        else:
            real_to, real_cc, subj, body = to, cc, subject, body_html
            if mode == "test":
                if not self.test_to:
                    out["status"], out["error"] = "failed", "Test mode is on but SNAPSHOT_EMAIL_TEST_TO is not set."
                    self._keep(out)
                    return out
                banner = ('<div style="background:#fef3c7;border:1px solid #fde68a;color:#713f12;padding:10px 14px;'
                          'border-radius:6px;margin:0 0 16px;font:13px Arial,sans-serif"><b>TEST EMAIL.</b> In live mode this '
                          'would go to: %s</div>' % html.escape(", ".join(out["intended"])))
                real_to, real_cc, subj, body = [self.test_to], [], "[TEST] " + subject, banner + body_html
            try:
                # test emails never leave a copy in the FA's Sent Items; live ones do
                self.transport.send(sender, real_to, real_cc, subj, body, attachments, save_sent=(mode == "live"))
                out["status"], out["to"] = ("test" if mode == "test" else "sent"), list(real_to) + list(real_cc)
            except Exception as e:
                out["status"], out["error"] = "failed", str(e)[:300]
        self._keep(out)
        return out

    def _keep(self, msg):
        self.outbox.append(msg)
        del self.outbox[:-self.keep]


class LocalTransport:
    """Click-through: 'sends' into the mailer's outbox only (viewable at /snapshots/dev/outbox)."""

    def send(self, sender, to, cc, subject, body_html, attachments, save_sent=True):
        return None


# ------------------------------------------------------------------ email bodies
INK, LABEL, BORDER, OK, BRAND = "#2d2520", "#8b7b6b", "#e8e0d8", "#065f46", "#a4262c"


def _shell(inner):
    return ('<div style="background:#f5f3f1;padding:24px 0;font-family:Arial,Helvetica,sans-serif;color:%s">'
            '<table role="presentation" width="100%%" cellpadding="0" cellspacing="0"><tr><td align="center">'
            '<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;background:#fff;'
            'border:1px solid %s;border-radius:10px;border-top:4px solid %s">'
            '<tr><td style="padding:24px 28px">%s</td></tr></table>'
            '<p style="font-size:11px;color:%s;margin:12px 0 0">Century Management &middot; Monthly Financial Snapshot</p>'
            '</td></tr></table></div>') % (INK, BORDER, BRAND, inner, LABEL)


def _button(link, label, color=OK):
    return ('<table role="presentation" cellpadding="0" cellspacing="0" style="margin:18px 0"><tr><td style="background:%s;'
            'border-radius:6px"><a href="%s" style="display:inline-block;padding:13px 26px;color:#fff;font-weight:bold;'
            'font-size:15px;text-decoration:none">%s</a></td></tr></table>') % (color, html.escape(link, quote=True), label)


def _kpis(kpis):
    cells = "".join('<td style="padding:10px 12px;border:1px solid %s;vertical-align:top;width:25%%">'
                    '<div style="font-size:10px;letter-spacing:1px;text-transform:uppercase;color:%s">%s</div>'
                    '<div style="font-size:18px;font-weight:bold;color:%s;margin-top:2px">%s</div>'
                    '<div style="font-size:11px;color:%s">%s</div></td>' % (BORDER, LABEL, html.escape(k[0]), INK, html.escape(k[1]),
                                                                            LABEL, html.escape(k[2])) for k in kpis)
    return '<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">' \
           '<tr>%s</tr></table>' % cells


def _notes(notes):
    return "".join('<p style="margin:0 0 12px;font-size:14px;line-height:1.5"><b>%s</b><br>%s</p>' % (
        html.escape(t), html.escape(x)) for t, x in notes)


def _ongoing(items, resolved):
    out = ""
    if items:
        out += ('<p style="margin:14px 0 6px;font-size:13px;color:%s"><b>Ongoing items, explained previously</b></p>'
                '<ul style="margin:0 0 10px;padding-left:18px;font-size:13px;line-height:1.5">' % INK)
        out += "".join("<li><b>%s</b> (since %s): %s</li>" % (html.escape(n), html.escape(s or "earlier"), html.escape(t))
                       for n, t, s in items)
        out += "</ul>"
    if resolved:
        out += '<p style="font-size:13px;color:%s;margin:0 0 10px">No longer flagged since last month: %s.</p>' % (
            LABEL, html.escape(", ".join(resolved)))
    return out


def pm_request(info, reminder=False):
    """info: building, entity, month_label, fa_name, pm_name, kpis[(label,value,sub)], notes[(title,text)],
    board_note, link, due_label."""
    lead = ("Reminder: this snapshot is still waiting for your confirmation." if reminder else
            "%s has reviewed this month's financial snapshot. Please read it and confirm it before it goes to the board." % info["fa_name"])
    inner = (
        '<p style="font-size:11px;letter-spacing:1.5px;text-transform:uppercase;color:%s;margin:0">%s &middot; %s</p>'
        '<h1 style="font-size:22px;margin:4px 0 12px;color:%s">%s</h1>'
        '<p style="font-size:14px;line-height:1.5;margin:0 0 16px">Hi %s,<br>%s</p>' % (
            LABEL, html.escape(info["entity"]), html.escape(info["month_label"]), INK, html.escape(info["building"]),
            html.escape(info["pm_name"].split(" ")[0]), html.escape(lead))
        + _kpis(info["kpis"])
        + '<h2 style="font-size:15px;margin:20px 0 8px;color:%s">What changed</h2>' % INK + _notes(info["notes"])
        + _ongoing(info.get("ongoing") or [], info.get("resolved") or [])
        + ('<p style="font-size:13px;background:#f7e3e2;border:1px solid %s;padding:10px 12px;border-radius:6px">'
           '<b>Note to the board.</b> %s</p>' % (BRAND, html.escape(info["board_note"])) if info.get("board_note") else "")
        + '<p style="font-size:13px;color:%s;margin:16px 0 0">The full two-page snapshot is attached.</p>' % LABEL
        + _button(info["link"], "Review and confirm")
        + '<p style="font-size:13px;color:%s;margin:0">Please confirm by <b>%s</b>. On that page you can also ask %s for changes. '
          'The link is for you only and works once.</p>' % (LABEL, html.escape(info["due_label"]), html.escape(info["fa_name"]))
    )
    subj = "%s%s %s: monthly financial snapshot to confirm" % ("Reminder: " if reminder else "", info["entity"], info["month_label"])
    return subj, _shell(inner)


def fa_changes_requested(info):
    inner = ('<h1 style="font-size:20px;margin:0 0 10px">%s asked for changes</h1>'
             '<p style="font-size:14px;line-height:1.5">%s &middot; %s &middot; %s</p>'
             '<p style="font-size:14px;line-height:1.5;padding:10px 12px;border-left:3px solid %s;background:#f5f0eb">%s</p>'
             '<p style="font-size:14px">Edit the notes in the snapshot portal and send it again. The PM gets a new link.</p>' % (
                 html.escape(info["pm_name"]), html.escape(info["entity"]), html.escape(info["building"]),
                 html.escape(info["month_label"]), BRAND, html.escape(info["note"])))
    inner += _button(info["portal_link"], "Open the snapshot portal", INK)
    return "%s %s: PM asked for changes" % (info["entity"], info["month_label"]), _shell(inner)


def overdue(info):
    inner = ('<h1 style="font-size:20px;margin:0 0 10px">Snapshot overdue for PM confirmation</h1>'
             '<p style="font-size:14px;line-height:1.5">%s &middot; %s &middot; %s was sent to %s on %s and has not been '
             'confirmed in 48 hours. It stays waiting; nothing is finalized without the PM.</p>' % (
                 html.escape(info["entity"]), html.escape(info["building"]), html.escape(info["month_label"]),
                 html.escape(info["pm_name"]), html.escape(info["sent_label"])))
    inner += _button(info["portal_link"], "Open the snapshot portal", INK)
    return "Overdue: %s %s snapshot not confirmed by the PM" % (info["entity"], info["month_label"]), _shell(inner)


def final_saved(info):
    inner = ('<h1 style="font-size:20px;margin:0 0 10px">Snapshot confirmed and final</h1>'
             '<p style="font-size:14px;line-height:1.5">%s confirmed %s &middot; %s &middot; %s. %s</p>' % (
                 html.escape(info["pm_name"]), html.escape(info["entity"]), html.escape(info["building"]),
                 html.escape(info["month_label"]), html.escape(info["save_line"])))
    return "%s %s: snapshot confirmed by the PM" % (info["entity"], info["month_label"]), _shell(inner)
