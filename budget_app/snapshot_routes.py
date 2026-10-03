"""Monthly Financial Snapshot: Flask blueprint.

Portal (FAs): /snapshots and /api/snapshots/*. In production every portal call needs Microsoft sign-in.
PM email link: /snapshot/confirm/<rid>/<token>. No sign-in; the one-time link is the credential.
  GET only shows the page (mail scanners open links); the decision is a POST.
Timer: POST /api/snapshots/cron/tick with X-Admin-Key (hourly Railway cron).
"""
import html
import os
import string

from flask import Blueprint, Response, has_request_context, jsonify, request

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = os.path.join(HERE, "..", "tasks", "snapshot_samples")


def _public_base():
    dom = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if dom:
        return "https://" + dom
    return request.url_root.rstrip("/") if has_request_context() else ""


def create_blueprint(service, identity=None, dev=True, signing_allowed=None, identity_detail=None, signin_url=None,
                     admin_key=None):
    bp = Blueprint("snapshots", __name__)
    service.base_url = _public_base

    def err(e, code=400):
        return jsonify({"error": str(e)}), code

    def uid():
        if not dev:  # production: only the signed-in identity counts, never a URL parameter
            return (identity() if identity else None) or 0
        try:
            return int(request.args.get("as") or (request.get_json(silent=True) or {}).get("as") or request.form.get("as") or 0)
        except (TypeError, ValueError):
            return 0

    def need_signin():
        if not dev and not uid():
            return err("Sign in with Microsoft first.", 401)
        return None

    # ------------------------------------------------------------------ portal
    @bp.route("/snapshots")
    def page():
        with open(os.path.join(HERE, "snapshot_page.html"), encoding="utf-8") as f:
            return Response(f.read(), mimetype="text/html")

    @bp.route("/api/snapshots/meta")
    def meta():
        samples = sorted(f for f in os.listdir(SAMPLES) if f.endswith(".pdf")) if dev and os.path.isdir(SAMPLES) else []
        me = uid()
        mine = []
        if me:
            mine = sorted({b["entity"] for b in service.directory.buildings()
                           if any(a["user_id"] == me for a in service.directory.team(b["entity"]))}) if dev else \
                getattr(service.directory, "entities_for", lambda u: [])(me)
        return jsonify({"dev": dev, "me": {"id": me, "name": service.directory.name(me)} if me else None,
                        "signing_allowed": True if signing_allowed is None else bool(signing_allowed()),
                        "signin_url": signin_url, "signout_url": "/auth/snapshot/logout" if signin_url else None,
                        "users": service.directory.users(), "buildings": service.directory.buildings() if (dev or me) else [],
                        "my_entities": mine, "email_mode": service.mailer.mode, "samples": samples})

    @bp.route("/api/snapshots")
    def listing():
        return need_signin() or jsonify(service.list())

    @bp.route("/api/snapshots/<rid>")
    def one(rid):
        blocked = need_signin()
        if blocked:
            return blocked
        try:
            return jsonify(service.view(rid, uid()))
        except ValueError as e:
            return err(e, 404)

    @bp.route("/api/snapshots/<rid>/pdf")
    def pdf(rid):
        blocked = need_signin()
        if blocked:
            return blocked
        rec = service.store.get(rid)
        if rec is None:
            return err("Snapshot not found.", 404)
        return Response(service.render(rec), mimetype="application/pdf",
                        headers={"Content-Disposition": "inline; filename=%s.pdf" % rid})

    @bp.route("/api/snapshots/generate", methods=["POST"])
    def generate():
        try:
            entity = request.form.get("entity", "")
            sample = request.form.get("sample", "")
            f = request.files.get("file")
            if sample and not dev:
                return err("Sample statements are only available in the click-through.")
            if f:
                data, source = f.read(), "uploaded file " + f.filename
            elif sample:
                p = os.path.join(SAMPLES, os.path.basename(sample))
                data, source = open(p, "rb").read(), "sample statement " + os.path.basename(sample)
            else:
                return err("Choose a sample statement or upload the monthly financial statement PDF.")
            who = uid()
            if not dev and not who:
                return err("Sign in with Microsoft first.", 401)
            rid = service.generate(entity, data, who, source)
            return jsonify({"id": rid})
        except Exception as e:  # parse failures must reach the screen, not a 500 page
            return err(e)

    def action(fn):
        try:
            out = fn()
            return jsonify({"ok": True, "result": out if isinstance(out, (dict, list, str, type(None))) else None})
        except ValueError as e:
            return err(e, 400)

    def guarded(fn):
        blocked = need_signin()
        return blocked or action(fn)

    @bp.route("/api/snapshots/<rid>/edit", methods=["POST"])
    def edit(rid):
        b = request.get_json(force=True)
        return guarded(lambda: service.edit(rid, uid(), b["commentary"], b.get("board_note", "")))

    @bp.route("/api/snapshots/<rid>/ack", methods=["POST"])
    def ack(rid):
        b = request.get_json(force=True)
        return guarded(lambda: service.acknowledge(rid, uid(), b["check_id"], b.get("note", "")))

    @bp.route("/api/snapshots/<rid>/note", methods=["POST"])
    def note(rid):
        b = request.get_json(force=True)
        return guarded(lambda: service.confirm_note(rid, uid(), int(b["index"]), b.get("text")))

    @bp.route("/api/snapshots/<rid>/notes/confirm-continuing", methods=["POST"])
    def confirm_continuing(rid):
        return guarded(lambda: {"confirmed": service.confirm_continuing(rid, uid())})

    @bp.route("/api/snapshots/<rid>/send", methods=["POST"])
    def send(rid):
        if signing_allowed is not None and not signing_allowed():
            return err("Sending is switched off until a secure sign-in is in place.", 403)
        via = identity_detail() if identity_detail else None

        def go():
            out = service.send_to_pm(rid, uid(), via=via)
            return {"pms": [p["name"] for p in out["pms"]]}
        return guarded(go)

    @bp.route("/api/snapshots/<rid>/release", methods=["POST"])
    def release(rid):
        return guarded(lambda: service.release_now(rid, uid()) and None)

    @bp.route("/api/snapshots/reset", methods=["POST"])
    def reset():
        if not dev:
            return err("Not available.", 404)
        service.store.reset()
        service.mailer.outbox.clear()
        return jsonify({"ok": True})

    # ------------------------------------------------------------------ the hourly timer
    @bp.route("/api/snapshots/cron/tick", methods=["POST"])
    def tick():
        key = (admin_key() if callable(admin_key) else admin_key) or ""
        if not dev and (not key or request.headers.get("X-Admin-Key", "") != key):
            return err("Not allowed.", 403)
        return jsonify(service.tick())

    # ------------------------------------------------------------------ the PM's email link
    with open(os.path.join(HERE, "snapshot_confirm_page.html"), encoding="utf-8") as f:
        tpl = string.Template(f.read())

    def page_out(title, body, code=200):
        return Response(tpl.safe_substitute(title=html.escape(title), body=body), status=code, mimetype="text/html",
                        headers={"Cache-Control": "no-store", "X-Robots-Tag": "noindex", "Referrer-Policy": "no-referrer"})

    def msg(kind, title, text):
        return page_out(title, '<div class="msg %s"><h1>%s</h1><p style="margin:0">%s</p></div>' % (kind, html.escape(title), text))

    def summary(st, rid, raw):
        info = st["info"]
        kp = "".join('<div class="kpi"><div class="l">%s</div><div class="v">%s</div><div class="s">%s</div></div>' % tuple(
            html.escape(x) for x in k) for k in info["kpis"])
        notes = "".join('<p class="note"><b>%s</b><br>%s</p>' % (html.escape(t), html.escape(x)) for t, x in info["notes"])
        if info.get("ongoing"):
            notes += '<h2 style="margin-top:18px">Previous notes</h2><ul style="margin:0 0 10px;padding-left:18px">' + \
                "".join("<li><b>%s</b> (since %s): %s</li>" % (html.escape(n), html.escape(s or "earlier"), html.escape(t)) for n, t, s in info["ongoing"]) + "</ul>"
        if info.get("resolved"):
            notes += '<p class="muted">No longer flagged since last month: %s.</p>' % html.escape(", ".join(info["resolved"]))
        board = '<p class="board"><b>Note to the board.</b> %s</p>' % html.escape(info["board_note"]) if info.get("board_note") else ""
        return ('<section class="card"><div class="eyebrow">%s &middot; %s</div><h1>%s</h1>'
                '<p class="muted" style="margin:0">Reviewed by %s (FA). Please confirm by %s.</p></section>'
                '<section class="card"><div class="kpis">%s</div></section>'
                '<section class="card"><h2>New notes</h2>%s%s'
                '<p style="margin:12px 0 0"><a class="pdf" href="/snapshot/confirm/%s/%s/pdf" target="_blank" rel="noopener">'
                'Open the full snapshot (PDF)</a></p></section>') % (
            html.escape(info["entity"]), html.escape(info["month_label"]), html.escape(info["building"]),
            html.escape(info["fa_name"]), html.escape(info["due_label"]), kp, notes, board,
            html.escape(rid, quote=True), html.escape(raw, quote=True))

    def explain(st):
        s = st["state"]
        if s == "used" or s == "closed":
            c = st.get("closed") or {}
            who = service.directory.name(c.get("by")) if c.get("by") else st["pm"]["name"]
            did = "confirmed" if (c.get("decision") or st.get("used", {}).get("decision")) == "approve" else "asked for changes on"
            return msg("ok", "Already done", "%s %s this snapshot on %s. There is nothing more to do." % (
                html.escape(who), did, html.escape(c.get("at") or st.get("used", {}).get("at", ""))))
        if s == "replaced":
            return msg("", "This link was replaced", "The snapshot was updated or the email was sent again. Please use the newest email.")
        if s == "expired":
            return msg("bad", "This link has expired", "Ask your FA to send the snapshot again.")
        return msg("bad", "This link is not valid", "Please use the button in your most recent snapshot email.")

    @bp.route("/snapshot/confirm/<rid>/<raw>", methods=["GET"])
    def confirm_page(rid, raw):
        st = service.link_state(rid, raw)
        if st["state"] != "ok":
            return explain(st)
        form = ('<section class="card"><form method="post"><input type="hidden" name="decision" value="approve">'
                '<button class="btn ok" type="submit">Confirm this snapshot</button></form>'
                '<p class="muted" style="margin:10px 0 0">Confirming makes it final and saves it to the building\'s Monthly Financials folder.</p>'
                '<details style="margin-top:14px"><summary>Something needs to change?</summary>'
                '<form method="post" style="margin-top:8px"><input type="hidden" name="decision" value="request_changes">'
                '<label for="n" class="muted">Tell %s what to fix</label><textarea id="n" name="note" required></textarea>'
                '<button class="btn" type="submit">Send back for changes</button></form></details></section>') % html.escape(st["info"]["fa_name"])
        return page_out("Confirm snapshot", summary(st, rid, raw) + form)

    @bp.route("/snapshot/confirm/<rid>/<raw>", methods=["POST"])
    def confirm_post(rid, raw):
        decision = request.form.get("decision", "")
        note_txt = (request.form.get("note") or "").strip()
        meta_info = {"ip": request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip(),
                     "agent": (request.headers.get("User-Agent") or "")[:160]}
        try:
            service.pm_decide(rid, raw, decision, note_txt, meta=meta_info)
        except ValueError as e:
            st = service.link_state(rid, raw)
            if st["state"] != "ok":
                return explain(st)
            return msg("bad", "Not done", html.escape(str(e)))
        if decision == "approve":
            return msg("ok", "Confirmed. Thank you.", "The snapshot is final and is being saved to the building's Monthly Financials folder. You can close this page.")
        return msg("", "Sent back for changes", "Your note was sent to the FA. You will get a new email when it is updated.")

    @bp.route("/snapshot/confirm/<rid>/<raw>/pdf")
    def confirm_pdf(rid, raw):
        st = service.link_state(rid, raw)
        if st["state"] in ("unknown", "replaced"):
            return explain(st)
        return Response(service.render(st["rec"]), mimetype="application/pdf",
                        headers={"Content-Disposition": "inline; filename=%s.pdf" % rid, "Cache-Control": "no-store"})

    # ------------------------------------------------------------------ click-through only: read the 'sent' emails
    @bp.route("/snapshots/dev/outbox")
    def outbox():
        if not dev:
            return err("Not available.", 404)
        rows = []
        for i, m in enumerate(reversed(service.mailer.outbox)):
            rows.append('<section class="card"><div class="eyebrow">%s &middot; %s</div><h2 style="margin:4px 0">%s</h2>'
                        '<p class="muted" style="margin:0 0 10px">From %s to %s%s</p><div style="border:1px solid #e8e0d8;border-radius:8px;overflow:hidden">%s</div></section>' % (
                            html.escape(m.get("kind", "")), html.escape(m["status"]), html.escape(m["subject"]),
                            html.escape(m["sender"] or "?"), html.escape(", ".join(m["intended"])),
                            (" &middot; attached: " + html.escape(", ".join(m["attachments"]))) if m["attachments"] else "", m["html"]))
        body = '<section class="card"><h1>Test outbox</h1><p class="muted">Emails the click-through would send, newest first.</p></section>' + \
               ("".join(rows) or '<section class="card"><p class="muted">No emails yet.</p></section>')
        return page_out("Test outbox", body)

    return bp
