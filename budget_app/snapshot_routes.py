"""Monthly Financial Snapshot: Flask blueprint (click-through version, test data only)."""
import os

from flask import Blueprint, Response, jsonify, request


HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = os.path.join(HERE, "..", "tasks", "snapshot_samples")


def create_blueprint(service, identity=None, dev=True, signing_allowed=None, identity_detail=None, signin_url=None):
    bp = Blueprint("snapshots", __name__)

    def err(e, code=400):
        return jsonify({"error": str(e)}), code

    def uid():
        if not dev:  # production: only the signed-in identity counts, never a URL parameter
            return (identity() if identity else None) or 0
        try:
            return int(request.args.get("as") or (request.get_json(silent=True) or {}).get("as") or 0)
        except (TypeError, ValueError):
            return 0

    @bp.route("/snapshots")
    def page():
        with open(os.path.join(HERE, "snapshot_page.html"), encoding="utf-8") as f:
            return Response(f.read(), mimetype="text/html")

    @bp.route("/api/snapshots/meta")
    def meta():
        samples = sorted(f for f in os.listdir(SAMPLES) if f.endswith(".pdf")) if dev and os.path.isdir(SAMPLES) else []
        me = uid()
        return jsonify({"dev": dev, "me": {"id": me, "name": service.directory.name(me)} if me else None,
                        "signing_allowed": True if signing_allowed is None else bool(signing_allowed()),
                        "signin_url": signin_url, "signout_url": "/auth/snapshot/logout" if signin_url else None,
                        "users": service.directory.users(), "buildings": service.directory.buildings(),
                        "samples": samples})

    @bp.route("/api/snapshots")
    def listing():
        return jsonify(service.list())

    @bp.route("/api/snapshots/<rid>")
    def one(rid):
        try:
            return jsonify(service.view(rid, uid()))
        except ValueError as e:
            return err(e, 404)

    @bp.route("/api/snapshots/<rid>/pdf")
    def pdf(rid):
        try:
            rec = service.store.get(rid)
            if rec is None:
                raise KeyError(rid)
        except KeyError:
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
            if dev:
                who = int(request.form.get("as") or 0)
            else:  # production: the signed-in person, never a form field
                who = uid()
                if not who:
                    return err("Sign in with Microsoft first.", 401)
            rid = service.generate(entity, data, who, source)
            return jsonify({"id": rid})
        except Exception as e:  # parse failures must reach the screen, not a 500 page
            return err(e)

    def action(fn):
        try:
            fn()
            return jsonify({"ok": True})
        except ValueError as e:
            return err(e, 400)

    @bp.route("/api/snapshots/<rid>/edit", methods=["POST"])
    def edit(rid):
        b = request.get_json(force=True)
        return action(lambda: service.edit(rid, uid(), b["commentary"], b.get("board_note", "")))

    @bp.route("/api/snapshots/<rid>/ack", methods=["POST"])
    def ack(rid):
        b = request.get_json(force=True)
        return action(lambda: service.acknowledge(rid, uid(), b["check_id"], b.get("note", "")))

    @bp.route("/api/snapshots/<rid>/note", methods=["POST"])
    def note(rid):
        b = request.get_json(force=True)
        return action(lambda: service.confirm_note(rid, uid(), int(b["index"]), b.get("text")))

    @bp.route("/api/snapshots/<rid>/send", methods=["POST"])
    def send(rid):
        return action(lambda: service.send(rid, uid()))

    @bp.route("/api/snapshots/<rid>/sign", methods=["POST"])
    def sign(rid):
        if signing_allowed is not None and not signing_allowed():
            return err("Signing is switched off until a secure sign-in is in place. Nothing was signed.", 403)
        if not uid():
            return err("Sign in with Microsoft first." if signin_url else "Choose who you are first.", 401)
        b = request.get_json(force=True)
        via = identity_detail() if identity_detail else None
        return action(lambda: service.sign(rid, uid(), b["role"], b["decision"], b.get("note", ""), via=via))

    @bp.route("/api/snapshots/<rid>/release", methods=["POST"])
    def release(rid):
        if signing_allowed is not None and not signing_allowed():
            return err("Signing is switched off until a secure sign-in is in place.", 403)
        return action(lambda: service.release_now(rid, uid()))

    @bp.route("/api/snapshots/reset", methods=["POST"])
    def reset():
        if not dev:
            return err("Not available.", 404)
        service.store.reset()
        return jsonify({"ok": True})

    return bp
