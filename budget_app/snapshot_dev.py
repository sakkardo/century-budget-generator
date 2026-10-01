"""Run the snapshot click-through locally (test people, test data, local folders).

    python budget_app/snapshot_dev.py      # then open http://localhost:5057/snapshots
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flask import Flask, redirect

import snapshot_routes
import snapshot_service

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tasks", "snapshot_dev_data")


def make_app(root=ROOT):
    root = os.path.abspath(root)
    store = snapshot_service.Store(os.path.join(root, "store.json"))
    service = snapshot_service.Service(store, snapshot_service.LocalReleaser(os.path.join(root, "sharepoint_test_copy")),
                                       snapshot_service.StaticDirectory())
    app = Flask(__name__)
    app.register_blueprint(snapshot_routes.create_blueprint(service))
    app.add_url_rule("/", "home", lambda: redirect("/snapshots"))
    return app


if __name__ == "__main__":
    make_app().run(port=5057, debug=False)
