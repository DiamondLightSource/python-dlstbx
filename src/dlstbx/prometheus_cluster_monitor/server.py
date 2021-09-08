from flask import Flask

from dlstbx.prometheus_cluster_monitor import parse_db

app = Flask(__name__)

dbparser = parse_db.DBParser()


@app.route("/metrics", methods=["GET"])
def create_prometheus_text():
    return dbparser.text


def run():
    app.run(host="0.0.0.0", port=8080)
