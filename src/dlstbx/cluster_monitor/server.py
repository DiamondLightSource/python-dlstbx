from flask import Flask

from dlstbx.cluster_monitor import parse_db

app = Flask(__name__)

dbparser = parse_db.DBParser()


@app.route("/", methods=["PUT"])
def create_prometheus_text():
    return dbparser.text


def run():
    app.run()
