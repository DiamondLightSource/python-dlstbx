from flask import Flask

from dlstbx.prometheus_interface_tools import zocalo_database

app = Flask(__name__)

dbinterface = zocalo_database.ZocaloDBInterface()


@app.route("/metrics", methods=["GET"])
def create_prometheus_text():
    return dbinterface.prom_text


def run():
    app.run(host="0.0.0.0", port=8080)
