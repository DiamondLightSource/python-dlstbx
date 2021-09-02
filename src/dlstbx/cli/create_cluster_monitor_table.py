import argparse
import json
import pathlib

import sqlalchemy

from dlstbx import cluster_monitor


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--credentials", dest="creds")
    args = parser.parse_args()
    configuration = pathlib.Path(args.creds).read_text()
    secret_ingredients = json.loads(configuration)
    url = "mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}".format(
        **secret_ingredients
    )
    engine = sqlalchemy.create_engine(url, echo=True, connect_args={"use_pure": True})
    cluster_monitor.PrometheusClusterMonitor.__table__.create(engine)
