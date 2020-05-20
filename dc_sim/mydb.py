import logging

import ispyb.connector.mysqlsp.main
from six.moves import configparser

log = logging.getLogger("dlstbx.dc_sim")


class DB:
    def __init__(self):
        config = configparser.RawConfigParser(allow_no_value=True)
        assert config.read("/dls_sw/dasc/mariadb/credentials/ispyb_scripts.cfg")
        credentials = dict(config.items("prod"))
        self.i = ispyb.connector.mysqlsp.main.ISPyBMySQLSPConnector(**credentials)
        self.cursor = self.i.create_cursor()

    def doQuery(self, querystr):
        cursor = self.i.create_cursor()
        log.debug("DB: %s", querystr)
        try:
            cursor.execute(querystr)
            return cursor.fetchall()
        finally:
            cursor.close()
