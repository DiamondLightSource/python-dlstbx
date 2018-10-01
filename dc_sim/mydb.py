from __future__ import absolute_import, division, print_function

import ispyb
import logging
import time

log = logging.getLogger()

class DB:
    def __init__(self, *args, **kwargs):
        '''All parameters are ignored.'''
        self.i = ispyb.open('/dls_sw/dasc/mariadb/credentials/ispyb_scripts.cfg')
        self.cursor = None

    def connect(self):
        '''Connection attempts are exclusively handled by ISPyB API class.'''
        log.info("DB: connect method called")
        self.createCursor()

    def reconnect(self, cleanup=True):
        '''Automatic reconnection is exclusively handled by ISPyB API class.'''
        log.info("DB: reconnecting to mysql... :-|")
        self.connect()

    def createCursor(self):
        if self.cursor:
          log.debug('DB: Closing previous cursor')
          self.cursor.close()

        log.debug('DB: Creating new cursor')
        self.cursor = self.i.create_cursor()
        return self.cursor

    def disposeCursor(self, cursor=None):
        if cursor:
            assert cursor is self.cursor
            self.cursor.close()
            self.cursor = None
        elif self.cursor:
            self.cursor.close()
            self.cursor = None
        else:
            log.warning("DB: trying to dispose of an unknown cursor :-P")

    def doQuery(self, querystr, cursor=None, return_fetch=True, return_id=False, debug=False):
        log.debug("DB: %s" % querystr)

        if cursor is None:
            cursor = self.cursor

        start_time = time.time()
        try:
            ret = cursor.execute(querystr)
        except:
            log.exception("DB: exception running sql statement :-(")
            raise
        else:
            if debug:
                log.debug("DB: query took %f seconds" % (time.time() - start_time))

        if return_fetch:
            start_time = time.time()
            try:
                ret = cursor.fetchall()
            except:
                log.exception("DB: exception fetching cursor :-(")
                raise
            if debug:
                log.debug("DB: fetch took %f seconds" % (time.time() - start_time))
        elif return_id:
            start_time = time.time()

            try:
                ret=int(self.dbConnection.insert_id())
            except:
                log.exception("DB: exception getting inserted id :-(")
                raise
            if debug:
                log.debug("DB: id took %f seconds" % (time.time() - start_time))

        return ret
