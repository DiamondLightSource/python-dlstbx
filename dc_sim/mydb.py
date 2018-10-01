import MySQLdb
import string
import logging
from logging.handlers import RotatingFileHandler
import time
import os
#import XSD

class DB:
    _typeDict = {'int':'integer', 'varchar':'string', 'datetime':'datetime', \
        'char':'boolean', 'float':'double', 'timestamp':'string', \
        'longtext':'string', 'enum':'string', 'date':'date', \
        'double':'double', 'tinytext':'string', 'clob':'string', 'number':'integer'}
    _fieldType = {}
    _attList = {}

    CONTAINER_STATUS = 'processing'

    CONN_INACTIVITY = 360

    def __init__(self, host, user, passwd, db, port=3306, conn_inactivity=None):
        if conn_inactivity is None:
            conn_inactivity = self.CONN_INACTIVITY
        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbName = db
        self.dbConnection = None
        self.port = int(port)
        self.cursor = None
        self.lastCursorTime = None
        self.connInactivity = conn_inactivity

    def connect(self):
        # logging.getLogger().info("DB: connecting to %s@%s/%s" % (self.user,self.host,self.dbName))
        logging.getLogger().info("DB: connecting to %s@%s" % (self.user, self.dbName))
        logging.getLogger().debug("DB: inactivity reconnection after %d minutes" % self.connInactivity)

        if self.dbConnection is None:
            try:
                #self.dbConnection=cx_Oracle.Connect(self.host,self.user,self.passwd,self.dbName)
                self.dbConnection = MySQLdb.connect(user=self.user, passwd=self.passwd, host=self.host, db=self.dbName, port=self.port)
            except:
                logging.getLogger().exception("DB: error while connecting :-(")
            else:
                try:
                    self.dbConnection.autocommit(True)
                    #self.dbConnection.autocommit = True
                except AttributeError:
                    pass
                logging.getLogger().debug("DB: mysql connection ok :-)")
                self.lastCursorTime = time.time()
                try:
                    self.cursor = self.dbConnection.cursor()
                except:
                    logging.getLogger().exception("DB: unable to create cursor :-(")
                else:
                    logging.getLogger().debug("DB: default cursor ok :-)")
        else:
            logging.getLogger().warning("DB: already connected! :-P")

    def reconnect(self, cleanup=True):
        logging.getLogger().info("DB: reconnecting to mysql... :-|")

        if cleanup:
            if self.cursor is not None:
                logging.getLogger().warning("DB: this will close the default cursor :-|")
                try:
                    self.cursor.close()
                except:
                    logging.getLogger().exception("DB: exception while closing default cursor :-(")
                self.cursor = None

            if self.dbConnection is not None:
                try:
                    self.dbConnection.close()
                except:
                    logging.getLogger().exception("DB: exception while closing the connection :-(")
                self.dbConnection = None
        else:
            self.dbConnection = None
            self.cursor = None

        self.connect()

    def createCursor(self):
        now = time.time()
        if self.lastCursorTime is not None:
            interval = int((now - self.lastCursorTime) / 60)
            if interval >= self.connInactivity:
                self.reconnect()
        if self.dbConnection is None:
            logging.getLogger().error("DB: unable to create cursor without being connected :-P")
            return None

        cursor = None
        ping_error = False

        try:
            #ping_res=self.dbConnection.ping()
            ping_res = self.doQuery("select 'ping'", cursor, debug=False)
        except:
            logging.getLogger().exception("DB: error ping'ing database server :-(")
            ping_error = True
        else:
            if ping_res != [('ping',)]:
                logging.getLogger().error("DB: error ping'ing database server: %s :-( " % str(ping_res))
                ping_error = True

        if ping_error:
            self.reconnect()

        self.lastCursorTime = time.time()
        try:
            cursor = self.dbConnection.cursor()
        except:
            logging.getLogger().exception("DB: unable to create cursor :-(")

        return cursor

    def disposeCursor(self, cursor):
        if cursor is not None:
            cursor.close()
        else:
            logging.getLogger().warning("DB: trying to dispose of an unknown cursor :-P")


    def doQuery(self, querystr, cursor=None, return_fetch=True, return_id=False, debug=False):
        logging.getLogger().debug("DB: %s" % querystr)

        if cursor is None:
            cursor = self.cursor

        start_time = time.time()
        try:
            ret = cursor.execute(querystr)
        except:
            logging.getLogger().exception("DB: exception running sql statement :-(")
            raise
        else:
            if debug:
                logging.getLogger().debug("DB: query took %f seconds" % (time.time() - start_time))

        if return_fetch:
            start_time = time.time()
            try:
                ret = cursor.fetchall()
            except:
                logging.getLogger().exception("DB: exception fetching cursor :-(")
                raise
            if debug:
                logging.getLogger().debug("DB: fetch took %f seconds" % (time.time() - start_time))
        elif return_id:
            start_time = time.time()

            try:
                ret=int(self.dbConnection.insert_id())
            except:
                logging.getLogger().exception("DB: exception getting inserted id :-(")
                raise
            if debug:
                logging.getLogger().debug("DB: id took %f seconds" % (time.time() - start_time))

        return ret
