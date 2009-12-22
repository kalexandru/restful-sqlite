#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.auth

from os import listdir,path
from sqlite3 import connect

from tornado.escape import json_encode as dumps
from tornado.escape import json_decode as loads

import settings

def list_databases():
    """List all databases"""
    return listdir(settings.data_path)


def list_tables(database):
    """List all tables"""
    # TODO: Sanitize 'database' var for directory traversal
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute("""SELECT name FROM sqlite_master WHERE type='table'
        ORDER BY name""")
    tables = [table for table in cursor]
    cursor.close()
    conn.close()
    return tables


def dump_data(database,table):
    """Dump all records in a table"""
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()

    # TODO: santize 'table' for SQL injection
    cursor.execute("SELECT ROWID,* FROM `%s`" % table)
    for row in cursor:
        yield row
    cursor.close()
    conn.close()


def get_record(database,table,rowid):
    """Dump all records in a table"""
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    # TODO: santize 'table' for SQL injection
    cursor.execute("SELECT ROWID,* FROM `%s` WHERE rowid=?" % table,
        rowid)
    return cursor.fetchone()
    cursor.close()
    conn.close()


class MainHandler(tornado.web.RequestHandler):
    """Main Handler... list all databases"""

    def get(self):
        self.write(dumps(list_databases()))


class ListTableHandler(tornado.web.RequestHandler):
    """List tables in specified database"""

    def get(self,database):
        self.write(dumps(list_tables(database)))


class DataHandler(tornado.web.RequestHandler):
    """Dump all records from a table"""

    def get(self,database,table,rowid=None):
        if rowid:
            self.write(dumps(get_record(database,table,rowid)))
        else:
            self.write(dumps([row for row in dump_data(database,table)]))


application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/([\w_\-\.]+)/", ListTableHandler),
    (r"/([\w_\-\.]+)/([\w]+)/([\d]+)?", DataHandler),
],
    cookie_secret=settings.cookie_secret,
)

if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(settings.port)
    tornado.ioloop.IOLoop.instance().start()

