#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.auth

from sqlite3 import connect

import settings

def list_databases():
    """List all databases"""
    from os import listdir
    return listdir(settings.data_path)


def list_tables(database):
    """List all tables"""
    from os import path
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
    from os import path
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    # TODO: santize 'table' for SQL injection
    cursor.execute("SELECT * FROM `%s`" % table)
    data = [row for row in cursor]
    cursor.close()
    conn.close()
    return data


class MainHandler(tornado.web.RequestHandler):
    """Main Handler... list all databases"""

    def get(self):
        self.write(repr(list_databases()))


class ListTableHandler(tornado.web.RequestHandler):
    """List tables in specified database"""

    def get(self,database):
        self.write(repr(list_tables(database)))


class DataHandler(tornado.web.RequestHandler):
    """Dump all records from a table"""

    def get(self,database,table):
        self.write(repr(dump_data(database,table)))


application = tornado.web.Application([
    (r"/", MainHandler),
    (r"/([\w]+)/", ListTableHandler),
    (r"/([\w]+)/([\w]+)/", DataHandler),
],
    cookie_secret=settings.cookie_secret,
)

if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(settings.port)
    tornado.ioloop.IOLoop.instance().start()

