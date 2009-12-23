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

# Major TODO: Handle exceptions for all SQL operations

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


def all_records(database,table):
    """Return all records in a given table - Generator function"""
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()

    # TODO: santize 'table' for SQL injection
    cursor.execute("SELECT ROWID,* FROM `%s`" % table)
    for row in cursor:
        yield row
    cursor.close()
    conn.close()


def get_record(database,table,rowid):
    """Return record in a given table based on ROWID"""
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    # TODO: santize 'table' for SQL injection
    cursor.execute("SELECT ROWID,* FROM `%s` WHERE rowid=?" % table,
        rowid)
    record = cursor.fetchone()
    cursor.close()
    conn.close()
    return record


def insert_record(database,table,**kwargs):
    """INSERT new records into a database and return ROWID"""

    if not kwargs:
        return  # Can't INSERT data if we don't have it

    # Build SQL INSERT statement
    # TODO: Escape single-quotes, etc
    columns = ','.join(['`%s`' % str(var) for var in kwargs.iterkeys()])
    values = ','.join(["'%s'" % str(val) for val in kwargs.itervalues()])
    statement = "INSERT INTO `%s` (%s) VALUES (%s)" % (table,columns,values)

    # Connect to database and return new record's ID
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute(statement)
    record_id = cursor.lastrowid
    cursor.close()
    conn.commit()
    conn.close()
    return record_id


def update_record(database,table,rowid,**kwargs):
    """UPDATE existing records"""

    if not kwargs:
        return

    # Build SQL UPDATE statement
    # TODO: Escaping, etc
    vals = ','.join(["`%s`='%s'" % (str(k), str(v))
        for k,v, in kwargs.iteritems()])
    statement = "UPDATE `%s` SET %s WHERE ROWID='%s'" % (table,vals,rowid)

    # Connect to database and return new record's ID
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute(statement)
    cursor.close()
    conn.commit()
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
    def get(self,database,table,rowid=None):
        """Dumps a single record or all records from a table"""
        if rowid:
            self.write(dumps(get_record(database,table,rowid)))
        else:
            self.write(dumps([row for row in all_records(database,table)]))

    def post(self,database,table,rowid=None):
        """INSERT or UPDATE records"""
        # Prepare request (POST) vars. We only have to do this because
        # we get a dict of lists and our funcs want a dict of single
        # values
        kwargs = {}
        for k,v in self.request.arguments.iteritems():
            kwargs[k] = v[0] 

        if rowid:
            # Perform UPDATE
            self.write(dumps(update_record(database,table,rowid,**kwargs)))
        else:
            # Perform INSERT
            self.write(dumps(insert_record(database,table,**kwargs)))


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

