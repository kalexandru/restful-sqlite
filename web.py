#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.auth
from tornado.web import HTTPError

from tornado.escape import json_encode as dumps
from tornado.escape import json_decode as loads

import db
import settings

# Major TODO: Handle exceptions for all SQL operations


class MainHandler(tornado.web.RequestHandler):
    """Main Handler... list all databases"""

    def get(self):
        self.write(dumps(db.list_databases()))


class ListTableHandler(tornado.web.RequestHandler):
    """List tables in specified database"""

    def get(self,database):
        try:
            self.write(dumps(db.list_tables(database)))
        except db.NoSuchDatabase:
            raise HTTPError(404)


class DataHandler(tornado.web.RequestHandler):
    def get(self,database,table,rowid=None):
        """Dumps a single record or all records from a table"""
        try:
            if rowid:
                self.write(dumps(db.get_record(database,table,rowid)))
            else:
                self.write(dumps([row
                    for row in db.all_records(database,table)]))
        except db.NoSuchDatabase:
            raise HTTPError(404)

    def post(self,database,table,rowid=None):
        """INSERT or UPDATE records"""
        # Prepare request (POST) vars. We only have to do this because
        # we get a dict of lists and our funcs want a dict of single
        # values
        kwargs = {}
        for k,v in self.request.arguments.iteritems():
            kwargs[k] = v[0] 

        try:
            if rowid:
                # Perform UPDATE
                json = dumps(db.update_record(database,table,rowid,**kwargs))
                self.write(json)

            else:
                # Perform INSERT
                id = db.insert_record(database,table,**kwargs)
                self.set_status = 201   # HTTP 201 Created
                loc = '/%s/%s/%d' % (database,table,id)
                self.set_header('Location', loc)
                self.write(loc)

        except db.NoSuchDatabase:
            raise HTTPError(404)
        

    def put(self,database,table,rowid=None):
        """REPLACE record"""

        if not rowid:
            raise HTTPError(405) # We need a rowid to use REPLACE

        obj = loads(self.request.body)
        try:
            db.replace_record(database,table,rowid,obj)
        except db.NoSuchDatabase:
            raise HTTPError(404)

    def delete(self,database,table,rowid=None):
        """DELETE record"""

        if not rowid:
            raise HTTPError(405) # Need ROWID

        try:
            db.delete_record(database,table,rowid)
        except db.NoSuchDatabase:
            raise HTTPError(404)


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

