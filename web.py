#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.auth

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
        self.write(dumps(db.list_tables(database)))


class DataHandler(tornado.web.RequestHandler):
    def get(self,database,table,rowid=None):
        """Dumps a single record or all records from a table"""
        if rowid:
            self.write(dumps(db.get_record(database,table,rowid)))
        else:
            self.write(dumps([row for row in db.all_records(database,table)]))

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
            self.write(dumps(db.update_record(database,table,rowid,**kwargs)))
        else:
            # Perform INSERT
            self.write(dumps(db.insert_record(database,table,**kwargs)))

    def put(self,database,table,rowid=None):
        """REPLACE record"""

        if not rowid:
            raise HTTPError(405) # We need a rowid to use REPLACE

        obj = loads(self.request.body)
        db.replace_record(database,table,rowid,obj)

    def delete(self,database,table,rowid=None):
        """DELETE record"""

        if not rowid:
            raise HTTPError(405) # Need ROWID

        db.delete_record(database,table,rowid)


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

