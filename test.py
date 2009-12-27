#!/usr/bin/env python

"""Unit Tests for SQLitebot (restful-sqlite)"""

import db
import unittest

from os import unlink,mkdir,removedirs,path,access,F_OK
from random import randint
import sqlite3

TESTDIR = '/tmp/testdb%d' % randint(0,99)
TESTDB = 'test'
TESTTBL = 'testtbl'
TESTTBLSQL = """CREATE TABLE `%s` (col1 INT, col2 TEXT)""" % TESTTBL

class TestDBConnect(unittest.TestCase):
    def setUp(self):
        mkdir(TESTDIR)

    def testconnectnocreate(self):
        self.assertRaises(db.NoSuchDatabase, db._connect, TESTDB, False)

    def testconnectwithcreate(self):
        conn = db._connect(TESTDB, True)
        self.assertNotEqual(conn, None, '_connect returned None')
        conn.close()

    def tearDown(self):
        if access(TESTDB,F_OK):
            unlink(TESTDB) 

        if access(TESTDIR,F_OK):
            removedirs(TESTDIR)


class TestDBMainFuncs(unittest.TestCase):
    def setUp(self):
        mkdir(TESTDIR)
        db.settings.data_path = TESTDIR
        self.conn = db._connect(path.join(TESTDIR,TESTDB),True)

    def test_list_databases(self):
        res = db.list_databases()
        self.assertEqual(res, [TESTDB],
            'list_databases should only include TESTDB')

    def test_list_tables(self):
        res = db.list_tables(TESTDB)
        self.assertEqual(res, [],
            'list_tables should be empty at this stage')

        self.conn.execute(TESTTBLSQL)
        res = db.list_tables(TESTDB)
        self.assertEqual(res, [TESTTBL],
            'list_tables should only include TESTTBL at this stage')

    def test_all_records(self):
        self.conn.execute(TESTTBLSQL)
        res = [rec for rec in db.all_records(TESTDB,TESTTBL)]
        self.assertEqual(res, [],
            'all_records should return an empty set at this stage')

        set = []
        for i in range(10):
            num = randint(0,99)
            set.append( (num, 'TEST%d' % num) )
            self.conn.execute("""INSERT INTO `%s` VALUES (?, ?)""" % TESTTBL,
                (num, 'TEST%d' % num))

        for record in db.all_records(TESTDB, TESTTBL):
            self.assert_(record in set)

    def test_get_record(self):
        self.conn.execute(TESTTBLSQL)
        res = db.get_record(TESTDB,TESTTBL,1)
        self.assertEqual(res, None)

        num = randint(0,99)
        expected_result = (1, num, 'TEST%d' % num)
        self.conn.execute("""INSERT INTO `%s` (ROWID,col1,col2)
            VALUES (1,?,?)""" % TESTTBL, (num, 'TEST%d' % num))
        self.conn.commit()
        res = db.get_record(TESTDB,TESTTBL,1)
        self.assertEqual(res, expected_result,
            'get_record did not return expected result. expected: %s. got: %s' % (repr(expected_result), repr(res)))

        
    def tearDown(self):
        self.conn.close()
        if access(path.join(TESTDIR,TESTDB),F_OK):
            unlink(path.join(TESTDIR,TESTDB))

        if access(TESTDIR,F_OK):
            removedirs(TESTDIR)


if __name__ == '__main__':
    unittest.main()

