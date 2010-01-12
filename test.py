#!/usr/bin/env python

"""Unit Tests for SQLitebot (restful-sqlite)"""

import db
import unittest

from os import unlink,mkdir,removedirs,path,access,F_OK
from random import randint

TEST_DIR = '/tmp/testdb%d' % randint(0,99)
TEST_DB = 'test'
TEST_TABLE = 'testtbl'
TEST_TABLESQL = """CREATE TABLE `%s` (col1 INT, col2 TEXT)""" % TEST_TABLE

class TestDBConnect(unittest.TestCase):
    """Test DB connect and create funcs"""

    def setUp(self):
        mkdir(TEST_DIR)

    def testconnectnocreate(self):
        self.assertRaises(db.NoSuchDatabase, db._connect, TEST_DB, False)

    def testconnectwithcreate(self):
        conn = db._connect(TEST_DB, True)
        self.assertNotEqual(conn, None, '_connect returned None')
        conn.close()

    def tearDown(self):
        if access(TEST_DB,F_OK):
            unlink(TEST_DB) 

        if access(TEST_DIR,F_OK):
            removedirs(TEST_DIR)


class TestDBReadFuncs(unittest.TestCase):
    """Test DB read-only funcs"""

    def setUp(self):
        mkdir(TEST_DIR)
        db.settings.data_path = TEST_DIR
        self.conn = db._connect(path.join(TEST_DIR,TEST_DB),True)

    def test_list_databases(self):
        res = db.list_databases()
        self.assertEqual(res, [TEST_DB],
            'list_databases should only include TEST_DB')

    def test_list_tables(self):
        res = db.list_tables(TEST_DB)
        self.assertEqual(res, [],
            'list_tables should be empty at this stage')

        self.conn.execute(TEST_TABLESQL)
        res = db.list_tables(TEST_DB)
        self.assertEqual(res, [TEST_TABLE],
            'list_tables should only include TEST_TABLE at this stage')

    def test_list_columns(self):
        self.assertRaises(db.NoSuchTable, db.list_columns, TEST_DB, TEST_TABLE)

        self.conn.execute(TEST_TABLESQL)
        expected_result = ['col1','col2']
        res = db.list_columns(TEST_DB,TEST_TABLE)
        self.assertEqual(res, expected_result,
            '''Incorrect value returned from list_columns
                expected: %s
                got: %s''' % (repr(expected_result), repr(res)))

    def test_all_records(self):
        self.conn.execute(TEST_TABLESQL)
        res = [rec for rec in db.all_records(TEST_DB,TEST_TABLE)]
        self.assertEqual(res, [],
            'all_records should return an empty set at this stage')

        expected_set = []
        for i in range(10):
            num = randint(0,99)
            expected_set.append({'rowid':num, 'col1':num, 'col2':'TEST%d' % num})
            self.conn.execute("""INSERT INTO `%s` VALUES (?,?)""" % TEST_TABLE,
                (num, 'TEST%d' % num))

        for record in db.all_records(TEST_DB, TEST_TABLE):
            self.assert_(record in expected_set)

    def test_get_record(self):
        self.conn.execute(TEST_TABLESQL)
        res = db.get_record(TEST_DB,TEST_TABLE,1)
        self.assertEqual(res, None)

        num = randint(0,99)
        expected_result = {'rowid': 1, 'col1':num, 'col2':'TEST%d' % num}
        self.conn.execute("""INSERT INTO `%s` (ROWID,col1,col2)
            VALUES (1,?,?)""" % TEST_TABLE, (num, 'TEST%d' % num))
        self.conn.commit()
        res = db.get_record(TEST_DB,TEST_TABLE,1)
        self.assertEqual(res, expected_result,
            '''get_record did not return expected result.
                expected: %s
                got: %s''' % (repr(expected_result), repr(res)))

    def tearDown(self):
        self.conn.close()
        if access(path.join(TEST_DIR,TEST_DB),F_OK):
            unlink(path.join(TEST_DIR,TEST_DB))

        if access(TEST_DIR,F_OK):
            removedirs(TEST_DIR)


class TestDBWriteFuncs(unittest.TestCase):
    """Test 'write' DB funcs"""

    def setUp(self):
        mkdir(TEST_DIR)
        db.settings.data_path = TEST_DIR
        self.conn = db._connect(path.join(TEST_DIR,TEST_DB),True)
        self.conn.execute(TEST_TABLESQL)
        self.conn.close()
        self.conn = None

    def test_insert_record(self):
        id = db.insert_record(TEST_DB,TEST_TABLE,col1=123,col2='abc')
        res = db.get_record(TEST_DB,TEST_TABLE,1)
        self.assertNotEqual(id, None)
        self.assertEqual(res, {'rowid':id,'col1':123,'col2':'abc'})

    def test_update_record(self):
        # TODO: define behavior for when a record does not exist
        id = db.insert_record(TEST_DB,TEST_TABLE,col1=123,col2='abc')

        db.update_record(TEST_DB,TEST_TABLE,999,col2='def')
        res = db.get_record(TEST_DB,TEST_TABLE,id)
        self.assertEqual(res, {'rowid':id,'col1':123,'col2':'abc'})

        db.update_record(TEST_DB,TEST_TABLE,id,col2='def')
        res = db.get_record(TEST_DB,TEST_TABLE,1)
        self.assertEqual(res, {'rowid':id,'col1':123,'col2':'def'})

    def test_replace_record(self):
        # TODO: define behavior for when a record does not exist
        id = db.insert_record(TEST_DB,TEST_TABLE,col1=123,col2='abc')
        db.replace_record(TEST_DB,TEST_TABLE,id,[555,'def'])
        res = db.get_record(TEST_DB,TEST_TABLE,id)
        self.assertEqual(res, {'rowid':id,'col1':555,'col2':'def'})

    def test_delete_record(self):
        # TODO: define behavior for when a record does not exist
        id = db.insert_record(TEST_DB,TEST_TABLE,col1=123,col2='abc')
        db.delete_record(TEST_DB,TEST_TABLE,id)
        res = db.get_record(TEST_DB,TEST_TABLE,id)
        self.assertEqual(res, None)

    def tearDown(self):
        if self.conn:
            self.conn.close()

        if access(path.join(TEST_DIR,TEST_DB),F_OK):
            unlink(path.join(TEST_DIR,TEST_DB))

        if access(TEST_DIR,F_OK):
            removedirs(TEST_DIR)


if __name__ == '__main__':
    unittest.main()

