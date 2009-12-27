#!/usr/bin/env python

from os import listdir,path,access,F_OK
from sqlite3 import connect,Row

import settings

# Major TODO: Handle exceptions for all SQL operations

class NoSuchDatabase(Exception):
    pass


def _sanitize(name):
    """Filter SQL column and table names"""
    return str(name).replace('`', '\`')


def _connect(database,create=False):
    if create is False and not access(database,F_OK):
        raise NoSuchDatabase
    return connect(database)


def list_databases():
    """List all databases"""
    return listdir(settings.data_path)


def list_tables(database):
    """List all tables"""
    # TODO: Sanitize 'database' var for directory traversal
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute("""SELECT name FROM sqlite_master WHERE type='table'
        ORDER BY name""")
    tables = [table[0] for table in cursor]
    cursor.close()
    conn.close()
    return tables


def list_columns(database,table):
    """List columns in a given table"""

    conn = _connect(path.join(settings.data_path,database))
    conn.row_factory = Row
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM `%s` LIMIT 1""" % _sanitize(table))
    c = cursor.fetchone()
    columns = c.keys()
    cursor.close()
    conn.close()
    return columns


def all_records(database,table):
    """Return all records in a given table - Generator function"""
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()

    cursor.execute("SELECT ROWID,* FROM `%s`" % _sanitize(table))
    for row in cursor:
        yield row
    cursor.close()
    conn.close()


def get_record(database,table,rowid):
    """Return record in a given table based on ROWID"""
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute("SELECT ROWID,* FROM `%s` WHERE rowid=?" %
        _sanitize(table), rowid)
    record = cursor.fetchone()
    cursor.close()
    conn.close()
    return record


def insert_record(database,table,**kwargs):
    """INSERT new record into a database and return ROWID"""

    if not kwargs:
        return  # Can't INSERT data if we don't have it

    # Build SQL INSERT statement
    columns = ','.join(['`%s`' % _sanitize(var)
        for var in kwargs.iterkeys()])
    statement = 'INSERT INTO `%s` (%s) VALUES (' % (
        _sanitize(table),columns)
    statement += ','.join(['?']*len(kwargs.keys())) + ')'

    # Connect to database and return new record's ID
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute(statement, tuple([val for val in kwargs.itervalues()]))
    record_id = cursor.lastrowid
    cursor.close()
    conn.commit()
    conn.close()
    return record_id


def replace_record(database,table,rowid,seq):
    """REPLACE entire records"""

    if not seq:
        return

    # Build SQL REPLACE statement
    # TODO: Escape single-quotes, etc
    columns = ','.join(["`%s`" % _sanitize(col)
        for col in list_columns(database,table)])
    values = ','.join(["'%s'" % str(val) for val in seq])
    statement = "REPLACE INTO `%s` (ROWID,%s) VALUES (%s,%s)" % (
        _sanitize(table),columns,str(rowid),values)

    # Connect to database and return new record's ID
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute(statement)
    cursor.close()
    conn.commit()
    conn.close()


def update_record(database,table,rowid,**kwargs):
    """UPDATE existing records"""

    if not kwargs:
        return

    # Build SQL UPDATE statement
    vals = ','.join(["`%s`='%s'" % (_sanitize(k), str(v))
        for k,v, in kwargs.iteritems()])
    statement = "UPDATE `%s` SET %s WHERE ROWID=?" % (
        _sanitize(table),vals)

    # Connect to database and return new record's ID
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute(statement,rowid)
    cursor.close()
    conn.commit()
    conn.close()


def delete_record(database,table,rowid):
    """DELETE record with given ROWID"""

    # Connect to database and delete record
    conn = _connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute("""DELETE FROM `%s` WHERE ROWID=?""" %
        _sanitize(table), rowid)
    cursor.close()
    conn.commit()
    conn.close()


