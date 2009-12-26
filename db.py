#!/usr/bin/env python

from os import listdir,path
from sqlite3 import connect,Row

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


def list_columns(database,table):
    """List columns in a given table"""

    conn = connect(path.join(settings.data_path,database))
    conn.row_factory = Row
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM `%s` LIMIT 1""" % table)
    c = cursor.fetchone()
    columns = c.keys()
    cursor.close()
    conn.close()
    return columns


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
    """INSERT new record into a database and return ROWID"""

    if not kwargs:
        return  # Can't INSERT data if we don't have it

    # Build SQL INSERT statement
    # TODO: Escape backticks, etc
    columns = ','.join(['`%s`' % str(var) for var in kwargs.iterkeys()])
    statement = 'INSERT INTO `%s` (%s) VALUES (' % (table,columns)
    statement += ','.join(['?']*len(kwargs.keys())) + ')'

    # Connect to database and return new record's ID
    conn = connect(path.join(settings.data_path,database))
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
    columns = ','.join(["`%s`" % str(col) for col in list_columns(database,table)])
    values = ','.join(["'%s'" % str(val) for val in seq])
    statement = "REPLACE INTO `%s` (ROWID,%s) VALUES (%s,%s)" % (
        table,columns,str(rowid),values)

    # Connect to database and return new record's ID
    conn = connect(path.join(settings.data_path,database))
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


def delete_record(database,table,rowid):
    """DELETE record with given ROWID"""

    # Connect to database and delete record
    conn = connect(path.join(settings.data_path,database))
    cursor = conn.cursor()
    cursor.execute("""DELETE FROM `%s` WHERE ROWID=?""" % table, rowid)
    cursor.close()
    conn.commit()
    conn.close()


