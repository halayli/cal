"""Cassandra Abstraction Library.

CAL abstracts Cassandra's confusing terminologies and allows you to use
Python like syntax to access/manipulate Cassandra records.

The library is not complete and doesn't support batch inserts/deletions yet.

Usage:
Assuming Users is a columnFamily of type Super in a Cassandara node.

from cal import dbConnect
keyspace = dbConnect()['MyDatabase']
users = keyspace['Users']

To insert a column/superColumn:
  users['joe']['personalInfo'].insert('emailAddress', 'joe@abc.com')

To fetch column/columns:
  users['joe'].get()
or more granular
  users['joe']['personalInfo'].get()
which is almost equivalent to:
  users['joe'].get(['personalInfo'])

To delete a column:
  del users['joe']
or
  del users['joe']['personalInfo']
or
  del users['joe']['personalInfo']['emailAddress']

To count the number of columns:
  users['joe'].count()
  or
  users['joe']['personalInfo'].count()

If Users is a columnFamily of type Standard, then you only nest one level deep:
users['joe'].insert('emailAddress', 'joe@abc.com')
"""

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from cassandra import Cassandra
from cassandra.ttypes import *

import time

def dbConnect(host='localhost', port=9160):
    """Returns _keySpaces object holding existing keyspaces in Cassandara.

    :Parameters:
        - `host`: Host to connect, default 'localhost' 
        - `port`: Port to connect to on host, default 9160

    This is all you need from the library to access/manipulate any cassandra
    record. Read the module docstring for examples.
    """

    socket = TSocket.TSocket("localhost", 9160)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    try:
        transport.open()
    except Thrift.TException, tx:
        print 'Thrift: %s' % tx.message
        raise
    return _keySpaces(client)

class _keySpaces:
    def __init__(self, client):
        self._client = client
        self._keySpaces = [ks for ks in client.describe_keyspaces()]

    def __getitem__(self, item):
        if item in self._keySpaces:
            return _KeySpace(self._client, item)

    def __repr__(self):
        return '<keySpaces: %s>' % repr(self._keySpaces)

class _KeySpace(object):
    def __init__(self, client, ks):
        self._client = client
        self._keySpace = ks
        self._columnFamilies = [cf for cf in client.describe_keyspace(ks)]

    def __getitem__(self, cf):
        if cf in self._columnFamilies:
            return _ColumnFamily(self._client, self._keySpace, cf)
        else:
            assert False, 'ColumnFamily %s does not exist' % cf

    def __repr__(self):
        return '<keySpace: %s, columnFamilies: %s>' % (self._keySpace,
            repr(self._columnFamilies))

class _ColumnFamily(object):
    def __init__(self, client, ks, cf):
        self._client = client
        self._keySpace = ks
        self._columnFamily = cf

    def __getitem__(self, csKey):
        return _SuperColumn(self._client, self._keySpace,
            self._columnFamily, csKey)
    
    def __delitem__(self, csKey):
        columnPath = ColumnPath(column_family=self._columnFamily)
        self._client.remove(self._keySpace, csKey, columnPath, time.time(),
            ConsistencyLevel.ONE)

    def __repr__(self):
        return '<keySpace: %s, columnFamily: %s>' % (self._keySpace,
            self._columnFamily)

class _SuperColumn(object):
    def __init__(self, client, ks, cf, csKey, cs=None):
        self._client = client
        self._keySpace = ks
        self._columnFamily = cf
        self._superColumn = cs
        self._columnKey = csKey

    def __repr__(self):
        return '<keySpace: %s, columnFamily: %s, column: %s, superColumn: %s>'%\
            (self._keySpace, self._columnFamily,
            self._columnKey, self._superColumn)

    def __delitem__(self, csKey):
        c = self._superColumn
        sc = csKey
        # if we have a super column, then swap the column/superColumn values
        if self._superColumn:
            sc, c = (c, sc)
        columnPath = ColumnPath(column_family=self._columnFamily,
            column=c, super_column=sc)
        self._client.remove(self._keySpace, self._columnKey, columnPath,
            time.time(), ConsistencyLevel.ONE)

    def _formatColumnValue(self, column):
        return {'value': column.value, 'timestamp': column.timestamp}

    def __getitem__(self, item):
        self._superColumn = item
        return self

    def get(self, key=[], start='', finish='', limit=100, reversed=False):
        """Get a column by column name or all columns if no name specified.

        :Parameters:
            - `key`: optional list of column names to fetch.
            - `start`: optional start value.
            - `finish`: optional finish value.
            - `reversed`: boolean, returns values in ascending/descending order.
            - `limit`: default=100, limits the number of records returned.

        :Returns:
          Returns a dict of key/value pairs.
          value is in the form of {'value': value, 'timestamp': timestamp}.
        """
        sliceRange = SliceRange(start=start, finish=finish, count=limit,
            reversed=reversed)

        if key and isinstance(key, list):
            predicate = SlicePredicate(key)
        else:
            predicate = SlicePredicate(slice_range=sliceRange)

        parentColumn = ColumnParent(column_family=self._columnFamily,
            super_column=self._superColumn)
        columns = self._client.get_slice(self._keySpace, self._columnKey,
            parentColumn, predicate, ConsistencyLevel.ONE)

        _results = {}

        for column in columns:
            if not column.column:
                column = column.super_column
                _results[column.name] = {}
                for subColumn in column.columns:
                    _results[column.name][subColumn.name] = \
                            self._formatColumnValue(subColumn)
            else:
                column = column.column
                _results.update({column.name:self._formatColumnValue(column)})
        return _results

    def count(self):
        """Returns the number of columns for a given key.

        :Returns:
            Returns the total number of columns.
        """
        key=self._columnKey
        parentColumn = ColumnParent(column_family=self._columnFamily,
            super_column=self._superColumn)
        return self._client.get_count(self._keySpace, key, parentColumn,
            ConsistencyLevel.ONE)
        
    def insert(self, key, value):
        """Inserts a new column.

        :Parameters:
            - `key`:  columnn key.
            - `value`: column value.
        """
        _columnPath = ColumnPath(column_family=self._columnFamily,
            column=key, super_column=self._superColumn)
        self._client.insert(self._keySpace, self._columnKey, _columnPath, value,
            time.time(), ConsistencyLevel.ONE)
