"""psycopg extensions to the DBAPI-2.0

This module holds all the extensions to the DBAPI-2.0 provided by psycopg.

- `connection` -- the new-type inheritable connection class
- `cursor` -- the new-type inheritable cursor class
- `lobject` -- the new-type inheritable large object class
- `adapt()` -- exposes the PEP-246_ compatible adapting mechanism used
  by psycopg to adapt Python types to PostgreSQL ones

.. _PEP-246: http://www.python.org/peps/pep-0246.html
"""
import sys as _sys

from psycopg2ct._impl.adapters import adapt, adapters
from psycopg2ct._impl.adapters import Binary, Boolean, Int, Float
from psycopg2ct._impl.adapters import QuotedString, AsIs, ISQLQuote
from psycopg2ct._impl.connection import Connection as connection
from psycopg2ct._impl.consts import *
from psycopg2ct._impl.cursor import Cursor as cursor
from psycopg2ct._impl.encodings import encodings
from psycopg2ct._impl.exceptions import QueryCanceledError
from psycopg2ct._impl.exceptions import TransactionRollbackError
from psycopg2ct._impl.notify import Notify
from psycopg2ct._impl.typecasts import (
    UNICODE, INTEGER, LONGINTEGER, BOOLEAN, FLOAT, TIME, DATE, INTERVAL,
    DECIMAL,
    BINARYARRAY, BOOLEANARRAY, DATEARRAY, DATETIMEARRAY, DECIMALARRAY,
    FLOATARRAY, INTEGERARRAY, INTERVALARRAY, LONGINTEGERARRAY, ROWIDARRAY,
    STRINGARRAY, TIMEARRAY, UNICODEARRAY)
from psycopg2ct._impl.typecasts import string_types, binary_types
from psycopg2ct._impl.typecasts import new_type, new_array_type, register_type
from psycopg2ct._impl.xid import Xid


# Return bytes from a string
if _sys.version_info[0] < 3:
    def b(s):
        return s
else:
    def b(s):
        return s.encode('utf8')


def register_adapter(typ, callable):
    """Register 'callable' as an ISQLQuote adapter for type 'typ'."""
    adapters[(typ, ISQLQuote)] = callable


# The SQL_IN class is the official adapter for tuples starting from 2.0.6.
class SQL_IN(object):
    """Adapt any iterable to an SQL quotable object."""

    def __init__(self, seq):
        self._seq = seq

    def prepare(self, conn):
        self._conn = conn

    def getquoted(self):
        # this is the important line: note how every object in the
        # list is adapted and then how getquoted() is called on it
        pobjs = [adapt(o) for o in self._seq]
        for obj in pobjs:
            if hasattr(obj, 'prepare'):
                obj.prepare(self._conn)
        qobjs = [o.getquoted() for o in pobjs]
        return b('(') + b(', ').join(qobjs) + b(')')

    def __str__(self):
        return str(self.getquoted())


class NoneAdapter(object):
    """Adapt None to NULL.

    This adapter is not used normally as a fast path in mogrify uses NULL,
    but it makes easier to adapt composite types.
    """
    def __init__(self, obj):
        pass

    def getquoted(self, _null=b("NULL")):
        return _null


__all__ = filter(lambda k: not k.startswith('_'), locals().keys())
