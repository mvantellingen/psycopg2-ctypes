import datetime
import decimal
import math

from psycopg2ct._config import PG_VERSION
from psycopg2ct._impl import libpq
from psycopg2ct._impl import typecasts
from psycopg2ct._impl.adapters import adapt, adapters
from psycopg2ct._impl.connection import Connection as connection
from psycopg2ct._impl.consts import *
from psycopg2ct._impl.cursor import Cursor as cursor
from psycopg2ct._impl.encodings import encodings
from psycopg2ct._impl.exceptions import ProgrammingError
from psycopg2ct._impl.exceptions import QueryCanceledError
from psycopg2ct._impl.xid import Xid


from psycopg2ct._impl.adapters import Binary, Boolean, Int, Float
from psycopg2ct._impl.adapters import QuotedString, AsIs, ISQLQuote


from psycopg2ct._impl.connection import Notify

# The following are not available in psycopg2.extensions so figure that out.
from psycopg2ct._impl.adapters import List, DateTime, Decimal

import sys as _sys

# Return bytes from a string
if _sys.version_info[0] < 3:
    def b(s):
        return s
else:
    def b(s):
        return s.encode('utf8')

string_types = typecasts.string_types



def register_adapter(typ, callable):
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


class Type(object):
    def __init__(self, name, values, caster=None, py_caster=None):
        self.name = name
        self.values = values
        self.caster = caster
        self.py_caster = py_caster

    def __eq__(self, other):
        return other in self.values

    def cast(self, value, length, cursor):
        if self.py_caster is not None:
            return self.py_caster(value, cursor)
        return self.caster(value, length, cursor)


def register_type(type_obj, scope=None):
    typecasts = string_types
    if scope:
        from psycopg2ct._impl.connection import Connection
        from psycopg2ct._impl.cursor import Cursor

        if isinstance(scope, Connection):
            typecasts = scope._typecasts
        elif isinstance(scope, Cursor):
            typecasts = scope._typecasts
        else:
            typecasts = None

    for value in type_obj.values:
        typecasts[value] = type_obj


def new_type(values, name, castobj):
    return Type(name, values, py_caster=castobj)


def new_array_type(values, name, baseobj):
    caster = typecasts.parse_array(baseobj)
    return Type(name, values, caster=caster)


def _default_type(name, oids, caster):
    """Shortcut to register internal types"""
    type_obj = Type(name, oids, caster)
    register_type(type_obj)
    return type_obj

# DB API 2.0 types
BINARY = _default_type('BINARY', [17], typecasts.parse_binary)
DATETIME = _default_type('DATETIME',  [1114, 1184, 704, 1186], typecasts.parse_datetime)
NUMBER = _default_type('NUMBER', [20, 33, 21, 701, 700, 1700], typecasts.parse_float)
ROWID = _default_type('ROWID', [26], typecasts.parse_integer)
STRING = _default_type('STRING', [19, 18, 25, 1042, 1043], typecasts.parse_string)

# Register the basic typecasters
UNKNOWN = _default_type('UNKNOWN', [705], typecasts.parse_unknown)
BOOLEAN = _default_type('BOOLEAN', [16], typecasts.parse_boolean)
DATE = _default_type('DATE', [1082], typecasts.parse_date)
DECIMAL = _default_type('DECIMAL', [1700], typecasts.parse_decimal)
FLOAT = _default_type('FLOAT', [701, 700], typecasts.parse_float)
INTEGER = _default_type('INTEGER', [23, 21], typecasts.parse_integer)
INTERVAL = _default_type('INTERVAL', [704, 1186], typecasts.parse_interval)
LONGINTEGER = _default_type('LONGINTEGER', [20], typecasts.parse_longinteger)
TIME = _default_type('TIME', [1083, 1266], typecasts.parse_time)
UNICODE = _default_type('UNICODE', [19, 18, 25, 1042, 1043], typecasts.parse_unicode)

# Array types
INTEGERARRAY = _default_type(
    'INTEGERARRAY', [1005, 1006, 1007], typecasts.parse_array(INTEGER))
FLOATARRAY = _default_type(
    'FLOATARRAY', [1017, 1021, 1022], typecasts.parse_array(FLOAT))
DECIMALARRAY = _default_type(
    'DECIMALARRAY', [1231], typecasts.parse_array(DECIMAL))
STRINGARRAY = _default_type(
    'STRINGARRAY', [1002, 1003, 1009, 1014, 1015], typecasts.parse_array(STRING))
BINARYARRAY = _default_type(
    'BINARYARRAY', [1001], typecasts.parse_array(BINARY))
DATETIMEARRAY = _default_type(
    'DATETIMEARRAY', [1115, 1185], typecasts.parse_array(DATETIME))
