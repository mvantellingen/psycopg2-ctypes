import datetime
from time import localtime

from psycopg2ct import extensions
from psycopg2ct import tz
from psycopg2ct._impl.adapters import Binary, Date, Time, Timestamp
from psycopg2ct._impl.adapters import DateFromTicks, TimeFromTicks
from psycopg2ct._impl.adapters import TimestampFromTicks
from psycopg2ct._impl.connection import _connect
from psycopg2ct._impl.exceptions import *
from psycopg2ct._impl.typecasts import BINARY, DATETIME, NUMBER, ROWID, STRING

__version__ = '2.4'
apilevel = '2.0'
paramstyle = 'pyformat'
threadsafety = 2

import psycopg2ct.extensions as _ext
_ext.register_adapter(tuple, _ext.SQL_IN)
_ext.register_adapter(type(None), _ext.NoneAdapter)


import re

def _param_escape(s,
        re_escape=re.compile(r"([\\'])"),
        re_space=re.compile(r'\s')):
    """
    Apply the escaping rule required by PQconnectdb
    """
    if not s: return "''"

    s = re_escape.sub(r'\\\1', s)
    if re_space.search(s):
        s = "'" + s + "'"

    return s

del re


def connect(dsn=None,
        database=None, user=None, password=None, host=None, port=None,
        connection_factory=None, async=False, **kwargs):
    """
    Create a new database connection.

    The connection parameters can be specified either as a string:

        conn = psycopg2.connect("dbname=test user=postgres password=secret")

    or using a set of keyword arguments:

        conn = psycopg2.connect(database="test", user="postgres", password="secret")

    The basic connection parameters are:

    - *dbname*: the database name (only in dsn string)
    - *database*: the database name (only as keyword argument)
    - *user*: user name used to authenticate
    - *password*: password used to authenticate
    - *host*: database host address (defaults to UNIX socket if not provided)
    - *port*: connection port number (defaults to 5432 if not provided)

    Using the *connection_factory* parameter a different class or connections
    factory can be specified. It should be a callable object taking a dsn
    argument.

    Using *async*=True an asynchronous connection will be created.

    Any other keyword parameter will be passed to the underlying client
    library: the list of supported parameter depends on the library version.

    """
    if dsn is None:
        # Note: reproducing the behaviour of the previous C implementation:
        # keyword are silently swallowed if a DSN is specified. I would have
        # raised an exception. File under "histerical raisins".
        items = []
        if database is not None:
            items.append(('dbname', database))
        if user is not None:
            items.append(('user', user))
        if password is not None:
            items.append(('password', password))
        if host is not None:
            items.append(('host', host))
        # Reproducing the previous C implementation behaviour: swallow a
        # negative port. The libpq would raise an exception for it.
        if port is not None and int(port) > 0:
            items.append(('port', port))

        items.extend(
            [(k, v) for (k, v) in kwargs.iteritems() if v is not None])
        dsn = " ".join(["%s=%s" % (k, _param_escape(str(v)))
            for (k, v) in items])

        if not dsn:
            raise InterfaceError('missing dsn and no parameters')

    return _connect(dsn,
        connection_factory=connection_factory, async=async)


__all__ = filter(lambda k: not k.startswith('_'), locals().keys())
