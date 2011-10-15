import datetime
from time import localtime

from psycopg2ct import extensions
from psycopg2ct import tz
from psycopg2ct._impl.adapters import Binary, Date, Time, Timestamp
from psycopg2ct._impl.adapters import DateFromTicks, TimeFromTicks
from psycopg2ct._impl.adapters import TimestampFromTicks
from psycopg2ct._impl.connection import connect
from psycopg2ct._impl.exceptions import *
from psycopg2ct._impl.typecasts import BINARY, DATETIME, NUMBER, ROWID, STRING

__version__ = '2.4'
apilevel = '2.0'
paramstyle = 'pyformat'

# TODO: psycopg2 thread safety is 2. I haven't reviewed it in the -ct
# but the lack of the word "lock" in the cursor module makes me assume
# it's not  -- piro
threadsafety = 1


import psycopg2ct.extensions as _ext
_ext.register_adapter(tuple, _ext.SQL_IN)
_ext.register_adapter(type(None), _ext.NoneAdapter)

__all__ = filter(lambda k: not k.startswith('_'), locals().keys())
