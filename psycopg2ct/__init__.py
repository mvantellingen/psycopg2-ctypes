import datetime
from time import localtime

from psycopg2ct import extensions
from psycopg2ct.extensions import Binary
from psycopg2ct.extensions import BINARY, DATETIME, NUMBER, ROWID, STRING
from psycopg2ct._impl.connection import connect
from psycopg2ct._impl.exceptions import *
from psycopg2ct.tz import LOCAL as _LOCAL

from psycopg2ct import tz

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
