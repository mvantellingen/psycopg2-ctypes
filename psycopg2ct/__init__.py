import datetime
from time import localtime

from psycopg2ct import extensions
from psycopg2ct.extensions import Binary
from psycopg2ct.extensions import BINARY, DATETIME, NUMBER, ROWID, STRING
from psycopg2ct._impl.connection import connect
from psycopg2ct._impl.exceptions import *
from psycopg2ct.tz import LOCAL as _LOCAL

__version__ = '2.4'
apilevel = '2.0'
paramstyle = 'pyformat'

# TODO: psycopg2 thread safety is 2. I haven't reviewed it in the -ct
# but the lack of the word "lock" in the cursor module makes me assume
# it's not  -- piro
threadsafety = 1


def Date(year, month, day):
    date = datetime.date(year, month, day)
    return extensions.DateTime(date)

def Time(hour, minutes, seconds, tzinfo=None):
    time = datetime.time(hour, minutes, seconds, tzinfo=tzinfo)
    return extensions.DateTime(time)

def Timestamp(year, month, day, hour, minutes, seconds, tzinfo=None):
    dt = datetime.datetime(
        year, month, day, hour, minutes, seconds, tzinfo=tzinfo)
    return extensions.DateTime(dt)

def DateFromTicks(ticks):
    date = datetime.datetime.fromtimestamp(ticks).date()
    return extensions.DateTime(date)

def TimeFromTicks(ticks):
    time = datetime.datetime.fromtimestamp(ticks).time()
    return extensions.DateTime(time)

def TimestampFromTicks(ticks):
    dt = datetime.datetime.fromtimestamp(ticks, _LOCAL)
    return extensions.DateTime(dt)

