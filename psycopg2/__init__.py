import datetime
from time import localtime

from psycopg2.exceptions import *
from psycopg2 import extensions
from psycopg2.connection import connect
from psycopg2.extensions import Binary, STRING


__version__ = '2.4'
apilevel = '2.0'
paramstyle = 'pyformat'


def Date(year, month, day):
    date = datetime.date(year, month, day)
    return extensions.DateTime(date)


def DateFromTicks(ticks):
    """FIXME: ?"""
    tm = localtime()
    return Date(tm.tm_year, tm.tm_mon, tm.tm_mday)


