import datetime
import decimal
import math

from psycopg2ct import libpq
from psycopg2ct import typecasts
from psycopg2ct.exceptions import ProgrammingError

ISOLATION_LEVEL_AUTOCOMMIT = 0
ISOLATION_LEVEL_READ_COMMITTED = ISOLATION_LEVEL_READ_UNCOMMITTED = 1
ISOLATION_LEVEL_SERIALIZABLE = ISOLATION_LEVEL_REPEATABLE_READ = 2

TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4

adapters = {}

encodings = {
    'UNICODE': 'utf_8',
    'UTF8': 'utf_8',
    'LATIN1': 'ISO-8859-1',
    'LATIN2': 'ISO-8859-2',
    'LATIN3': 'ISO-8859-3',
    'LATIN4': 'ISO-8859-4',
    'LATIN5': 'ISO-8859-9',
    'LATIN6': 'ISO-8859-10',
    'LATIN7': 'ISO-8859-13',
    'LATIN8': 'ISO-8859-14',
    'LATIN9': 'ISO-8859-15',
    'LATIN10': 'ISO-8859-16'
}

string_types = {}


class _BaseAdapter(object):
    def __init__(self, wrapped_object):
        self._wrapped = wrapped_object
        self._conn = None

    def __str__(self):
        return self.getquoted()


class ISQLQuote(_BaseAdapter):
    def getquoted(self):
        pass


class AsIs(_BaseAdapter):
    def getquoted(self):
        return str(self._wrapped)


class Float(ISQLQuote):
    def getquoted(self):
        n = float(self._wrapped)
        if math.isnan(n):
            return "'NaN'::float"
        elif math.isinf(n):
            return "'Infinity'::float"
        else:
            return repr(self._wrapped)


class Decimal(_BaseAdapter):
    def getquoted(self):
        if self._wrapped.is_finite():
            return str(self._wrapped)
        return "'NaN'::numeric"


class Boolean(_BaseAdapter):
    def getquoted(self):
        return 'true' if self._wrapped else 'false'


class Binary(_BaseAdapter):
    def prepare(self, connection):
        self._conn = connection

    def __conform__(self):
        return self

    def getquoted(self):
        to_length = libpq.c_uint()

        if self._conn:
            data_pointer = libpq.PQescapeByteaConn(
                self._conn._pgconn, str(self._wrapped), len(self._wrapped),
                libpq.pointer(to_length))
            template = r"E'%s'::bytea"
        else:
            data_pointer = libpq.PQescapeBytea(
                self._wrapped, len(self._wrapped), libpq.pointer(to_length))
            template = r"'%s'::bytea"

        data = data_pointer[:to_length.value - 1]
        libpq.PQfreemem(data_pointer)
        return template % data


class List(_BaseAdapter):

    def prepare(self, connection):
        self._conn = connection

    def getquoted(self):
        length = len(self._wrapped)
        if length == 0:
            return "'{}'"

        quoted = [None] * length
        for i in xrange(length):
            obj = self._wrapped[i]
            quoted[i] = str(_getquoted(obj, self._conn))
        return "ARRAY[%s]" % ", ".join(quoted)


class DateTime(_BaseAdapter):
    def getquoted(self):
        obj = self._wrapped
        if isinstance(obj, datetime.timedelta):
            # TODO: microseconds
            return "'%d days %d.0 seconds'::interval" % (
                int(obj.days), int(obj.seconds))
        else:
            iso = obj.isoformat()
            if isinstance(obj, datetime.datetime):
                format = 'timestamp'
                if getattr(obj, 'tzinfo', None):
                    format = 'timestamptz'
            elif isinstance(obj, datetime.time):
                format = 'time'
            else:
                format = 'date'
            return "'%s'::%s" % (str(iso), format)


class QuotedString(_BaseAdapter):
    def __init__(self, obj):
        super(QuotedString, self).__init__(obj)
        self.encoding = "latin-1"

    def prepare(self, conn):
        self._conn = conn
        self.encoding = conn.encoding

    def getquoted(self):

        obj = self._wrapped
        if isinstance(self._wrapped, unicode):
            encoding = encodings[self.encoding]
            obj = obj.encode(encoding)
        string = str(obj)
        length = len(string)

        if not self._conn:
            to = libpq.create_string_buffer('\0', (length * 2) + 1)
            libpq.PQescapeString(to, string, length)
            return "E'%s'" % to.value

        data_pointer = libpq.PQescapeLiteral(
            self._conn._pgconn, string, length)
        data = libpq.cast(data_pointer, libpq.c_char_p).value
        libpq.PQfreemem(data_pointer)
        return data


class NoneAdapter(_BaseAdapter):
    def prepare(self, conn):
        pass

    def getquoted(self):
        return 'NULL'


class SQL_IN(_BaseAdapter):
    pass


def adapt(value):
    """Return the adapter for the given value"""
    obj_type = type(value)
    try:
        return adapters[obj_type](value)
    except KeyError:
        for subtype in obj_type.mro()[1:]:
            try:
                return adapters[subtype](value)
            except KeyError:
                pass

    conform = getattr(value, '__conform__', None)
    if conform is not None:
        return conform()
    raise ProgrammingError("can't adapt type '%s'", obj_type)


def register_adapter(typ, callable):
    adapters[typ] = callable


def _getquoted(param, conn):
    """Helper method"""
    adapter = adapt(param)
    try:
        adapter.prepare(conn)
    except AttributeError:
        pass
    return adapter.getquoted()


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
        from psycopg2ct.connection import Connection
        from psycopg2ct.cursor import Cursor

        if isinstance(scope, Connection):
            typecasts = scope._typecasts
        elif isinstance(scope, Cursor):
            typecasts = scope._typecasts
        else:
            typecasts = None

    for value in type_obj.values:
        typecasts[value] = type_obj


def new_type(oids, name, adapter):
    return Type(name, oids, py_caster=adapter)


def typecast(caster, value, length, cursor):
    return caster.cast(value, length, cursor)


# Register default adapters
register_adapter(type(None), NoneAdapter)
register_adapter(str, QuotedString)
register_adapter(unicode, QuotedString)
register_adapter(int, AsIs)
register_adapter(long, AsIs)
register_adapter(float, Float)
register_adapter(bool, Boolean)
register_adapter(buffer, Binary)
register_adapter(list, List)
register_adapter(datetime.datetime, DateTime)
register_adapter(datetime.date, DateTime)
register_adapter(datetime.time, DateTime)
register_adapter(datetime.timedelta, DateTime)
register_adapter(decimal.Decimal, Decimal)


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
