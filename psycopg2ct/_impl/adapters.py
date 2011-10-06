import datetime
import decimal
import math

from psycopg2ct._impl import libpq
from psycopg2ct._impl.encodings import encodings
from psycopg2ct._impl.exceptions import ProgrammingError
from psycopg2ct._config import PG_VERSION


adapters = {}


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


class Int(_BaseAdapter):
    def getquoted(self):
        value = str(self._wrapped)

        # Prepend a space in front of negative numbers
        if value.startswith('-'):
            value = ' ' + value
        return value


class Long(_BaseAdapter):
    def getquoted(self):
        value = str(self._wrapped)

        # Prepend a space in front of negative numbers
        if value.startswith('-'):
            value = ' ' + value
        return value


class Float(ISQLQuote):
    def getquoted(self):
        n = float(self._wrapped)
        if math.isnan(n):
            return "'NaN'::float"
        elif math.isinf(n):
            if n > 0:
                return "'Infinity'::float"
            else:
                return "'-Infinity'::float"
        else:
            value = repr(self._wrapped)

            # Prepend a space in front of negative numbers
            if value.startswith('-'):
                value = ' ' + value
            return value


class Decimal(_BaseAdapter):
    def getquoted(self):
        if self._wrapped.is_finite():
            value = str(self._wrapped)

            # Prepend a space in front of negative numbers
            if value.startswith('-'):
                value = ' ' + value
            return value
        return "'NaN'::numeric"


class Boolean(_BaseAdapter):
    def getquoted(self):
        return 'true' if self._wrapped else 'false'


class Binary(_BaseAdapter):
    def prepare(self, connection):
        self._conn = connection

    def __conform__(self, proto):
        return self

    def getquoted(self):
        if self._wrapped is None:
            return 'NULL'

        to_length = libpq.c_uint()

        if self._conn:
            data_pointer = libpq.PQescapeByteaConn(
                self._conn._pgconn, str(self._wrapped), len(self._wrapped),
                libpq.pointer(to_length))
        else:
            data_pointer = libpq.PQescapeBytea(
                self._wrapped, len(self._wrapped), libpq.pointer(to_length))

        data = data_pointer[:to_length.value - 1]
        libpq.PQfreemem(data_pointer)
        return r"'%s'::bytea" % data


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
            return "'%s'" % to.value

        if PG_VERSION < 0x090000:
            to = libpq.create_string_buffer('\0', (length * 2) + 1)
            err = libpq.c_int()
            libpq.PQescapeStringConn(
                self._conn._pgconn, to, string, length, err)
            return "'%s'" % to.value

        data_pointer = libpq.PQescapeLiteral(
            self._conn._pgconn, string, length)
        data = libpq.cast(data_pointer, libpq.c_char_p).value
        libpq.PQfreemem(data_pointer)
        return data


def adapt(value, proto=ISQLQuote, alt=None):
    """Return the adapter for the given value"""
    obj_type = type(value)
    try:
        return adapters[(obj_type, proto)](value)
    except KeyError:
        for subtype in obj_type.mro()[1:]:
            try:
                return adapters[(subtype, proto)](value)
            except KeyError:
                pass

    conform = getattr(value, '__conform__', None)
    if conform is not None:
        return conform(proto)
    raise ProgrammingError("can't adapt type '%s'", obj_type)


def _getquoted(param, conn):
    """Helper method"""
    adapter = adapt(param)
    try:
        adapter.prepare(conn)
    except AttributeError:
        pass
    return adapter.getquoted()



built_in_adapters = {
    bool: Boolean,
    str: QuotedString,
    unicode: QuotedString,
    list: List,
    bytearray: Binary,
    buffer: Binary,
    int: Int,
    long: Long,
    float: Float,
    datetime.date: DateTime, # DateFromPY
    datetime.datetime: DateTime, # TimestampFromPy
    datetime.time: DateTime, # TimeFromPy
    datetime.timedelta: DateTime, # IntervalFromPy
    decimal.Decimal: Decimal,
}

try:
    built_in_adapters[memoryview] = Binary
except NameError:
    # Python 2.6
    pass

for k, v in built_in_adapters.iteritems():
    adapters[(k, ISQLQuote)] = v
