import datetime
import math

from psycopg2 import libpq
from psycopg2.exceptions import ProgrammingError

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


class NoneAdapter(_BaseAdapter):
    def prepare(self, conn):
        pass

    def getquoted(self):
        return 'NULL'


class QuotedString(_BaseAdapter):
    def __init__(self, obj):
        super(QuotedString, self).__init__(obj)
        self.encoding = "latin-1"

    def prepare(self, conn):
        self._conn = conn
        self.encoding = conn.encoding

    def getquoted(self):
        from psycopg2.extensions import types

        obj = self._wrapped
        if isinstance(self._wrapped, unicode):
            encoding = types.encodings[self.encoding]
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
