import datetime
import math

from psycopg2 import libpq
from psycopg2.exceptions import ProgrammingError

adapters = {}

def register_adapter(cls, adapter):
    adapters[cls] = adapter


def adapt(value):
    adapted = None
    try:
        adapter = adapters[type(value)]
    except KeyError:
        superclass_adapter = get_superclass_adapter(value)
        if superclass_adapter is not None:
            adapted = superclass_adapter(value)
        else:
            conform = getattr(value, '__conform__', None)
            if conform:
                adapted = conform()
            if adapted is None:
                raise ProgrammingError("can't adapt type '%s'", type(value))
    else:
        adapted = adapter(value)
    return adapted


def get_superclass_adapter(value):
    obj_type = type(value)
    for subtype in obj_type.mro()[1:]:
        try:
            return adapters[subtype]
        except KeyError:
            pass
    return None


def quote(value, connection):
    if value is None:
        return 'NULL'

    # TODO: there really should be some proto object, not sure what it is.
    adapted = adapt(value)

    prepare = getattr(adapted, 'prepare', None)
    if prepare:
        prepare(connection)
    return adapted.getquoted()


class BaseAdapter(object):
    typedef = None

    def __init__(self, obj):
        self.obj = obj
        self.buffer = None
        self.connection = None

    def prepare(self, connection):
        self.connection = connection

    def quote(self):
        raise NotImplementedError

    def __conform__(self):
        return self

    def getquoted(self):
        if self.buffer is None:
            self.buffer = self.quote()
        return self.buffer

    def __str__(self):
        return self.getquoted()


class QuotedString(BaseAdapter):
    def __init__(self, obj):
        BaseAdapter.__init__(self, obj)
        self.encoding = "latin-1"

    def prepare(self, connection):
        super(QuotedString, self).prepare(connection)
        if isinstance(self.obj, unicode):
            self.encoding = self.connection.encoding

    def quote(self):
        from psycopg2 import types

        obj = self.obj
        if isinstance(obj, unicode):
            encoding = types.encodings[self.encoding]
            obj = obj.encode(encoding)
        string = str(obj)
        length = len(string)

        to = libpq.create_string_buffer('\000' * (length * 2))
        err = libpq.c_int()

        if self.connection is not None:
            libpq.PQescapeStringConn(
                self.connection.pgconn, to, string, length, err)
        else:
            libpq.PQescapeString(to, string, length)
        return "E'%s'" % to.value


class AsIs(BaseAdapter):
    def quote(self):
        return str(self.obj)


class Float(BaseAdapter):
    def quote(self):
        n = float(self.obj)
        if math.isnan(n):
            return "'NaN'::float"
        elif math.isinf(n):
            return "'Infinity'::float"
        else:
            return repr(self.obj)


class Decimal(BaseAdapter):
    def quote(self):
        if self.obj.is_finite():
            return str(self.obj)
        return "'NaN'::numeric"


class Boolean(BaseAdapter):
    def quote(self):
        return 'true' if self.obj else 'false'


class Binary(BaseAdapter):

    def quote(self):
        to_length = libpq.c_uint()

        if self.connection is None:
            raw_escaped = libpq.PQescapeBytea(self.obj, len(self.obj),
                libpq.pointer(to_length))

        else:
            raw_escaped = libpq.PQescapeByteaConn(self.connection.pgconn,
                str(self.obj), len(self.obj), libpq.pointer(to_length))

        escaped = raw_escaped[:to_length.value - 1]
        libpq.PQfreemem(raw_escaped)
        res = "'%s'::bytea" % escaped
        if self.connection is not None:
            res = "E" + res
        return res


class List(BaseAdapter):
    def quote(self):
        length = len(self.obj)
        if length == 0:
            return "'{}'"

        quoted = [None] * length
        for i in xrange(length):
            obj = self.obj[i]
            quoted[i] = str(quote(obj, self.connection))
        return "ARRAY[%s]" % ", ".join(quoted)


class DateTime(BaseAdapter):
    def quote(self):
        obj = self.obj
        if isinstance(obj, datetime.timedelta):
            # TODO: microseconds
            return "'%d days %d.0 seconds'::interval" % (
                int(obj.days), int(obj.seconds))
        else:
            iso = obj.isoformat()
            if isinstance(obj, datetime.datetime):
                format = 'timestamp'
                if not getattr(obj, 'tzinfo', None):
                    format = 'timestamptz'
            elif isinstance(obj, datetime.time):
                format = 'time'
            else:
                format = 'date'
            return "'%s'::%s" % (str(iso), format)

AVAILABLE_ADAPTERS = [
    ('QuotedString', QuotedString)
]
