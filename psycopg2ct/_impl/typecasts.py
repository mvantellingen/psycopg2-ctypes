import datetime
import decimal
import math
from time import localtime

from psycopg2ct._impl import libpq


string_types = {}

binary_types = {}


class Type(object):
    def __init__(self, name, values, caster=None, py_caster=None):
        self.name = name
        self.values = values
        self.caster = caster
        self.py_caster = py_caster

    def __eq__(self, other):
        return other in self.values

    def cast(self, value, cursor, length=None):
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
    caster = parse_array(baseobj)
    return Type(name, values, caster=caster)


def typecast(caster, value, length, cursor):
    return caster.cast(value, cursor, length)


def parse_unknown(value, length, cursor):
    if value != '{}':
        return value
    else:
        return []


def parse_string(value, length, cursor):
    return value


def parse_longinteger(value, length, cursor):
    return long(value)


def parse_integer(value, length, cursor):
    return int(value)


def parse_float(value, length, cursor):
    return float(value)


def parse_decimal(value, length, cursor):
    return decimal.Decimal(value)


def parse_binary(value, length, cursor):
    to_length = libpq.c_uint()
    s = libpq.PQunescapeBytea(value, libpq.pointer(to_length))
    try:
        res = buffer(s[:to_length.value])
    finally:
        libpq.PQfreemem(s)
    return res


def parse_boolean(value, length, cursor):
    return value[0] == "t"


class parse_array(object):
    def __init__(self, caster):
        self._caster = caster

    def cast(self, value, length, cursor):
        return self(value, length, cursor)

    def __call__(self, value, length, cursor):
        s = value
        assert s[0] == "{" and s[-1] == "}"
        i = 1
        array = []
        stack = [array]
        value_length = len(s) - 1
        while i < value_length:
            if s[i] == '{':
                sub_array = []
                array.append(sub_array)
                stack.append(sub_array)
                array = sub_array
                i += 1
            elif s[i] == '}':
                stack.pop()
                array = stack[-1]
                i += 1
            elif s[i] in ', ':
                i += 1
            else:
                # Number of quotes, this will always be 0 or 2 (int vs str)
                quotes = 0

                # Whether or not the next char should be escaped
                escape_char = False

                buf = []
                while i < value_length:
                    if not escape_char:
                        if s[i] == '"':
                            quotes += 1
                        elif s[i] == '\\':
                            escape_char = True
                        elif quotes % 2 == 0 and (s[i] == '}' or s[i] == ','):
                            break
                        else:
                            buf.append(s[i])
                    else:
                        escape_char = False
                        buf.append(s[i])

                    i += 1

                str_buf = ''.join(buf)
                if len(str_buf) == 4 and str_buf.lower() == 'null':
                    val = typecast(self._caster, None, 0, cursor)
                else:
                    val = typecast(self._caster, str_buf, len(str_buf), cursor)
                array.append(val)
        return stack[-1]


def parse_unicode(value, length, cursor):
    return value.decode(cursor._connection._py_enc)


def _parse_date(value):
    return datetime.date(*map(int, value.split('-')))


def _parse_time(time, cursor):
    microsecond = 0
    hour, minute, second = time.split(":", 2)

    tzinfo = None
    sign = 0
    timezone = None
    if "-" in second:
        sign = -1
        second, timezone = second.split("-")
    elif "+" in second:
        sign = 1
        second, timezone = second.split("+")
    if not cursor.tzinfo_factory is None and sign:
        parts = timezone.split(":")
        tz_min = sign * 60 * int(parts[0])
        if len(parts) > 1:
            tz_min += int(parts[1])
        if len(parts) > 2:
            tz_min += int(int(parts[2]) / 60.)
        tzinfo = cursor.tzinfo_factory(tz_min)
    if "." in second:
        second, microsecond = second.split(".")
        microsecond = int(microsecond) * int(math.pow(10.0, 6.0 - len(microsecond)))

    return datetime.time(int(hour), int(minute), int(second), microsecond,
        tzinfo)


def parse_datetime(value, length, cursor):
    date, time = value.split(' ')
    date = _parse_date(date)
    time = _parse_time(time, cursor)
    return datetime.datetime.combine(date, time)


def parse_date(value, length, cursor):
    return _parse_date(value)


def parse_time(value, length, cursor):
    return _parse_time(value, cursor)


def parse_interval(value, length, cursor):
    years = months = days = 0
    hours = minutes = seconds = hundreths = 0.0
    v = 0.0
    sign = 1
    denominator = 1.0
    part = 0
    skip_to_space = False

    s = value
    for c in s:
        if skip_to_space:
            if c == " ":
                skip_to_space = False
            continue
        if c == "-":
            sign = -1
        elif "0" <= c <= "9":
            v = v * 10 + ord(c) - ord("0")
            if part == 6:
                denominator *= 10
        elif c == "y":
            if part == 0:
                years = int(v * sign)
                skip_to_space = True
                v = 0.0
                sign = 1
                part = 1
        elif c == "m":
            if part <= 1:
                months = int(v * sign)
                skip_to_space = True
                v = 0.0
                sign = 1
                part = 2
        elif c == "d":
            if part <= 2:
                days = int(v * sign)
                skip_to_space = True
                v = 0.0
                sign = 1
                part = 3
        elif c == ":":
            if part <= 3:
                hours = v
                v = 0.0
                part = 4
            elif part == 4:
                minutes = v
                v = 0.0
                part = 5
        elif c == ".":
            if part == 5:
                seconds = v
                v = 0.0
                part = 6

    if part == 4:
        minutes = v
    elif part == 5:
        seconds = v
    elif part == 6:
        hundreths = v / denominator

    if sign < 0.0:
        seconds = - (hundreths + seconds + minutes * 60 + hours * 3600)
    else:
        seconds += hundreths + minutes * 60 + hours * 3600

    days += years * 365 + months * 30
    micro = (seconds - math.floor(seconds)) * 1000000.0
    seconds = int(math.floor(seconds))
    return datetime.timedelta(days, seconds, int(micro))



def Date(year, month, day):
    from psycopg2ct.extensions.adapters import DateTime
    date = datetime.date(year, month, day)
    return DateTime(date)


def DateFromTicks(ticks):
    tm = localtime()
    return Date(tm.tm_year, tm.tm_mon, tm.tm_mday)


def Binary(obj):
    from psycopg2ct.extensions.adapters import Binary
    return Binary(obj)


def _default_type(name, oids, caster):
    """Shortcut to register internal types"""
    type_obj = Type(name, oids, caster)
    register_type(type_obj)
    return type_obj


# DB API 2.0 types
BINARY = _default_type('BINARY', [17], parse_binary)
DATETIME = _default_type('DATETIME',  [1114, 1184, 704, 1186], parse_datetime)
NUMBER = _default_type('NUMBER', [20, 33, 21, 701, 700, 1700], parse_float)
ROWID = _default_type('ROWID', [26], parse_integer)
STRING = _default_type('STRING', [19, 18, 25, 1042, 1043], parse_string)

# Register the basic typecasters
BOOLEAN = _default_type('BOOLEAN', [16], parse_boolean)
DATE = _default_type('DATE', [1082], parse_date)
DECIMAL = _default_type('DECIMAL', [1700], parse_decimal)
FLOAT = _default_type('FLOAT', [701, 700], parse_float)
INTEGER = _default_type('INTEGER', [23, 21], parse_integer)
INTERVAL = _default_type('INTERVAL', [704, 1186], parse_interval)
LONGINTEGER = _default_type('LONGINTEGER', [20], parse_longinteger)
TIME = _default_type('TIME', [1083, 1266], parse_time)
UNKNOWN = _default_type('UNKNOWN', [705], parse_unknown)

# Array types
BINARYARRAY = _default_type(
    'BINARYARRAY', [1001], parse_array(BINARY))
BOOLEANARRAY = _default_type(
    'BOOLEANARRAY', [1000], parse_array(BOOLEAN))
DATEARRAY = _default_type(
    'DATEARRAY', [1182], parse_array(DATE))
DATETIMEARRAY = _default_type(
    'DATETIMEARRAY', [1115, 1185], parse_array(DATETIME))
DECIMALARRAY = _default_type(
    'DECIMALARRAY', [1231], parse_array(DECIMAL))
FLOATARRAY = _default_type(
    'FLOATARRAY', [1017, 1021, 1022], parse_array(FLOAT))
INTEGERARRAY = _default_type(
    'INTEGERARRAY', [1005, 1006, 1007], parse_array(INTEGER))
INTERVALARRAY = _default_type(
    'INTERVALARRAY', [1187], parse_array(INTERVAL))
LONGINTEGERARRAY = _default_type(
    'LONGINTEGERARRAY', [1016], parse_array(LONGINTEGER))
ROWIDARRAY = _default_type(
    'ROWIDARRAY', [1013, 1028], parse_array(ROWID))
STRINGARRAY = _default_type(
    'STRINGARRAY', [1002, 1003, 1009, 1014, 1015], parse_array(STRING))
TIMEARRAY = _default_type(
    'TIMEARRAY', [1183, 1270], parse_array(TIME))


UNICODE = Type('UNICODE', [19, 18, 25, 1042, 1043], parse_unicode)
UNICODEARRAY = Type('UNICODEARRAY', [1002, 1003, 1009, 1014, 1015],
    parse_array(UNICODE))
