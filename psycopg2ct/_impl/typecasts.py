import datetime
import decimal
import math
from time import localtime

from psycopg2ct._impl import libpq


string_types = {}


def typecast(caster, value, length, cursor):
    return caster.cast(value, length, cursor)


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
        from psycopg2ct.extensions import typecast

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
                start = i

                # Number of quotes, this will always be 0 or 2 (int vs str)
                quotes = 0

                # Whether or not the next char should be escaped
                escape_char = False

                while i < value_length:
                    if s[i] == '"':
                        if not escape_char:
                            quotes += 1
                    elif s[i] == '\\':
                        escape_char = not escape_char
                    elif s[i] == '}' or s[i] == ',':
                        if not escape_char and quotes % 2 == 0:
                            break
                    i += 1

                if quotes:
                    start += 1
                    end = i - 1
                else:
                    end = i

                str_buf = s[start:end].replace(r'\\', '\\')
                val = typecast(self._caster, str_buf, end - start, cursor)
                array.append(val)
        return stack[-1]


def parse_unicode(value, length, cursor):
    from psycopg2ct.extensions import encodings
    encoding = encodings[cursor.connection.encoding]
    return value.decode(encoding)


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
