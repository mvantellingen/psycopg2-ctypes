"""NOT_RPYTHON"""
# THIS FILE IS COPIED, FROM psycopg2.
# TODO: figure out what are my legal obligations?

import datetime
import time


ZERO = datetime.timedelta(0)

class FixedOffsetTimezone(datetime.tzinfo):
    """Fixed offset in minutes east from UTC.

    This is exactly the implementation__ found in Python 2.3.x documentation,
    with a small change to the `!__init__()` method to allow for pickling
    and a default name in the form ``sHH:MM`` (``s`` is the sign.).

    .. __: http://docs.python.org/library/datetime.html#datetime-tzinfo
    """
    _name = None
    _offset = ZERO

    def __init__(self, offset=None, name=None):
        if offset is not None:
            self._offset = datetime.timedelta(minutes = offset)
        if name is not None:
            self._name = name

    def __repr__(self):
        return "psycopg2.tz.FixedOffsetTimezone(offset=%r, name=%r)" \
            % (self._offset.seconds // 60, self._name)

    def utcoffset(self, dt):
        return self._offset

    def tzname(self, dt):
        if self._name is not None:
            return self._name
        else:
            seconds = self._offset.seconds + self._offset.days * 86400
            hours, seconds = divmod(seconds, 3600)
            minutes = seconds / 60
            if minutes:
                return "%+03d:%d" % (hours, minutes)
            else:
                return "%+03d" % hours

    def dst(self, dt):
        return ZERO

