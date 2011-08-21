from functools import wraps

from psycopg2 import extensions, libpq, tz
from psycopg2.exceptions import InterfaceError, ProgrammingError
from psycopg2.extensions.adapters import quote


def check_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise InterfaceError("connection already closed")
        return func(self, *args, **kwargs)
    return wrapper

def check_no_tuples(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._no_tuples:
            raise ProgrammingError("no results to fetch")
        return func(self, *args, **kwargs)
    return wrapper


class Cursor(object):

    def __init__(self, connection, name):
        self._connection = connection
        self.arraysize = 1
        self.tzinfo_factory = tz.FixedOffsetTimezone

        self._caster = None
        self._closed = False
        self._description = None
        self._lastrowid = 0
        self._name = name
        self._no_tuples = True
        self._rowcount = -1
        self._rownumber = 0
        self._query = None
        self._statusmessage = None
        self._typecasts = {}
        self._pgres = None

    def __del__(self):
        if self._pgres:
            libpq.PQclear(self._pgres)
            self._pgres = None

    @property
    def closed(self):
        return self._closed or self._connection.closed

    @property
    def connection(self):
        return self._connection

    @property
    def description(self):
        return self._description

    @property
    def lastrowid(self):
        return self._lastrowid

    @property
    def name(self):
        return self._name

    @property
    def query(self):
        return self._query

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def rownumber(self):
        return self._rownumber

    @property
    def statusmessage(self):
        return self._statusmessage

    def close(self):
        self._closed = True

    @check_closed
    def execute(self, query, parameters=None):
        """Execute the given query after combining the query and parameters.

        """
        self._description = None
        conn = self._connection

        if isinstance(query, unicode):
            encoding = extensions.encodings[self._connection.encoding]
            query = query.encode(encoding)

        if parameters is not None:
            self._query = _combine_cmd_params(query, parameters, conn)
        else:
            self._query = query

        conn._begin_transaction()
        self._clear_pgres()

        self._pgres = libpq.PQexec(conn._pgconn, self._query)

        if not self._pgres:
            conn._raise_operational_error(self._pgres)

        pgstatus = libpq.PQresultStatus(self._pgres)
        self._statusmessage = libpq.PQcmdStatus(self._pgres)

        self._no_tuples = True
        self._rownumber = 0

        if pgstatus == libpq.PGRES_COMMAND_OK:
            rowcount = libpq.PQcmdTuples(self._pgres)
            if not rowcount or not rowcount[0]:
                self._rowcount = -1
            else:
                self._rowcount = int(rowcount)
            self._lastrowid = libpq.PQoidValue(self._pgres)
            self._clear_pgres()

        elif pgstatus == libpq.PGRES_TUPLES_OK:
            self._rowcount = libpq.PQntuples(self._pgres)
            self._no_tuples = False
            self._nfields = libpq.PQnfields(self._pgres)
            description = [None] * self._nfields
            casts = []
            for i in xrange(self._nfields):
                field_description = [None] * 7
                field_description[0] = libpq.PQfname(self._pgres, i)

                ftype = libpq.PQftype(self._pgres, i)
                field_description[1] = ftype
                try:
                    cast = self._typecasts[ftype]
                except KeyError:
                    try:
                        cast = self._connection._typecasts[ftype]
                    except KeyError:

                        try:
                            cast = extensions.string_types[ftype]
                        except KeyError:
                            cast = extensions.string_types[19]
                casts.append(cast)
                description[i] = tuple(field_description)

            self._description = tuple(description)
            self._casts = casts

        else:
            conn._raise_operational_error(self._pgres)

    @check_closed
    def executemany(self, query, paramlist):
        self._rowcount = -1
        rowcount = 0
        for params in paramlist:
            self.execute(query, params)
            if self.rowcount == -1:
                rowcount = -1
            else:
                rowcount += self.rowcount
        self._rowcount = rowcount

    @check_closed
    def callproc(self, procname, parameters=None):
        if parameters is None:
            length = 0
        else:
            length = len(parameters)
        sql = "SELECT * FROM %s(%s)" % (
            procname,
            ", ".join(["%s"] * length)
        )
        self.execute(sql, parameters)
        return parameters

    def mogrify(self, query, vars=None):
        return _combine_cmd_params(query, vars, self._connection)

    @check_closed
    @check_no_tuples
    def fetchone(self):
        if self._rownumber >= self._rowcount:
            return None

        row = self._build_row(self._rownumber)
        self._rownumber += 1
        return row

    @check_closed
    @check_no_tuples
    def fetchall(self):
        size = self._rowcount - self._rownumber
        if size <= 0:
            return []

        result = []
        for row in xrange(size):
            result.append(self._build_row(self._rownumber))
            self._rownumber += 1
        return result

    @check_closed
    @check_no_tuples
    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        if size > self._rowcount - self._rownumber or size < 0:
            size = self._rowcount - self._rownumber

        if size <= 0:
            return []

        rows = []
        for i in xrange(size):
            rows.append(self._build_row(self._rownumber))
            self._rownumber += 1
        return rows

    @check_closed
    def __iter__(self):
        return self

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration()
        return row

    def _clear_pgres(self):
        if self._pgres:
            libpq.PQclear(self._pgres)
            self._pgres = None

    def _build_row(self, row_num):
        n = self._nfields
        row = []
        for i in xrange(n):
            if libpq.PQgetisnull(self._pgres, row_num, i):
                val = None
            else:
                val = libpq.PQgetvalue(self._pgres, row_num, i)
                length = libpq.PQgetlength(self._pgres, row_num, i)
                val = extensions.typecast(self._casts[i], val, length, self)

            row.append(val)
        return tuple(row)


def _combine_cmd_params(cmd, params, conn):
    """Combine the command string and params"""
    idx = 0
    param_num = 0
    arg_values = None
    named_args_format = None

    def check_format_char(format_char, pos):
        """Raise an exception when the format_char is unsupported"""
        if format_char not in 's ':
            raise ValueError(
                "unsupported format character '%s' (0x%x) at index %d" %
                (format_char, ord(format_char), pos))


    while idx < len(cmd):

        # Escape
        if cmd[idx] == '%' and cmd[idx + 1] == '%':
            idx += 1

        # Named parameters
        elif cmd[idx] == '%' and cmd[idx + 1] == '(':

            # Validate that we don't mix formats
            if named_args_format is False:
                raise ValueError("argument formats can't be mixed")
            elif named_args_format is None:
                named_args_format = True

            # Check for incomplate placeholder
            max_lookahead = cmd.find('%', idx + 2)
            end = cmd.find(')', idx + 2, max_lookahead)
            if end < 0:
                raise ProgrammingError("incomplete placeholder: '%(' without ')'")

            key = cmd[idx + 2:end]
            if arg_values is None:
                arg_values = {}
            if key not in arg_values:
                arg_values[key] = quote(params[key], conn)

            check_format_char(cmd[end + 1], idx)

        # Indexed parameters
        elif cmd[idx] == '%':

            # Validate that we don't mix formats
            if named_args_format is True:
                raise ValueError("argument formats can't be mixed")
            elif named_args_format is None:
                named_args_format = False

            check_format_char(cmd[idx + 1], idx)

            if arg_values is None:
                arg_values = []

            value = quote(params[param_num], conn)
            arg_values.append(value)

            param_num += 1
            idx += 1

        idx += 1

    if named_args_format is False:
        if len(arg_values) != len(params):
            raise TypeError("not all arguments converted during string formatting")
        arg_values = tuple(arg_values)

    if not arg_values:
        return cmd % tuple()  # Required to unescape % chars
    return cmd % arg_values

