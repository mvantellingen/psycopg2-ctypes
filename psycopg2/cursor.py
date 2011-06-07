from functools import wraps

from psycopg2 import libpq, tz, extensions
from psycopg2.adapters import quote
from psycopg2.exceptions import OperationalError, InterfaceError, ProgrammingError


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
        if self.no_tuples:
            raise ProgrammingError("no results to fetch")
        return func(self, *args, **kwargs)
    return wrapper


class Cursor(object):

    def __init__(self, connection, name):
        self.connection = connection
        self.name = name
        self.arraysize = 1
        self.description = None
        self._closed = False
        self.row = 0
        self.rowcount = -1
        self.query = None
        self.pgstatus = None
        self.typecasts = {}
        self.tzinfo_factory = tz.FixedOffsetTimezone
        self.caster = None
        self.pgres = None
        self.lastrowid = 0
        self.no_tuples = True

    def __del__(self):
        if self.pgres:
            libpq.PQclear(self.pgres)
            self.pgres = None

    @property
    def closed(self):
        return self._closed or self.connection.closed

    @property
    def statusmessage(self):
        return self.pgstatus

    def clear_pgres(self):
        if self.pgres:
            libpq.PQclear(self.pgres)
            self.pgres = None

    def close(self):
        self._closed = True

    def combine_query_params(self, query, parameters):
        converted_params = None
        params_tuple = True
        index = 0
        idx = 0
        while idx < len(query):
            if query[idx] == "%" and query[idx+1] == "%":
                idx += 1
            elif query[idx] == "%" and query[idx + 1] == "(":
                end = query.find(")", idx)
                if end < 0:
                    idx += 1
                    continue
                key = query[idx+2:end]
                value = parameters.get(key)
                if converted_params is None:
                    converted_params = {}
                converted_params[key] = quote(value, self.connection)
                params_tuple = False
                idx = end
            elif query[idx] == "%":
                value = parameters[index]
                if converted_params is None:
                    converted_params = [None] * len(parameters)
                converted_params[index] = quote(value, self.connection)
                index += 1
                idx += 1
            idx += 1
        if converted_params is None:
            converted_params = tuple()
        elif params_tuple:
            converted_params = tuple(converted_params)

        try:
            return query % converted_params
        except TypeError, e:
            msg = e.message
            if msg == "not all arguments converted during string formatting":
                raise ProgrammingError()
            # TODO: catch some TypeError's and transform them into
            # ProgrammingErrors
            return query

    @check_closed
    def execute(self, query, parameters=None):
        self.description = None
        self.query = self.combine_query_params(query, parameters)

        conn = self.connection
        conn._begin_transaction()
        self.clear_pgres()
        self.pgres = libpq.PQexec(conn._pgconn, str(self.query))
        if not self.pgres:
            conn._raise_operational_error(self.pgres)

        pgstatus = libpq.PQresultStatus(self.pgres)
        self.pgstatus = libpq.PQcmdStatus(self.pgres)

        self.no_tuples = True
        self.row = 0

        if pgstatus == libpq.PGRES_COMMAND_OK:
            rowcount = libpq.PQcmdTuples(self.pgres)
            if not rowcount or not rowcount[0]:
                self.rowcount = -1
            else:
                self.rowcount = int(rowcount)
            self.lastrowid = libpq.PQoidValue(self.pgres)
            self.clear_pgres()

        elif pgstatus == libpq.PGRES_TUPLES_OK:
            self.rowcount = libpq.PQntuples(self.pgres)
            self.no_tuples = False
            self.nfields = libpq.PQnfields(self.pgres)
            description = [None] * self.nfields
            casts = []
            for i in xrange(self.nfields):
                field_description = [None] * 7
                field_description[0] = libpq.PQfname(self.pgres, i)

                ftype = libpq.PQftype(self.pgres, i)
                field_description[1] = ftype
                try:
                    cast = self.typecasts[ftype]
                except KeyError:
                    try:
                        cast = self.connection.typecasts[ftype]
                    except KeyError:

                        try:
                            cast = extensions.string_types[ftype]
                        except KeyError:
                            cast = extensions.string_types[19]
                casts.append(cast)
                description[i] = tuple(field_description)

            self.description = tuple(description)
            self.casts = casts

        else:
            conn._raise_operational_error(self.pgres)

    @check_closed
    def executemany(self, query, paramlist):
        self.rowcount = -1
        rowcount = 0
        for params in paramlist:
            self.execute(query, params)
            if self.rowcount == -1:
                rowcount = -1
            else:
                rowcount += self.rowcount
        self.rowcount = rowcount

    def build_row(self, row_num):
        n = self.nfields
        row = []
        for i in xrange(n):
            if libpq.PQgetisnull(self.pgres, row_num, i):
                val = None
            else:
                val = libpq.PQgetvalue(self.pgres, row_num, i)
                length = libpq.PQgetlength(self.pgres, row_num, i)
                val = extensions.typecast(self.casts[i], val, length, self)

            row.append(val)
        return tuple(row)

    @check_closed
    @check_no_tuples
    def fetchone(self):
        if self.row >= self.rowcount:
            return None

        row = self.build_row(self.row)
        self.row += 1
        return row

    @check_closed
    @check_no_tuples
    def fetchall(self):
        size = self.rowcount - self.row
        if size <= 0:
            return []

        result = []
        for row in xrange(size):
            result.append(self.build_row(self.row))
            self.row += 1
        return result

    @check_closed
    @check_no_tuples
    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        if size > self.rowcount - self.row or size < 0:
            size = self.rowcount - self.row

        if size <= 0:
            return []

        rows = []
        for i in xrange(size):
            rows.append(self.build_row(self.row))
            self.row += 1
        return rows

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
        return self.combine_query_params(query, vars)

    @check_closed
    def __iter__(self):
        return self

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration()
        return row



