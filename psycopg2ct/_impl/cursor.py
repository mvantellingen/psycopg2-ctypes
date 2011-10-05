from functools import wraps
from collections import namedtuple

from psycopg2ct import tz
from psycopg2ct._impl import encodings
from psycopg2ct._impl import libpq
from psycopg2ct._impl import typecasts
from psycopg2ct._impl.adapters import _getquoted
from psycopg2ct._impl.exceptions import InterfaceError, ProgrammingError


def check_closed(func):
    """Check if the connection is closed and raise an error"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise InterfaceError("connection already closed")
        return func(self, *args, **kwargs)
    return wrapper


def check_no_tuples(func):
    """Check if there are tuples available. This is only the case when the
    postgresql status was PGRES_TUPLES_OK

    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._no_tuples:
            raise ProgrammingError("no results to fetch")
        return func(self, *args, **kwargs)
    return wrapper

# Used for Cursor.description
Column = namedtuple('Column', ['name', 'type_code', 'display_size',
    'internal_size', 'precision', 'scale', 'null_ok'])


class Cursor(object):
    """These objects represent a database cursor, which is used to manage
    the context of a fetch operation.

    Cursors created from the same connection are not isolated, i.e., any
    changes done to the database by a cursor are immediately visible by the
    other cursors. Cursors created from different connections can or can not
    be isolated, depending on how the transaction support is implemented
    (see also the connection's .rollback() and .commit() methods).

    """

    def __init__(self, connection, name, row_factory=None):

        self._connection = connection

        #: This read/write attribute specifies the number of rows to fetch at
        #: a time with .fetchmany(). It defaults to 1 meaning to fetch a
        #: single row at a time.
        #:
        #: Implementations must observe this value with respect to the
        #: .fetchmany() method, but are free to interact with the database a
        #: single row at a time. It may also be used in the implementation of
        #: .executemany().
        self.arraysize = 1

        self.tzinfo_factory = tz.FixedOffsetTimezone
        self.row_factory = row_factory

        self._closed = False
        self._description = None
        self._lastrowid = 0
        self._name = name
        self._withhold = False
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
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result
        column:

          (name,
           type_code,
           display_size,
           internal_size,
           precision,
           scale,
           null_ok)

        The first two items (name and type_code) are mandatory, the other
        five are optional and are set to None if no meaningful values can be
        provided.

        This attribute will be None for operations that do not return rows or
        if the cursor has not had an operation invoked via the .execute*()
        method yet.

        The type_code can be interpreted by comparing it to the Type Objects
        specified in the section below.

        """
        return self._description

    @property
    def rowcount(self):
        """This read-only attribute specifies the number of rows that the
        last .execute*() produced (for DQL statements like 'select') or
        affected (for DML statements like 'update' or 'insert').

        The attribute is -1 in case no .execute*() has been performed on the
        cursor or the rowcount of the last operation is cannot be determined
        by the interface.

        Note: Future versions of the DB API specification could redefine the
        latter case to have the object return None instead of -1.

        """
        return self._rowcount

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

    def close(self):
        """Close the cursor now (rather than whenever __del__ is called).

        The cursor will be unusable from this point forward; an Error
        (or subclass) exception will be raised if any operation is attempted
        with the cursor.

        """
        self._closed = True

    @check_closed
    def execute(self, query, parameters=None):
        """Prepare and execute a database operation (query or command).

        Parameters may be provided as sequence or mapping and will be bound to
        variables in the operation.  Variables are specified in a
        database-specific notation (see the module's paramstyle attribute for
        details).

        A reference to the operation will be retained by the cursor.  If the
        same operation object is passed in again, then the cursor can optimize
        its behavior.  This is most effective for algorithms where the same
        operation is used, but different parameters are bound to it
        (many times).

        For maximum efficiency when reusing an operation, it is best to use
        the .setinputsizes() method to specify the parameter types and sizes
        ahead of time.  It is legal for a parameter to not match the
        predefined information; the implementation should compensate,
        possibly with a loss of efficiency.

        The parameters may also be specified as list of tuples to e.g. insert
        multiple rows in a single operation, but this kind of usage is
        deprecated: .executemany() should be used instead.

        Return values are not defined.

        """
        self._description = None
        conn = self._connection

        if isinstance(query, unicode):
            encoding = encodings[self._connection.encoding]
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
            description = []
            casts = []
            for i in xrange(self._nfields):
                ftype = libpq.PQftype(self._pgres, i)
                try:
                    cast = self._typecasts[ftype]
                except KeyError:
                    try:
                        cast = self._connection._typecasts[ftype]
                    except KeyError:

                        try:
                            cast = typecasts.string_types[ftype]
                        except KeyError:
                            cast = typecasts.string_types[19]
                casts.append(cast)

                description.append(Column(
                    name=libpq.PQfname(self._pgres, i),
                    type_code=ftype,
                    display_size=None,
                    internal_size=None,
                    precision=None,
                    scale=None,
                    null_ok=None,
                ))


            self._description = tuple(description)
            self._casts = casts

        else:
            conn._raise_operational_error(self._pgres)

    @check_closed
    def executemany(self, query, paramlist):
        """Prepare a database operation (query or command) and then execute
        it against all parameter sequences or mappings found in the sequence
        seq_of_parameters.

        Modules are free to implement this method using multiple calls to the
        .execute() method or by using array operations to have the database
        process the sequence as a whole in one call.

        Use of this method for an operation which produces one or more result
        sets constitutes undefined behavior, and the implementation is
        permitted (but not required) to raise an exception when it detects
        that a result set has been created by an invocation of the operation.

        The same comments as for .execute() also apply accordingly to this
        method.

        Return values are not defined.

        """
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
    @check_no_tuples
    def fetchone(self):
        """Fetch the next row of a query result set, returning a single
        sequence, or None when no more data is available. [6]


        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        """
        if self._rownumber >= self._rowcount:
            return None

        row = self._build_row(self._rownumber)
        self._rownumber += 1
        return row

    @check_closed
    @check_no_tuples
    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty sequence is
        returned when no more rows are available.

        The number of rows to fetch per call is specified by the parameter.
        If it is not given, the cursor's arraysize determines the number of
        rows to be fetched. The method should try to fetch as many rows as
        indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be
        returned.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note there are performance considerations involved with the size
        parameter.  For optimal performance, it is usually best to use the
        arraysize attribute.  If the size parameter is used, then it is best
        for it to retain the same value from one .fetchmany() call to the
        next.

        """
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
    @check_no_tuples
    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples).

        Note that the cursor's arraysize attribute can affect the performance
        of this operation.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        """
        size = self._rowcount - self._rownumber
        if size <= 0:
            return []

        result = []
        for row in xrange(size):
            result.append(self._build_row(self._rownumber))
            self._rownumber += 1
        return result

    def nextset(self):
        """This method will make the cursor skip to the next available set,
        discarding any remaining rows from the current set.

        If there are no more sets, the method returns None. Otherwise, it
        returns a true value and subsequent calls to the fetch methods will
        return rows from the next result set.

        An Error (or subclass) exception is raised if the previous call to
        .execute*() did not produce any result set or no call was issued yet.

        Note: this method is not supported

        """
        raise NotImplementedError()

    def mogrify(self, query, vars=None):
        """Return the the querystring with the vars binded.

        This is not part of the dbapi 2 standard, but a psycopg2 extension.

        """
        return _combine_cmd_params(query, vars, self._connection)

    def setinputsizes(self, sizes):
        """This can be used before a call to .execute*() to predefine memory
        areas for the operation's parameters.

        sizes is specified as a sequence -- one item for each input
        parameter.  The item should be a Type Object that corresponds to the
        input that will be used, or it should be an integer specifying the
        maximum length of a string parameter.  If the item is None, then no
        predefined memory area will be reserved for that column (this is
        useful to avoid predefined areas for large inputs).

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are
        free to not use it.

        """
        pass

    def setoutputsize(self, size, column=None):
        """Set a column buffer size for fetches of large columns (e.g.
        LONGs, BLOBs, etc.).

        The column is specified as an index into the result sequence.  Not
        specifying the column will set the default size for all large columns
        in the cursor.

        This method would be used before the .execute*() method is invoked.

        Implementations are free to have this method do nothing and users are
        free to not use it.

        """
        pass

    @property
    def rownumber(self):
        """This read-only attribute should provide the current 0-based index
        of the cursor in the result set or None if the index cannot be
        determined.

        The index can be seen as index of the cursor in a sequence (the
        result set). The next fetch operation will fetch the row indexed by
        .rownumber in that sequence.

        This is an optional DB API extension.

        """
        return self._rownumber

    @property
    def connection(self):
        """This read-only attribute return a reference to the Connection
        object on which the cursor was created.

        The attribute simplifies writing polymorph code in multi-connection
        environments.

        This is an optional DB API extension.

        """
        return self._connection

    @check_closed
    def __iter__(self):
        """Return self to make cursors compatible to the iteration protocol

        This is an optional DB API extension.

        """
        return self

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration()
        return row

    @property
    def lastrowid(self):
        """This read-only attribute provides the OID of the last row inserted
        by the cursor.

        If the table wasn't created with OID support or the last operation is
        not a single record insert, the attribute is set to None.

        This is a Psycopg extension to the DB API 2.0

        """
        return self._lastrowid

    @property
    def name(self):
        """Name of the cursor if it was created with a name

        This is a Psycopg extension to the DB API 2.0

        """
        return self._name

    @property
    def query(self):
        return self._query

    @property
    def statusmessage(self):
        """Read-only attribute containing the message returned by the last
        command.

        This is a Psycopg extension to the DB API 2.0

        """
        return self._statusmessage

    @property
    def withhold(self):
        return self._withhold

    @withhold.setter
    def withhold(self, value):
        if not self._name:
            raise ProgrammingError(
                "trying to set .withhold on unnamed cursor")

        self._withhold = bool(value)

    def _clear_pgres(self):
        if self._pgres:
            libpq.PQclear(self._pgres)
            self._pgres = None

    def _build_row(self, row_num):

        # Create the row
        if self.row_factory:
            row = self.row_factory(self)
            is_tuple = False
        else:
            row = [None] * len(self.description)
            is_tuple = True

        # Fill it
        n = self._nfields
        for i in xrange(n):
            if libpq.PQgetisnull(self._pgres, row_num, i):
                val = None
            else:
                val = libpq.PQgetvalue(self._pgres, row_num, i)
                length = libpq.PQgetlength(self._pgres, row_num, i)
                val = typecasts.typecast(self._casts[i], val, length, self)
            row[i] = val

        if is_tuple:
            return tuple(row)
        return row



def _combine_cmd_params(cmd, params, conn):
    """Combine the command string and params"""

    # Return when no argument binding is required.  Note that this method is
    # not called from .execute() if `params` is None.
    if '%' not in cmd:
        return cmd

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

    cmd_length = len(cmd)
    while idx < cmd_length:

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
                raise ProgrammingError(
                    "incomplete placeholder: '%(' without ')'")

            key = cmd[idx + 2:end]
            if arg_values is None:
                arg_values = {}
            if key not in arg_values:
                arg_values[key] = _getquoted(params[key], conn)

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

            value = _getquoted(params[param_num], conn)
            arg_values.append(value)

            param_num += 1
            idx += 1

        idx += 1

    if named_args_format is False:
        if len(arg_values) != len(params):
            raise TypeError(
                "not all arguments converted during string formatting")
        arg_values = tuple(arg_values)

    if not arg_values:
        return cmd % tuple()  # Required to unescape % chars
    return cmd % arg_values

