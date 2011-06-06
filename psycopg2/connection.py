from functools import wraps

from psycopg2 import libpq
from psycopg2 import exceptions
from psycopg2.cursor import Cursor
from psycopg2.xid import Xid


def conn_notice_callback(notices_addr, message):
    notices = Notices.from_address(notices_addr)
    new_message = libpq.cast(
        libpq.pointer(libpq.create_string_buffer(message)), libpq.c_char_p)


    if notices.stop == 50:
        idx = notices.start
        notices.items[idx] = new_message
        notices.start = (notices.start + 1) % 50
    else:
        notices.items[notices.stop] = new_message
        notices.stop += 1


def check_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise exceptions.InterfaceError("connection already closed")
        return func(self, *args, **kwargs)
    return wrapper

def check_tpc(command):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if self.tpc_xid:
                raise exceptions.ProgrammingError(
                    "%s cannot be used during a two-phase transaction" % command)
        return wrapper
    return decorator


class Notices(libpq.Structure):
    _fields_ = [
        ('start', libpq.c_uint),
        ('stop', libpq.c_uint),
        ('items', libpq.c_char_p * 50)
    ]


class Connection(object):

    ISOLATION_LEVEL_AUTOCOMMIT = 0
    ISOLATION_LEVEL_READ_COMMITTED = ISOLATION_LEVEL_READ_UNCOMMITTED = 1
    ISOLATION_LEVEL_SERIALIZABLE = ISOLATION_LEVEL_REPEATABLE_READ = 2

    CONN_STATUS_SETUP = 0
    CONN_STATUS_READY = 1
    CONN_STATUS_BEGIN = 2
    CONN_STATUS_PREPARED = 5


    def __init__(self, dsn):

        self.dsn = dsn
        self.status = self.CONN_STATUS_SETUP
        self.pgconn = libpq.PQconnectdb(self.dsn)
        self.closed = True
        self.encoding = None
        if not self.pgconn:
            raise exceptions.OperationalError('pgconnectdb() failed')

        elif libpq.PQstatus(self.pgconn) != libpq.CONNECTION_OK:
            error_msg = libpq.PQerrorMessage(self.pgconn)
            raise exceptions.OperationalError(error_msg)

        self._notices = Notices(start=0, stop=0)
        self._notice_callback = libpq.PQnoticeProcessor(conn_notice_callback)
        libpq.PQsetNoticeProcessor(self.pgconn, self._notice_callback,
            libpq.byref(self._notices))
        self.typecasts = {}
        self.tpc_xid = None
        self.setup()

    def __del__(self):
        self._close()

    def setup(self):
        pgres = libpq.PQexec(self.pgconn, 'SHOW default_transaction_isolation')
        if not pgres or libpq.PQresultStatus(pgres) != libpq.PGRES_TUPLES_OK:
            raise exceptions.OperationalError(
                "can't fetch default_isolation_level")

        isolation_level = libpq.PQgetvalue(pgres, 0, 0)
        libpq.PQclear(pgres)
        if (isolation_level == 'read uncommitted' or
            isolation_level == 'read committed'):
            self.isolation_level = self.ISOLATION_LEVEL_READ_COMMITTED
        else:
            self.isolation_level = self.ISOLATION_LEVEL_SERIALIZABLE
        client_encoding = libpq.PQparameterStatus(self.pgconn, 'client_encoding')
        self.encoding = client_encoding.upper()

        self.closed = False
        self.status = self.CONN_STATUS_READY

    def begin_transaction(self):
        if (self.status == self.CONN_STATUS_READY and
            self.isolation_level != self.ISOLATION_LEVEL_AUTOCOMMIT):
            sql = [
                    None,
                    'BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED',
                    'BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
                ][self.isolation_level]

            self.execute_command(sql)
            self.status = self.CONN_STATUS_BEGIN

    def execute_command(self, command):
        pgres = libpq.PQexec(self.pgconn, command)
        if not pgres:
            self.raise_operational_error(None)
        try:
            pgstatus = libpq.PQresultStatus(pgres)
            if pgstatus != libpq.PGRES_COMMAND_OK:
                self.raise_operational_error(pgres)
        finally:
            libpq.PQclear(pgres)

    def execute_tpc_command(self, command):
        from psycopg2 import QuotedString

        tid = self.tpc_xid.as_tid()
        tid = QuotedString(tid)
        tid.prepare(self)
        tid = str(tid.quote())
        cmd = "%s %s;" % (command, tid)
        self.execute_command(cmd)

    def finish_tpc(self, command, fallback):

        if not self.tpc_xid:
            raise exceptions.ProgrammingError(
                "tpc_commit/tpc_rollback with no parameter must be "
                "called in a two-phase transaction")

        if self.status == self.CONN_STATUS_BEGIN:
            if fallback == "commit":
                self._commit()
            elif fallback == "abort":
                self._rollback()
            else:
                raise exceptions.InternalError(
                    "bad fallback passed to finish_tpc")
        elif self.status == self.CONN_STATUS_PREPARED:
            self.execute_tpc_command(command)
        else:
            raise exceptions.InterfaceError(
                "unexpected state in tpc_commit/tpc_rollback")

        self.status = self.CONN_STATUS_READY
        self.tpc_xid = None

    def _close(self):
        self.closed = True

        if self.pgconn:
            libpq.PQfinish(self.pgconn)
            self.pgconn = None

        if self._notices:
            self._notices = None

    def _commit(self):
        if (self.isolation_level == self.ISOLATION_LEVEL_AUTOCOMMIT or
            self.status != self.CONN_STATUS_BEGIN):
            return
        self.execute_command("COMMIT")
        self.status = self.CONN_STATUS_READY

    def _rollback(self):
        if (self.isolation_level == self.ISOLATION_LEVEL_AUTOCOMMIT or
            self.status != self.CONN_STATUS_BEGIN):
            return
        self.execute_command("ROLLBACK")
        self.status = self.CONN_STATUS_READY

    @check_closed
    def close(self):
        return self._close()

    @check_closed
    @check_tpc('rollback')
    def rollback(self):
        self._rollback()

    @check_closed
    @check_tpc('commit')
    def commit(self):
        self._commit()

    @check_closed
    def reset(self):
        self.setup()

    @check_closed
    def set_isolation_level(self, level):
        if level < 0 or level > 2:
            raise ValueError("isolation level must be between 0 and 2")
        if self.isolation_level == level:
            return
        if self.isolation_level != self.ISOLATION_LEVEL_AUTOCOMMIT:
            self._rollback()
        self.isolation_level = level

    def get_transaction_status(self):
        return libpq.PQtransactionStatus(self.pgconn)

    def cursor(self, name=None):
        return Cursor(self, name)

    @check_closed
    def set_client_encoding(self, encoding):
        encoding = "".join([c for c in encoding if c != "-" and c != "_"])
        if self.encoding == encoding:
            return
        self._rollback()
        self.execute_command("SET client_encoding = %s" % encoding)
        self.encoding = encoding

    def get_exc_type_for_state(self, code):
        exc_type = None
        if code[0] == "2":
            if code[1] == "3":
                exc_type = exceptions.IntegrityError
        elif code[0] == "4":
            if code[1] == "2":
                exc_type = exceptions.ProgrammingError
        return exc_type

    @property
    def notices(self):
        if self._notices.stop == 50:
            notices = []
            for i in xrange(self._notices.start, self._notices.stop):
                notices.append(self._notices.items[i])
            for i in xrange(self._notices.start):
                notices.append(self._notices.items[i])
            return notices
        return [self._notices.items[i] for i in xrange(self._notices.stop)]

    def raise_operational_error(self, pgres):
        code = None
        error = None
        if pgres:
            error = libpq.PQresultErrorMessage(pgres)
            if error is not None:
                code = libpq.PQresultErrorField(pgres, libpq.PG_DIAG_SQLSTATE)
        if error is None:
            error = libpq.PQerrorMessage(self.pgconn)
        exc_type = None
        if code is not None:
            exc_type = self.get_exc_type_for_state(code)
        if exc_type is None:
            exc_type = exceptions.OperationalError
        raise exc_type(error)

    @check_closed
    def xid(self, format_id, gtrid, bqual):
        return Xid(format_id, gtrid, bqual)

    @check_closed
    def tpc_begin(self, xid):
        if self.status != self.CONN_STATUS_READY:
            raise exceptions.ProgrammingError(
                "tpc_begin must be called outside a transaction")

        if self.isolation_level == self.ISOLATION_LEVEL_AUTOCOMMIT:
            raise exceptions.ProgrammingError(
                "tpc_begin can't be called in autocommit mode")

        self.begin_transaction()
        self.tpc_xid = xid

    @check_closed
    def tpc_commit(self):
        self.finish_tpc("COMMIT PREPARED", "commit")

    @check_closed
    def tpc_rollback(self):
        self.finish_tpc("ROLLBACK PREPARED", "abort")

    @check_closed
    def tpc_prepare(self):
        if not self.tpc_xid:
            raise exceptions.ProgrammingError(
                "prepare must be called inside a two-phase transaction")


def connect(dsn=None, database=None, host=None, port=None, user=None,
            password=None):

    if dsn is None:
        dsn = ""
        if database is not None:
            dsn += "dbname=%s" % database
        if host is not None:
            dsn += " host=%s" % host
        if port is not None:
            if isinstance(port, str):
                port = int(port)

            if not isinstance(port, int):
                raise TypeError("port must be a string or int")
            dsn += " port=%d" % port
        if user is not None:
            dsn += " user=%s" % user
        if password is not None:
            dsn += " password=%s" % password
    return Connection(dsn)

