from functools import wraps
from collections import deque

from psycopg2ct._impl import consts
from psycopg2ct._impl import encodings as _enc
from psycopg2ct._impl import exceptions
from psycopg2ct._impl import libpq
from psycopg2ct._impl.cursor import Cursor
from psycopg2ct._impl.xid import Xid


# Map between isolation levels names and values and back.
_isolevels = {
    '':                 consts.ISOLATION_LEVEL_AUTOCOMMIT,
    'read uncommitted': consts.ISOLATION_LEVEL_READ_UNCOMMITTED,
    'read committed':   consts.ISOLATION_LEVEL_READ_COMMITTED,
    'repeatable read':  consts.ISOLATION_LEVEL_REPEATABLE_READ,
    'serializable':     consts.ISOLATION_LEVEL_SERIALIZABLE,
    'default':         -1,
}

for k, v in _isolevels.items():
    _isolevels[v] = k

del k, v


def check_closed(func):
    @wraps(func)
    def check_closed_(self, *args, **kwargs):
        if self.closed:
            raise exceptions.InterfaceError('connection already closed')
        return func(self, *args, **kwargs)
    return check_closed_

def check_notrans(func):
    @wraps(func)
    def check_notrans_(self, *args, **kwargs):
        if self.status != consts.STATUS_READY:
            raise exceptions.ProgrammingError('not valid in transaction')
        return func(self, *args, **kwargs)
    return check_notrans_

def check_tpc(func):
    @wraps(func)
    def check_tpc_(self, *args, **kwargs):
        if self._tpc_xid:
            raise exceptions.ProgrammingError(
                '%s cannot be used during a two-phase transaction'
                % func.__name__)
        return func(self, *args, **kwargs)
    return check_tpc_


class Connection(object):

    # Various exceptions which should be accessible via the Connection
    # class according to dbapi 2.0
    Error = exceptions.Error
    DatabaseError = exceptions.DatabaseError
    IntegrityError = exceptions.IntegrityError
    InterfaceError = exceptions.InterfaceError
    InternalError = exceptions.InternalError
    NotSupportedError = exceptions.NotSupportedError
    OperationalError = exceptions.OperationalError
    ProgrammingError = exceptions.ProgrammingError
    Warning = exceptions.Warning


    def __init__(self, dsn):

        self.dsn = dsn
        self.status = consts.STATUS_SETUP
        self._encoding = None

        self._closed = True
        self._cancel = None
        self._typecasts = {}
        self._tpc_xid = None
        self._notices = deque(maxlen=50)
        self._autocommit = False

        # Connect
        self._pgconn = libpq.PQconnectdb(dsn)
        if not self._pgconn:
            raise exceptions.OperationalError('pgconnectdb() failed')
        elif libpq.PQstatus(self._pgconn) != libpq.CONNECTION_OK:
            error_msg = libpq.PQerrorMessage(self._pgconn)
            raise exceptions.OperationalError(error_msg)

        # Register notice processor
        self._notice_callback = libpq.PQnoticeProcessor(
            lambda arg, message: self._notices.append(message))
        libpq.PQsetNoticeProcessor(self._pgconn, self._notice_callback, None)

        # Setup the connection
        self._setup()

    def __del__(self):
        self._close()

    @check_closed
    def close(self):
        return self._close()

    @check_closed
    @check_tpc
    def rollback(self):
        self._rollback()

    @check_closed
    @check_tpc
    def commit(self):
        self._commit()

    @check_closed
    def reset(self):
        self._execute_command(
            "ABORT; RESET ALL; SET SESSION AUTHORIZATION DEFAULT;")
        self.status = consts.STATUS_READY
        self._autocommit = False
        self._tpc_xid = None

    def _get_guc(self, name):
        """Return the value of a configuration parameter."""
        pgres = libpq.PQexec(self._pgconn, 'SHOW %s' % name)
        if not pgres or libpq.PQresultStatus(pgres) != libpq.PGRES_TUPLES_OK:
            raise exceptions.OperationalError(
                "can't fetch %s" % name)

        rv = libpq.PQgetvalue(pgres, 0, 0)
        libpq.PQclear(pgres)

        return rv

    def _set_guc(self, name, value):
        """Set the value of a configuration parameter."""
        if value.lower() != 'default':
            # TODO: use the string adapter here
            value = "'%s'" % value;

        self._execute_command('SET %s TO %s' % (name, value))

    def _set_guc_onoff(self, name, value):
        """Set the value of a configuration parameter to a boolean.
        
        The string 'default' is accepted too.
        """
        if isinstance(value, basestring) and value.lower() == 'default':
            value = 'default'
        else:
            value = value and 'on' or 'off'

        self._set_guc(name, value)

    @property
    @check_closed
    def isolation_level(self):
        if self._autocommit:
            return consts.ISOLATION_LEVEL_AUTOCOMMIT
        else:
            name = self._get_guc('default_transaction_isolation')
            return _isolevels[name.lower()]

    def set_isolation_level(self, level):
        if level < 0 or level > 4:
            raise ValueError('isolation level must be between 0 and 4')

        prev = self.isolation_level
        if prev == level:
            return

        self._rollback()
        if level == consts.ISOLATION_LEVEL_AUTOCOMMIT:
            return self.set_session(autocommit=True)
        else:
            return self.set_session(isolation_level=level, autocommit=False)

    @check_closed
    @check_notrans
    def set_session(self, isolation_level=None, readonly=None, deferrable=None,
                    autocommit=None):
        if isolation_level is not None:
            if isinstance(isolation_level, int):
                if isolation_level < 1 or isolation_level > 4:
                    raise ValueError('isolation level must be between 1 and 4')
                isolation_level = _isolevels[isolation_level]
            elif isinstance(isolation_level, basestring):
                if not isolation_level \
                or isolation_level.lower() not in _isolevels:
                    raise ValueError("bad value for isolation level: '%s'" %
                        isolation_level)
            else:
                raise TypeError("bad isolation level: '%r'" % isolation_level)

            self._set_guc("default_transaction_isolation", isolation_level)

        if readonly is not None:
            self._set_guc_onoff('default_transaction_read_only', readonly)

        if deferrable is not None:
            self._set_guc_onoff('default_transaction_deferrable', deferrable)

        if autocommit is not None:
            self._autocommit = bool(autocommit)

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.set_session(autocommit=value)

    @check_closed
    def get_backend_pid(self):
        return libpq.PQbackendPID(self._pgconn)

    def get_parameter_status(self, parameter):
        return libpq.PQparameterStatus(self._pgconn, parameter)

    def get_transaction_status(self):
        return libpq.PQtransactionStatus(self._pgconn)

    def cursor(self, name=None, cursor_factory=Cursor, withhold=False):
        cur = cursor_factory(self, name)
        if withhold:
            if name:
                cur.withhold = True
            else:
                raise exceptions.ProgrammingError(
                    "withhold=True can be specified only for named cursors")

        return cur

    @check_closed
    @check_tpc
    def cancel(self):
        errbuf = libpq.create_string_buffer(256)

        if libpq.PQcancel(self._cancel, errbuf, len(errbuf)) == 0:
            self._raise_operational_error(errbuf)

    @property
    def encoding(self):
        return self._encoding

    @check_closed
    def set_client_encoding(self, encoding):
        encoding = _enc.normalize(encoding)
        if self.encoding == encoding:
            return

        pyenc = _enc.encodings[encoding]
        self._rollback()
        self._set_guc('client_encoding', encoding)
        self._encoding = encoding
        self._py_enc = pyenc

    def get_exc_type_for_state(self, code):
        exc_type = None
        if code[0] == '2':
            if code[1] == '3':
                exc_type = exceptions.IntegrityError
        elif code[0] == '4':
            if code[1] == '2':
                exc_type = exceptions.ProgrammingError
        return exc_type

    @property
    def notices(self):
        return self._notices

    @property
    @check_closed
    def protocol_version(self):
        return libpq.PQprotocolVersion(self._pgconn)

    @property
    @check_closed
    def server_version(self):
        return libpq.PQserverVersion(self._pgconn)

    @property
    def closed(self):
        return self._closed

    @check_closed
    def xid(self, format_id, gtrid, bqual):
        return Xid(format_id, gtrid, bqual)

    @check_closed
    def tpc_begin(self, xid):
        if not isinstance(xid, Xid):
            xid = Xid.from_string(xid)

        if self.status != consts.STATUS_READY:
            raise exceptions.ProgrammingError(
                'tpc_begin must be called outside a transaction')

        if self._autocommit:
            raise exceptions.ProgrammingError(
                "tpc_begin can't be called in autocommit mode")

        self._begin_transaction()
        self._tpc_xid = xid

    @check_closed
    def tpc_commit(self, xid=None):
        self._finish_tpc('COMMIT PREPARED', self._commit, xid)

    @check_closed
    def tpc_rollback(self, xid=None):
        self._finish_tpc('ROLLBACK PREPARED', self._rollback, xid)

    @check_closed
    def tpc_prepare(self):
        if not self._tpc_xid:
            raise exceptions.ProgrammingError(
                'prepare must be called inside a two-phase transaction')

        self._execute_tpc_command('PREPARE TRANSACTION', self._tpc_xid)
        self.status = consts.STATUS_PREPARED

    @check_closed
    def tpc_recover(self):
        return Xid.tpc_recover(self)

    def _setup(self):
        # Get encoding
        client_encoding = self.get_parameter_status('client_encoding')
        self._encoding = _enc.normalize(client_encoding)
        self._py_enc = _enc.encodings[self.encoding]

        self._cancel = libpq.PQgetCancel(self._pgconn)
        if self._cancel is None:
            raise exceptions.OperationalError("can't get cancellation key")

        self._closed = False
        self.status = consts.STATUS_READY

    def _begin_transaction(self):
        if self.status == consts.STATUS_READY and not self._autocommit:
            self._execute_command('BEGIN')
            self.status = consts.STATUS_BEGIN

    def _execute_command(self, command):
        pgres = libpq.PQexec(self._pgconn, command)
        if not pgres:
            self._raise_operational_error(None)
        try:
            pgstatus = libpq.PQresultStatus(pgres)
            if pgstatus != libpq.PGRES_COMMAND_OK:
                self._raise_operational_error(pgres)
        finally:
            libpq.PQclear(pgres)

    def _execute_tpc_command(self, command, xid):
        from psycopg2ct.extensions import QuotedString
        tid = QuotedString(str(xid))
        tid.prepare(self)
        cmd = '%s %s' % (command, tid)
        self._execute_command(cmd)

    def _finish_tpc(self, command, fallback, xid):
        if xid:
            # committing/aborting a received transaction.
            if self.status != consts.STATUS_READY:
                raise exceptions.ProgrammingError(
                    "tpc_commit/tpc_rollback with a xid "
                    "must be called outside a transaction")

            self._execute_tpc_command(command, xid)

        else:
            # committing/aborting our own transaction.
            if not self._tpc_xid:
                raise exceptions.ProgrammingError(
                    "tpc_commit/tpc_rollback with no parameter "
                    "must be called in a two-phase transaction")

            if self.status == consts.STATUS_BEGIN:
                fallback()
            elif self.status == consts.STATUS_PREPARED:
                self._execute_tpc_command(command, self._tpc_xid)
            else:
                raise exceptions.InterfaceError(
                    'unexpected state in tpc_commit/tpc_rollback')

            self.status = consts.STATUS_READY
            self._tpc_xid = None

    def _close(self):
        self._closed = True

        if self._pgconn:
            libpq.PQfinish(self._pgconn)
            self._pgconn = None
        self._notices = None

    def _commit(self):
        if self._autocommit or self.status != consts.STATUS_BEGIN:
            return
        self._execute_command('COMMIT')
        self.status = consts.STATUS_READY

    def _rollback(self):
        if self._autocommit or self.status != consts.STATUS_BEGIN:
            return
        self._execute_command('ROLLBACK')
        self.status = consts.STATUS_READY

    def _raise_operational_error(self, pgres):
        code = None
        error = None
        if pgres:
            error = libpq.PQresultErrorMessage(pgres)
            if error is not None:
                code = libpq.PQresultErrorField(pgres, libpq.PG_DIAG_SQLSTATE)
        if error is None:
            error = libpq.PQerrorMessage(self._pgconn)
        exc_type = None
        if code is not None:
            exc_type = self.get_exc_type_for_state(code)
        if exc_type is None:
            exc_type = exceptions.OperationalError
        raise exc_type(error)


def connect(dsn=None, database=None, host=None, port=None, user=None,
            password=None, async=False, connection_factory=Connection):
    if async:
        raise NotImplementedError()

    if dsn is None:
        args = []
        if database is not None:
            args.append('dbname=%s' % database)
        if host is not None:
            args.append('host=%s' % host)
        if port is not None:
            if isinstance(port, str):
                port = int(port)

            if not isinstance(port, int):
                raise TypeError('port must be a string or int')
            args.append('port=%d' % port)
        if user is not None:
            args.append('user=%s' % user)
        if password is not None:
            args.append('password=%s' % password)
        dsn = ' '.join(args)
    return connection_factory(dsn)

