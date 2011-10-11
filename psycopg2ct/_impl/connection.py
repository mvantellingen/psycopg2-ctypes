from functools import wraps

from psycopg2ct._impl import consts
from psycopg2ct._impl import encodings as _enc
from psycopg2ct._impl import exceptions
from psycopg2ct._impl import libpq
from psycopg2ct._impl import util
from psycopg2ct._impl.cursor import Cursor
from psycopg2ct._impl.lobject import LargeObject
from psycopg2ct._impl.notify import Notify
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


def check_async(func):
    @wraps(func)
    def check_async_(self, *args, **kwargs):
        if self._async:
            raise exceptions.ProgrammingError(
                '%s cannot be used in asynchronous mode' % func.__name__)
        return func(self, *args, **kwargs)
    return check_async_





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

    def __init__(self, dsn, async=False):

        self.dsn = dsn
        self.status = consts.STATUS_SETUP
        self._encoding = None

        self._closed = False
        self._cancel = None
        self._typecasts = {}
        self._tpc_xid = None
        self._notifies = []
        self._autocommit = False
        self._pgconn = None
        self._equote = False


        # The number of commits/rollbacks done so far
        self._mark = 0

        self._async = async
        self._async_status = consts.ASYNC_DONE
        self._async_cursor = None

        self.notices = []

        if not self._async:
            self._connect_sync()
        else:
            self._connect_async()

    def _connect_sync(self):
        self._pgconn = libpq.PQconnectdb(self.dsn)
        if not self._pgconn:
            raise exceptions.OperationalError('PQconnectdb() failed')
        elif libpq.PQstatus(self._pgconn) == libpq.CONNECTION_BAD:
            raise util.create_operational_error(self._pgconn)

        # Register notice processor
        self._notice_callback = libpq.PQnoticeProcessor(self._process_notice)
        libpq.PQsetNoticeProcessor(self._pgconn, self._notice_callback, None)

        self.status = consts.STATUS_READY
        self._setup()

    def _connect_async(self):
        """Create an async connection.

        The connection will be completed banging on poll():
        First with self._conn_poll_connecting() that will finish connection,
        then with self._poll_setup_async() that will do the same job
        of self._setup().

        """
        self._pgconn = libpq.PQconnectStart(self.dsn)
        if not self._pgconn:
            raise exceptions.OperationalError('PQconnectStart() failed')
        elif libpq.PQstatus(self._pgconn) == libpq.CONNECTION_BAD:
            raise util.create_operational_error(self._pgconn)

        # Register notice processor
        self._notice_callback = libpq.PQnoticeProcessor(self._process_notice)
        libpq.PQsetNoticeProcessor(self._pgconn, self._notice_callback, None)

    def __del__(self):
        self._close()

    @check_closed
    def close(self):
        return self._close()

    @check_closed
    @check_async
    @check_tpc
    def rollback(self):
        self._rollback()

    @check_closed
    @check_async
    @check_tpc
    def commit(self):
        self._commit()

    @check_closed
    @check_async
    def reset(self):
        self._execute_command(
            "ABORT; RESET ALL; SET SESSION AUTHORIZATION DEFAULT;")
        self.status = consts.STATUS_READY
        self._mark += 1
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

    @check_async
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

    @property
    def async(self):
        return self._async

    @check_closed
    def get_backend_pid(self):
        return libpq.PQbackendPID(self._pgconn)

    def get_parameter_status(self, parameter):
        return libpq.PQparameterStatus(self._pgconn, parameter)

    def get_transaction_status(self):
        return libpq.PQtransactionStatus(self._pgconn)

    def cursor(self, name=None, cursor_factory=Cursor, withhold=False):
        cur = cursor_factory(self, name)

        if not isinstance(cur, Cursor):
            raise TypeError(
                "cursor factory must be subclass of %s" %
                '.'.join([Cursor.__module__, Cursor.__name__]))

        if withhold:
            if name:
                cur.withhold = True
            else:
                raise exceptions.ProgrammingError(
                    "withhold=True can be specified only for named cursors")

        if name and self._async:
            raise exceptions.ProgrammingError(
                "asynchronous connections cannot produce named cursors")

        cur._mark = self._mark
        return cur

    @check_closed
    @check_tpc
    def cancel(self):
        errbuf = libpq.create_string_buffer(256)

        if libpq.PQcancel(self._cancel, errbuf, len(errbuf)) == 0:
            raise self._create_exception(msg=errbuf)

    def isexecuting(self):
        if not self._async:
            return False

        if self.status != consts.STATUS_READY:
            return True

        if self._async_cursor is not None:
            return True

        return False

    @property
    def encoding(self):
        return self._encoding

    @check_closed
    @check_async
    def set_client_encoding(self, encoding):
        encoding = _enc.normalize(encoding)
        if self.encoding == encoding:
            return

        pyenc = _enc.encodings[encoding]
        self._rollback()
        self._set_guc('client_encoding', encoding)
        self._encoding = encoding
        self._py_enc = pyenc

    @property
    def notifies(self):
        return self._notifies

    @property
    @check_closed
    def protocol_version(self):
        return libpq.PQprotocolVersion(self._pgconn)

    @property
    @check_closed
    def server_version(self):
        return libpq.PQserverVersion(self._pgconn)

    def fileno(self):
        return libpq.PQsocket(self._pgconn)

    @property
    def closed(self):
        return self._closed

    @check_closed
    def xid(self, format_id, gtrid, bqual):
        return Xid(format_id, gtrid, bqual)

    @check_closed
    @check_async
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
    @check_async
    def tpc_commit(self, xid=None):
        self._finish_tpc('COMMIT PREPARED', self._commit, xid)

    @check_closed
    @check_async
    def tpc_rollback(self, xid=None):
        self._finish_tpc('ROLLBACK PREPARED', self._rollback, xid)

    @check_closed
    @check_async
    def tpc_prepare(self):
        if not self._tpc_xid:
            raise exceptions.ProgrammingError(
                'prepare must be called inside a two-phase transaction')

        self._execute_tpc_command('PREPARE TRANSACTION', self._tpc_xid)
        self.status = consts.STATUS_PREPARED

    @check_closed
    @check_async
    def tpc_recover(self):
        return Xid.tpc_recover(self)

    def lobject(self, oid=0, mode='', new_oid=0, new_file=None,
                lobject_factory=LargeObject):
        obj = lobject_factory(self, oid, mode, new_oid, new_file)
        return obj

    def poll(self):
        if self.status == consts.STATUS_SETUP:
            self.status = consts.STATUS_CONNECTING
            return consts.POLL_WRITE

        if self.status == consts.STATUS_CONNECTING:
            res = self._poll_connecting()
            if res == consts.POLL_OK and self._async:
                return self._poll_setup_async()
            return res

        if self.status in (consts.STATUS_READY, consts.STATUS_BEGIN,
                           consts.STATUS_PREPARED):
            res = self._poll_query()

            if res == consts.POLL_OK and self._async and self._async_cursor:

                # Get the cursor object from the weakref
                curs = self._async_cursor()
                if curs is None:
                    util.pq_clear_async(self._pgconn)
                    raise exceptions.InterfaceError(
                        "the asynchronous cursor has disappeared")

                libpq.PQclear(curs._pgres)

                curs._pgres = util.pq_get_last_result(self._pgconn)
                curs._pq_fetch()
                self._async_cursor = None
            return res

        return consts.POLL_ERROR

    def _poll_connecting(self):
        """poll during a connection attempt until the connection has
        established.

        """
        status_map = {
            libpq.PGRES_POLLING_OK: consts.POLL_OK,
            libpq.PGRES_POLLING_READING: consts.POLL_READ,
            libpq.PGRES_POLLING_WRITING: consts.POLL_WRITE,
            libpq.PGRES_POLLING_FAILED: consts.POLL_ERROR,
            libpq.PGRES_POLLING_ACTIVE: consts.POLL_ERROR
        }
        res = status_map.get(libpq.PQconnectPoll(self._pgconn), None)

        if res is None:
            return consts.POLL_ERROR
        elif res == consts.POLL_ERROR:
            raise exceptions.OperationalError("asynchronous connection failed")
        return res

    def _poll_query(self):
        """Poll the connection for the send query/retrieve result phase

        Advance the async_status (usually going WRITE -> READ -> DONE) but
        don't mess with the connection status.

        """
        if self._async_status == consts.ASYNC_WRITE:
            ret = self._poll_advance_write(libpq.PQflush(self._pgconn))

        elif self._async_status == consts.ASYNC_READ:
            if self._async:
                ret = self._poll_advance_read(self._is_busy())
            else:
                ret = self._poll_advance_read(self._is_busy())

        elif self._async_status == consts.ASYNC_DONE:
            ret = self._poll_advance_read(self._is_busy())

        else:
            ret = consts.POLL_ERROR

        return ret

    def _poll_advance_write(self, flush):
        """Advance to the next state after an attempt of flushing output"""
        if flush == 0:
            self._async_status = consts.ASYNC_READ
            return consts.POLL_READ

        if flush == 1:
            return consts.POLL_WRITE

        if flush == -1:
            raise util.create_operational_error(self._pgconn)

        return consts.POLL_ERROR

    def _poll_advance_read(self, busy):
        """Advance to the next state after a call to a _is_busy* method"""
        if busy == 0:
            self._async_status = consts.ASYNC_DONE
            return consts.POLL_OK

        if busy == 1:
            return consts.POLL_READ

        return consts.POLL_ERROR

    def _poll_setup_async(self):
        """Advance to the next state during an async connection setup

        If the connection is green, this is performed by the regular sync
        code so the queries are sent by conn_setup() while in
        CONN_STATUS_READY state.

        """
        if self.status == consts.STATUS_CONNECTING:
            util.pq_set_non_blocking(self._pgconn, 1, True)

            self._equote = self._get_equote()
            self._get_encoding()
            self._cancel = libpq.PQgetCancel(self._pgconn)
            if self._cancel is None:
                raise exceptions.OperationalError("can't get cancellation key")

            self._autocommit = True

            # If the current datestyle is not compatible (not ISO) then
            # force it to ISO
            if not util.validate_datestyle(self._pgconn):
                self.status = consts.STATUS_DATESTYLE

                if libpq.PQsendQuery(self._pgconn, "SET DATESTYLE TO 'ISO'"):
                    self._async_status = consts.ASYNC_WRITE
                    return consts.POLL_WRITE
                else:
                    raise util.create_operational_error(self._pgconn)

            self.status = consts.STATUS_READY
            return consts.POLL_OK

        if self.status == consts.STATUS_DATESTYLE:
            res = self._poll_query()
            if res != consts.POLL_OK:
                return res

            pgres = util.pq_get_last_result(self._pgconn)
            if not pgres or \
                libpq.PQresultStatus(pgres) != libpq.PGRES_COMMAND_OK:
                raise exceptions.OperationalError("can't set datetyle to ISO")
            libpq.PQclear(pgres)

            self.status = consts.STATUS_READY
            return consts.POLL_OK

        return consts.POLL_ERROR

    def _setup(self):
        self._equote = self._get_equote()
        self._get_encoding()

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
            raise self._create_exception()
        try:
            pgstatus = libpq.PQresultStatus(pgres)
            if pgstatus != libpq.PGRES_COMMAND_OK:
                raise self._create_exception(pgres=pgres)
        finally:
            libpq.PQclear(pgres)

    def _execute_tpc_command(self, command, xid):
        from psycopg2ct.extensions import QuotedString
        tid = QuotedString(str(xid))
        tid.prepare(self)
        cmd = '%s %s' % (command, tid)
        self._execute_command(cmd)
        self._mark += 1

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

        #if self._cancel:
        #    libpq.PQfreeCancel(self._cancel)

        if self._pgconn:
            libpq.PQfinish(self._pgconn)
            self._pgconn = None

        # Remove the notice processor, this removes a cyclic reference so
        # that the connection object can be garbage collected
        if self._notice_callback:
            self._notice_callback = None

        self.notices = []

    def _commit(self):
        if self._autocommit or self.status != consts.STATUS_BEGIN:
            return
        self._mark += 1
        self._execute_command('COMMIT')
        self.status = consts.STATUS_READY

    def _rollback(self):
        if self._autocommit or self.status != consts.STATUS_BEGIN:
            return
        self._mark += 1
        self._execute_command('ROLLBACK')
        self.status = consts.STATUS_READY

    def _get_encoding(self):
        """Retrieving encoding"""
        client_encoding = self.get_parameter_status('client_encoding')
        self._encoding = _enc.normalize(client_encoding)
        self._py_enc = _enc.encodings[self._encoding]

    def _get_equote(self):
        ret = libpq.PQparameterStatus(
            self._pgconn, 'standard_conforming_strings')
        return ret and ret == 'off'

    def _is_busy(self):
        if libpq.PQconsumeInput(self._pgconn) == 0:
            raise exceptions.OperationalError(
                libpq.PQerrorMessage(self._pgconn))
        res = libpq.PQisBusy(self._pgconn)
        self._process_notifies()
        return res

    def _process_notice(self, arg, message):
        self.notices.append(message)
        length = len(self.notices)
        if length > 50:
            del self.notices[:length - 50]

    def _process_notifies(self):
        while True:
            pg_notify = libpq.PQnotifies(self._pgconn)
            if not pg_notify:
                break

            notify = Notify(
                pg_notify.contents.be_pid,
                pg_notify.contents.relname,
                pg_notify.contents.extra)
            self._notifies.append(notify)

            libpq.PQfreemem(pg_notify)

    def _get_exc_type_for_state(self, code):
        exc_type = None
        if code[0] == '2':
            if code[1] == '3':
                exc_type = exceptions.IntegrityError
        elif code[0] == '4':
            if code[1] == '2':
                exc_type = exceptions.ProgrammingError
        return exc_type

    def _create_exception(self, pgres=None, msg=None):
        """Return the exception to be raise'd"""
        if not pgres:
            if not msg:
                msg = libpq.PQerrorMessage(self._pgconn)
            return exceptions.OperationalError(msg)

        if msg is None:
            msg = libpq.PQresultErrorMessage(pgres)

        exc_type = None
        if msg is not None:
            code = libpq.PQresultErrorField(pgres, libpq.PG_DIAG_SQLSTATE)
            if code is not None:
                exc_type = self._get_exc_type_for_state(code)
        else:
            msg = libpq.PQerrorMessage(self._pgconn)

        if not exc_type:
            exc_type = exceptions.OperationalError
        return exc_type(msg)


def connect(dsn=None, database=None, host=None, port=None, user=None,
            password=None, async=False, connection_factory=Connection):

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

    # Mimic the construction method as used by psycopg2, which notes:
    # Here we are breaking the connection.__init__ interface defined
    # by psycopg2. So, if not requiring an async conn, avoid passing
    # the async parameter.
    if async:
        return connection_factory(dsn, async=True)
    return connection_factory(dsn)

