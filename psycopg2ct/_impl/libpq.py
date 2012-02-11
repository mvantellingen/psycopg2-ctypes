"""ctypes interface to the libpq library"""
from ctypes import *

from psycopg2ct._config import PG_LIBRARY, PG_VERSION


if not PG_LIBRARY:
    raise RuntimeError('libpq not found!')
libpq = cdll.LoadLibrary(PG_LIBRARY)


class PGconn(Structure):
    _fields_ = []

PGconn_p = POINTER(PGconn)


class PGresult(Structure):
    _fields_ = []

PGresult_p = POINTER(PGresult)


class PGcancel(Structure):
    _fields_ = []

PGcancel_p = POINTER(PGcancel)


CONNECTION_OK = 0
CONNECTION_BAD = 1

ConnStatusType = c_int

PGRES_EMPTY_QUERY = 0
PGRES_COMMAND_OK = 1
PGRES_TUPLES_OK = 2
PGRES_COPY_OUT = 3
PGRES_COPY_IN = 4
PGRES_BAD_RESPONSE = 5
PGRES_NONFATAL_ERROR = 6
PGRES_FATAL_ERROR = 7

ExecStatusType = c_int

PG_DIAG_SEVERITY = ord('S')
PG_DIAG_SQLSTATE = ord('C')
PG_DIAG_MESSAGE_PRIMARY = ord('M')
PG_DIAG_MESSAGE_DETAIL = ord('D')
PG_DIAG_MESSAGE_HINT = ord('H')
PG_DIAG_STATEMENT_POSITION = 'P'
PG_DIAG_INTERNAL_POSITION = 'p'
PG_DIAG_INTERNAL_QUERY = ord('q')
PG_DIAG_CONTEXT = ord('W')
PG_DIAG_SOURCE_FILE = ord('F')
DIAG_SOURCE_LINE = ord('L')
PG_DIAG_SOURCE_FUNCTION = ord('R')


PGRES_POLLING_FAILED = 0
PGRES_POLLING_READING = 1
PGRES_POLLING_WRITING = 2
PGRES_POLLING_OK = 3
PGRES_POLLING_ACTIVE = 4

PostgresPollingStatusType = c_int


class PGnotify(Structure):
    _fields_ = [
        ('relname', c_char_p),
        ('be_pid', c_int),
        ('extra', c_char_p)
    ]

PGnotify_p = POINTER(PGnotify)


# Database connection control functions

PQconnectdb = libpq.PQconnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_p

PQconnectStart = libpq.PQconnectStart
PQconnectStart.argtypes = [c_char_p]
PQconnectStart.restype = PGconn_p

PQconnectPoll = libpq.PQconnectPoll
PQconnectPoll.argtypes = [PGconn_p]
PQconnectPoll.restype = PostgresPollingStatusType

PQfinish = libpq.PQfinish
PQfinish.argtypes = [PGconn_p]
PQfinish.restype = None

# Connection status functions

PQdb = libpq.PQdb
PQdb.argtypes = [PGconn_p]
PQdb.restype = c_char_p

PQuser = libpq.PQuser
PQuser.argtypes = [PGconn_p]
PQuser.restype = c_char_p

PQstatus = libpq.PQstatus
PQstatus.argtypes = [PGconn_p]
PQstatus.restype = ConnStatusType

PQtransactionStatus = libpq.PQtransactionStatus
PQtransactionStatus.argtypes = [PGconn_p]
PQtransactionStatus.restype = c_int

PQparameterStatus = libpq.PQparameterStatus
PQparameterStatus.argtypes = [PGconn_p, c_char_p]
PQparameterStatus.restype = c_char_p

PQprotocolVersion = libpq.PQprotocolVersion
PQprotocolVersion.argtypes = [PGconn_p]
PQprotocolVersion.restype = c_int

PQserverVersion = libpq.PQserverVersion
PQserverVersion.argtypes = [PGconn_p]
PQserverVersion.restype = c_int

PQerrorMessage = libpq.PQerrorMessage
PQerrorMessage.argtypes = [PGconn_p]
PQerrorMessage.restype = c_char_p

PQsocket = libpq.PQsocket
PQsocket.argtypes = [PGconn_p]
PQsocket.restype = c_int

PQbackendPID = libpq.PQbackendPID
PQbackendPID.argtypes = [PGconn_p]
PQbackendPID.restype = c_int

# Command execution functions

PQexec = libpq.PQexec
PQexec.argtypes = [PGconn_p, c_char_p]
PQexec.restype = PGresult_p

PQresultStatus = libpq.PQresultStatus
PQresultStatus.argtypes = [PGresult_p]
PQresultStatus.restype = ExecStatusType

PQresultErrorMessage = libpq.PQresultErrorMessage
PQresultErrorMessage.argtypes = [PGresult_p]
PQresultErrorMessage.restype = c_char_p

PQresultErrorField = libpq.PQresultErrorField
PQresultErrorField.argtypes = [PGresult_p, c_int]
PQresultErrorField.restype = c_char_p

PQclear = libpq.PQclear
PQclear.argtypes = [POINTER(PGresult)]
PQclear.restype = None

# Retrieving query result information

PQntuples = libpq.PQntuples
PQntuples.argtypes = [PGresult_p]
PQntuples.restype = c_int

PQnfields = libpq.PQnfields
PQnfields.argtypes = [PGresult_p]
PQnfields.restype = c_int

PQfname = libpq.PQfname
PQfname.argtypes = [PGresult_p, c_int]
PQfname.restype = c_char_p

PQftype = libpq.PQftype
PQftype.argtypes = [PGresult_p, c_int]
PQftype.restype = c_uint

PQfsize = libpq.PQfsize
PQfsize.argtypes = [PGresult_p, c_int]
PQfsize.restype = c_int

PQfmod = libpq.PQfmod
PQfmod.argtypes = [PGresult_p, c_int]
PQfmod.restype = c_int

PQgetisnull = libpq.PQgetisnull
PQgetisnull.argtypes = [PGresult_p, c_int, c_int]
PQgetisnull.restype = c_int

PQgetlength = libpq.PQgetlength
PQgetlength.argtypes = [PGresult_p, c_int, c_int]
PQgetlength.restype = c_int

PQgetvalue = libpq.PQgetvalue
PQgetvalue.argtypes = [PGresult_p, c_int, c_int]
PQgetvalue.restype = c_char_p

# Retrieving other result information

PQcmdStatus = libpq.PQcmdStatus
PQcmdStatus.argtypes = [PGresult_p]
PQcmdStatus.restype = c_char_p

PQcmdTuples = libpq.PQcmdTuples
PQcmdTuples.argtypes = [PGresult_p]
PQcmdTuples.restype = c_char_p

PQoidValue = libpq.PQoidValue
PQoidValue.argtypes = [PGresult_p]
PQoidValue.restype = c_uint

# Escaping string for inclusion in sql commands

if PG_VERSION >= 0x090000:
    PQescapeLiteral = libpq.PQescapeLiteral
    PQescapeLiteral.argtypes = [PGconn_p, c_char_p, c_uint]
    PQescapeLiteral.restype = POINTER(c_char)

PQescapeStringConn = libpq.PQescapeStringConn
PQescapeStringConn.restype = c_uint
PQescapeStringConn.argtypes = [PGconn_p, c_char_p, c_char_p, c_uint, POINTER(c_int)]

PQescapeString = libpq.PQescapeString
PQescapeString.argtypes = [c_char_p, c_char_p, c_uint]
PQescapeString.restype = c_uint

PQescapeByteaConn = libpq.PQescapeByteaConn
PQescapeByteaConn.argtypes = [PGconn_p, c_char_p, c_uint, POINTER(c_uint)]
PQescapeByteaConn.restype = POINTER(c_char)

PQescapeBytea = libpq.PQescapeBytea
PQescapeBytea.argtypes = [c_char_p, c_uint, POINTER(c_uint)]
PQescapeBytea.restype = POINTER(c_char)

PQunescapeBytea = libpq.PQunescapeBytea
PQunescapeBytea.argtypes = [POINTER(c_char), POINTER(c_uint)]
PQunescapeBytea.restype = POINTER(c_char)

# Asynchronous Command Processing

PQsendQuery = libpq.PQsendQuery
PQsendQuery.argtypes = [PGconn_p, c_char_p]
PQsendQuery.restype = c_int

PQgetResult = libpq.PQgetResult
PQgetResult.argtypes = [PGconn_p]
PQgetResult.restype = PGresult_p

PQconsumeInput = libpq.PQconsumeInput
PQconsumeInput.argtypes = [PGconn_p]
PQconsumeInput.restype = c_int

PQisBusy = libpq.PQisBusy
PQisBusy.argtypes = [PGconn_p]
PQisBusy.restype = c_int

PQsetnonblocking = libpq.PQsetnonblocking
PQsetnonblocking.argtypes = [PGconn_p, c_int]
PQsetnonblocking.restype = c_int

PQflush = libpq.PQflush
PQflush.argtypes = [PGconn_p]
PQflush.restype = c_int

# Cancelling queries in progress

PQgetCancel = libpq.PQgetCancel
PQgetCancel.argtypes = [PGconn_p]
PQgetCancel.restype = PGcancel_p

PQfreeCancel = libpq.PQfreeCancel
PQfreeCancel.argtypes = [PGcancel_p]
PQfreeCancel.restype = None

PQcancel = libpq.PQcancel
PQcancel.argtypes = [PGcancel_p, c_char_p, c_int]
PQcancel.restype = c_int

PQrequestCancel = libpq.PQrequestCancel
PQrequestCancel.argtypes = [PGconn_p]
PQrequestCancel.restype = c_int

# Functions Associated with the COPY Command

PQgetCopyData = libpq.PQgetCopyData
PQgetCopyData.argtypes = [PGconn_p, POINTER(c_char_p), c_int]
PQgetCopyData.restype = c_int

PQputCopyData = libpq.PQputCopyData
PQputCopyData.argtypes = [PGconn_p, c_char_p, c_int]
PQputCopyData.restype = c_int

PQputCopyEnd = libpq.PQputCopyEnd
PQputCopyEnd.argtypes = [PGconn_p, c_char_p]
PQputCopyEnd.restype = c_int

# Miscellaneous functions

PQfreemem = libpq.PQfreemem
PQfreemem.argtypes = [c_void_p]
PQfreemem.restype = None

# Notice processing

PQnoticeProcessor = CFUNCTYPE(None, c_void_p, c_char_p)

PQsetNoticeProcessor = libpq.PQsetNoticeProcessor
PQsetNoticeProcessor.argtypes = [PGconn_p, PQnoticeProcessor, c_void_p]
PQsetNoticeProcessor.restype = PQnoticeProcessor


PQnotifies = libpq.PQnotifies
PQnotifies.argtypes = [PGconn_p]
PQnotifies.restype = PGnotify_p


# Large object
Oid = c_int
lo_open = libpq.lo_open
lo_open.argtypes = [PGconn_p, Oid, c_int]
lo_open.restype = c_int

lo_create = libpq.lo_create
lo_create.argtypes = [PGconn_p, Oid]
lo_create.restype = Oid

lo_import = libpq.lo_import
lo_import.argtypes = [PGconn_p, c_char_p]
lo_import.restype = Oid

lo_read = libpq.lo_read
lo_read.argtypes = [PGconn_p, c_int, c_char_p, c_int]
lo_read.restype = c_int

lo_write = libpq.lo_write
lo_write.argtypes = [PGconn_p, c_int, c_char_p, c_int]
lo_write.restype = c_int

lo_tell = libpq.lo_tell
lo_tell.argtypes = [PGconn_p, c_int]
lo_tell.restype = c_int

lo_lseek = libpq.lo_lseek
lo_lseek.argtypes = [PGconn_p, c_int, c_int, c_int]
lo_lseek.restype = c_int

lo_close = libpq.lo_close
lo_close.argtypes = [PGconn_p, c_int]
lo_close.restype = c_int

lo_unlink = libpq.lo_unlink
lo_unlink.argtypes = [PGconn_p, Oid]
lo_unlink.restype = c_int

lo_export = libpq.lo_export
lo_export.argtypes = [PGconn_p, Oid, c_char_p]
lo_export.restype = c_int

lo_truncate = libpq.lo_truncate
lo_truncate.argtypes = [PGconn_p, c_int, c_int]
lo_truncate.restype = c_int

