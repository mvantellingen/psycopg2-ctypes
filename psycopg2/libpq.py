"""ctypes interface to the libpq library"""
from ctypes import *
from ctypes.util import find_library

path = find_library('libpq')
if not path:
    path = '/opt/local/lib/postgresql90/libpq.dylib'
libpq = cdll.LoadLibrary(path)


class PGconn(Structure):
    _fields_ = []

PGconn_p = POINTER(PGconn)


class PGresult(Structure):
    _fields_ = []

PGresult_p = POINTER(PGresult)

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


class PGnotify(Structure):
    pass


PQnoticeProcessor = CFUNCTYPE(None, c_void_p, c_char_p)


PQconnectdb = libpq.PQconnectdb
PQconnectdb.argtypes = [c_char_p]
PQconnectdb.restype = PGconn_p

PQfinish = libpq.PQfinish
PQfinish.argtypes = [PGconn_p]
PQfinish.restype = None

PQclear = libpq.PQclear
PQclear.argtypes = [POINTER(PGresult)]
PQclear.restype = None

PQfreemem = libpq.PQfreemem
PQfreemem.argtypes = [c_void_p]
PQfreemem.restype = None

PQdb = libpq.PQdb
PQdb.argtypes = [PGconn_p]
PQdb.restype = c_char_p

PQerrorMessage = libpq.PQerrorMessage
PQerrorMessage.argtypes = [PGconn_p]
PQerrorMessage.restype = c_char_p

PQparameterStatus = libpq.PQparameterStatus
PQparameterStatus.argtypes = [PGconn_p, c_char_p]
PQparameterStatus.restype = c_char_p

PQescapeStringConn = libpq.PQescapeStringConn
PQescapeStringConn.restype = c_uint
PQescapeStringConn.argtypes = [PGconn_p, c_char_p, c_char_p, c_uint, POINTER(c_int)]

PQescapeString = libpq.PQescapeString
PQescapeString.restype = c_uint
PQescapeString.argtypes = [c_char_p, c_char_p, c_uint]

PQescapeBytea = libpq.PQescapeBytea
PQescapeBytea.argtypes = [c_char_p, c_uint, POINTER(c_uint)]
PQescapeBytea.restype = POINTER(c_char)

PQescapeByteaConn = libpq.PQescapeByteaConn
PQescapeByteaConn.argtypes = [PGconn_p, c_char_p, c_uint, POINTER(c_uint)]
PQescapeByteaConn.restype = POINTER(c_char)

PQunescapeBytea = libpq.PQunescapeBytea
PQunescapeBytea.argtypes = [POINTER(c_char), POINTER(c_uint)]
PQunescapeBytea.restype = POINTER(c_char)

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

PQsetNoticeProcessor = libpq.PQsetNoticeProcessor
PQsetNoticeProcessor.argtypes = [PGconn_p, PQnoticeProcessor, c_void_p]
PQsetNoticeProcessor.restype = PQnoticeProcessor

PQcmdStatus = libpq.PQcmdStatus
PQcmdStatus.argtypes = [PGresult_p]
PQcmdStatus.restype = c_char_p

PQcmdTuples = libpq.PQcmdTuples
PQcmdTuples.argtypes = [PGresult_p]
PQcmdTuples.restype = c_char_p

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

PQuser = libpq.PQuser
PQuser.argtypes = [PGconn_p]
PQuser.restype = c_char_p

PQstatus = libpq.PQstatus
PQstatus.argtypes = [PGconn_p]
PQstatus.restype = ConnStatusType

PQtransactionStatus = libpq.PQtransactionStatus
PQtransactionStatus.argtypes = [PGconn_p]
PQtransactionStatus.restype = c_int

PQgetisnull = libpq.PQgetisnull
PQgetisnull.argtypes = [PGresult_p, c_int, c_int]
PQgetisnull.restype = c_int

PQgetlength = libpq.PQgetlength
PQgetlength.argtypes = [PGresult_p, c_int, c_int]
PQgetlength.restype = c_int

PQgetvalue = libpq.PQgetvalue
PQgetvalue.argtypes = [PGresult_p, c_int, c_int]
PQgetvalue.restype = c_char_p

PQoidValue = libpq.PQoidValue
PQoidValue.argtypes = [PGresult_p]
PQoidValue.restype = c_uint
