"""ctypes interface to the libpq library"""
from ctypes import *

libpq = cdll.LoadLibrary('/opt/local/lib/postgresql90/libpq.dylib')


class PGconn(Structure):
    pass

class PGresult(Structure):
    pass

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
PQconnectdb.restype = POINTER(PGconn)

PQfinish = libpq.PQfinish
PQfinish.argtypes = [POINTER(PGconn)]
PQfinish.restype = None

PQclear = libpq.PQclear
PQclear.argtypes = [POINTER(PGresult)]
PQclear.restype = None

PQfreemem = libpq.PQfreemem
PQfreemem.argtypes = [c_void_p]
PQfreemem.restype = None

PQdb = libpq.PQdb
PQdb.argtypes = [POINTER(PGconn)]
PQdb.restype = c_char_p

PQerrorMessage = libpq.PQerrorMessage
PQerrorMessage.argtypes = [POINTER(PGconn)]
PQerrorMessage.restype = c_char_p

PQparameterStatus = libpq.PQparameterStatus
PQparameterStatus.argtypes = [POINTER(PGconn), c_char_p]
PQparameterStatus.restype = c_char_p

PQescapeStringConn = libpq.PQescapeStringConn
PQescapeStringConn.restype = c_uint
PQescapeStringConn.argtypes = [POINTER(PGconn), c_char_p, c_char_p, c_uint, POINTER(c_int)]

PQescapeString = libpq.PQescapeString
PQescapeString.restype = c_uint
PQescapeString.argtypes = [c_char_p, c_char_p, c_uint]

PQescapeBytea = libpq.PQescapeBytea
PQescapeBytea.argtypes = [c_char_p, c_uint, POINTER(c_uint)]
PQescapeBytea.restype = POINTER(c_char)

PQescapeByteaConn = libpq.PQescapeByteaConn
PQescapeByteaConn.argtypes = [POINTER(PGconn), c_char_p, c_uint, POINTER(c_uint)]
PQescapeByteaConn.restype = POINTER(c_char)

PQunescapeBytea = libpq.PQunescapeBytea
PQunescapeBytea.argtypes = [POINTER(c_char), POINTER(c_uint)]
PQunescapeBytea.restype = POINTER(c_char)

PQexec = libpq.PQexec
PQexec.argtypes = [POINTER(PGconn)]
PQexec.restype = POINTER(PGresult)

PQresultStatus = libpq.PQresultStatus
PQresultStatus.argtypes = [POINTER(PGresult)]
PQresultStatus.restype = ExecStatusType

PQresultErrorMessage = libpq.PQresultErrorMessage
PQresultErrorMessage.argtypes = [POINTER(PGresult)]
PQresultErrorMessage.restype = c_char_p

PQresultErrorField = libpq.PQresultErrorField
PQresultErrorField.argtypes = [POINTER(PGresult), c_int]
PQresultErrorField.restype = c_char_p

PQsetNoticeProcessor = libpq.PQsetNoticeProcessor
PQsetNoticeProcessor.restype = PQnoticeProcessor
PQsetNoticeProcessor.argtypes = [POINTER(PGconn), PQnoticeProcessor, c_void_p]

PQcmdStatus = libpq.PQcmdStatus
PQcmdStatus.argtypes = [POINTER(PGresult)]
PQcmdStatus.restype = c_char_p

PQcmdTuples = libpq.PQcmdTuples
PQcmdTuples.argtypes = [POINTER(PGresult)]
PQcmdTuples.restype = c_char_p

PQntuples = libpq.PQntuples
PQntuples.argtypes = [POINTER(PGresult)]
PQntuples.restype = c_int

PQnfields = libpq.PQnfields
PQnfields.argtypes = [POINTER(PGresult)]
PQnfields.restype = c_int

PQfname = libpq.PQfname
PQfname.argtypes = [POINTER(PGresult)]
PQfname.restype = c_char_p

PQftype = libpq.PQftype
PQftype.argtypes = [POINTER(PGresult)]
PQftype.restype = c_uint

PQuser = libpq.PQuser
PQuser.argtypes = [POINTER(PGconn)]
PQuser.restype = c_char_p

PQstatus = libpq.PQstatus
PQstatus.argtypes = [POINTER(PGconn)]
PQstatus.restype = ConnStatusType

PQtransactionStatus = libpq.PQtransactionStatus
PQtransactionStatus.argtypes = [POINTER(PGconn)]
PQtransactionStatus.restype = c_int

PQgetisnull = libpq.PQgetisnull
PQgetisnull.argtypes = [POINTER(PGresult), c_int, c_int]
PQgetisnull.restype = c_int

PQgetlength = libpq.PQgetlength
PQgetlength.argtypes = [POINTER(PGresult), c_int, c_int]
PQgetlength.restype = c_int

PQgetvalue = libpq.PQgetvalue
PQgetvalue.argtypes = [POINTER(PGresult), c_int, c_int]
PQgetvalue.restype = c_char_p

PQoidValue = libpq.PQoidValue
PQoidValue.argtypes = [POINTER(PGresult)]
PQoidValue.restype = c_uint
