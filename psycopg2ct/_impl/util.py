from psycopg2ct._impl import exceptions
from psycopg2ct._impl import libpq


def pq_is_busy(conn):

    if libpq.PQconsumeInput(conn._pgconn) == 0:
        raise exceptions.OperationalError(
            libpq.PQerrorMessage(conn._pgconn))

    return libpq.PQisBusy(conn._pgconn)


def pq_set_non_blocking(pgconn, arg, raise_exception=False):
    ret = libpq.PQsetnonblocking(pgconn, arg)
    if ret != 0 and raise_exception:
        raise exceptions.OperationalError('PQsetnonblocking() failed')
    return ret


def pq_clear_async(pgconn):
    while True:
        pgres = libpq.PQgetResult(pgconn)
        if not pgres:
            break
        libpq.PQclear(pgres)


def pq_get_last_result(pgconn):
    pgres = pgres_next = None

    while True:
        pgres_next = libpq.PQgetResult(pgconn)
        if not pgres_next:
            break

        if pgres:
            libpq.PQclear(pgres)
        pgres = pgres_next
    return pgres


def escape_string(conn, value):
    from psycopg2ct.extensions import QuotedString
    obj = QuotedString(value)
    obj.prepare(conn)
    return obj.getquoted()


def create_operational_error(pgconn):
    error_msg = libpq.PQerrorMessage(pgconn)
    return exceptions.OperationalError(error_msg)


def validate_datestyle(pgconn):
    """Validates if the datestyle is an ISO format"""
    datestyle = libpq.PQparameterStatus(pgconn, 'DateStyle')

    # pgbouncer does not pass on DateStyle
    if datestyle is None:
        return False
    return datestyle.startswith('ISO')
