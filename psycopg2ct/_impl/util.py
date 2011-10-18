from psycopg2ct._impl import exceptions
from psycopg2ct._impl import libpq
from psycopg2ct._impl.adapters import QuotedString


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
    pgres_next = None
    pgres = libpq.PQgetResult(pgconn)
    if not pgres:
        return

    while True:
        pgres_next = libpq.PQgetResult(pgconn)
        if not pgres_next:
            break

        if pgres:
            libpq.PQclear(pgres)
        pgres = pgres_next

    return pgres


def quote_string(conn, value):
    obj = QuotedString(value)
    obj.prepare(conn)
    return obj.getquoted()


def get_exception_for_sqlstate(code):
    """Translate the sqlstate to a relevant exception.

    See for a list of possible errors:
    http://www.postgresql.org/docs/current/static/errcodes-appendix.html

    """
    if code[0] == '0':
        # Class 0A - Feature Not Supported
        if code[1] == 'A':
            return exceptions.NotSupportedError

    elif code[0] == '2':
        # Class 21 - Cardinality Violation
        if code[1] == '1':
            return exceptions.ProgrammingError

        # Class 22 - Data Exception
        if code[1] == '2':
            return exceptions.DataError

        # Class 23 - Integrity Constraint Violation
        if code[1] == '3':
            return exceptions.IntegrityError

        # Class 24 - Invalid Cursor State
        # Class 25 - Invalid Transaction State
        if code[1] in '45':
            return exceptions.InternalError

        # Class 26 - Invalid SQL Statement Name
        # Class 27 - Triggered Data Change Violation
        # Class 28 - Invalid Authorization Specification
        if code[1] in '678':
            return exceptions.OperationalError

        # Class 2B - Dependent Privilege Descriptors Still Exist
        # Class 2D - Invalid Transaction Termination
        # Class 2F - SQL Routine Exception
        if code[1] in 'BDF':
            return exceptions.InternalError

    elif code[0] == '3':
        # Class 34 - Invalid Cursor Name
        if code[1] == '4':
            return exceptions.OperationalError

        # Class 38 - External Routine Exception
        # Class 39 - External Routine Invocation Exception
        # Class 3B - Savepoint Exception
        if code[1] in '89B':
            return exceptions.InternalError

        # Class 3D - Invalid Catalog Name
        # Class 3F - Invalid Schema Name
        if code[1] in 'DF':
            return exceptions.ProgrammingError

    elif code[0] == '4':
        # Class 40 - Transaction Rollback
        if code[1] == '0':
            return exceptions.TransactionRollbackError

        # Class 42 - Syntax Error or Access Rule Violation
        # Class 44 - WITH CHECK OPTION Violation
        if code[1] in '24':
            return exceptions.ProgrammingError

    elif code[0] == '5':
        if code == '57014':
            return exceptions.QueryCanceledError

        # Class 53 - Insufficient Resources
        # Class 54 - Program Limit Exceeded
        # Class 55 - Object Not In Prerequisite State
        # Class 57 - Operator Intervention
        # Class 58 - System Error (errors external to PostgreSQL itself)
        if code in '34578':
            return exceptions.OperationalError

    elif code[0] == 'F':
        # Class F0 - Configuration File Error
        return exceptions.InternalError

    elif code[0] == 'P':
        # Class P0 - PL/pgSQL Error
        return exceptions.InternalError

    elif code[0] == 'X':
        # Class XX - Internal Error
        return exceptions.InternalError

    # Fallback exception
    return exceptions.DatabaseError

