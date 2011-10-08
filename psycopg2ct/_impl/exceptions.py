
class OperationError(Exception):
    pass


class Warning(StandardError):
    pass


class Error(StandardError):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class QueryCanceledError(OperationalError):
    pass


class TransactionRollbackError(OperationalError):
    pass
