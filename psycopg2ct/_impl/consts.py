"""psycopg2ct -- global constants

This module can be imported from everywhere without problems of cross imports.
"""

# Isolation level values.
ISOLATION_LEVEL_AUTOCOMMIT = 0
ISOLATION_LEVEL_READ_UNCOMMITTED = 1
ISOLATION_LEVEL_READ_COMMITTED = 2
ISOLATION_LEVEL_REPEATABLE_READ = 3
ISOLATION_LEVEL_SERIALIZABLE = 4

# psycopg connection status values.
STATUS_SETUP = 0
STATUS_READY = 1
STATUS_BEGIN = 2
STATUS_SYNC = 3     # currently unused
STATUS_ASYNC = 4    # currently unused
STATUS_PREPARED = 5

# This is a usefull mnemonic to check if the connection is in a transaction
STATUS_IN_TRANSACTION = STATUS_BEGIN

# psycopg asynchronous connection polling values
POLL_OK = 0
POLL_READ = 1
POLL_WRITE = 2
POLL_ERROR = 3

# Backend transaction status values.
TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4

