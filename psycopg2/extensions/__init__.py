import decimal

from psycopg2.extensions.adapters import *
from psycopg2.extensions.types import *


ISOLATION_LEVEL_AUTOCOMMIT = 0
ISOLATION_LEVEL_READ_COMMITTED = ISOLATION_LEVEL_READ_UNCOMMITTED = 1
ISOLATION_LEVEL_SERIALIZABLE = ISOLATION_LEVEL_REPEATABLE_READ = 2


TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4

# Register default adapters
register_adapter(type(None), NoneAdapter)
register_adapter(str, QuotedString)
register_adapter(unicode, QuotedString)
register_adapter(int, AsIs)
register_adapter(long, AsIs)
register_adapter(float, Float)
register_adapter(bool, Boolean)
register_adapter(buffer, Binary)
register_adapter(list, List)

register_adapter(datetime.datetime, DateTime)
register_adapter(datetime.date, DateTime)
register_adapter(datetime.time, DateTime)
register_adapter(datetime.timedelta, DateTime)
register_adapter(decimal.Decimal, Decimal)

def _reg_type(name, oids, caster):
    type_obj = Type(name, oids, caster)
    register_type(type_obj)
    return type_obj



# Register the default typecasters
STRING = _reg_type('STRING', [19, 18, 25, 1042, 1043], cast_string)
NUMBER = _reg_type('NUMBER', [20, 33, 21, 701, 700, 1700], cast_float)
LONGINTEGER = _reg_type('LONGINTEGER', [20], cast_longinteger)
INTEGER = _reg_type('INTEGER', [23, 21], cast_integer)
FLOAT = _reg_type('FLOAT', [701, 700], cast_float)
DECIMAL = _reg_type('DECIMAL', [1700], cast_decimal)
BOOLEAN = _reg_type('BOOLEAN', [16], cast_boolean)
BINARY = _reg_type('BINARY', [17], cast_binary)
ROWID = _reg_type('ROWID', [26], cast_integer)


INTEGERARRAY = _reg_type('INTEGERARRAY', [1005, 1006, 1007],
    cast_generic_array(INTEGER))
FLOATARRAY = _reg_type('FLOATARRAY', [1017, 1021, 1022],
    cast_generic_array(FLOAT))
DECIMALARRAY = _reg_type('DECIMALARRAY', [1231],
    cast_generic_array(DECIMAL))
STRINGARRAY = _reg_type('STRINGARRAY', [1002, 1003, 1009, 1014, 1015],
    cast_generic_array(STRING))
BINARYARRAY = _reg_type('BINARYARRAY', [1001],
    cast_generic_array(BINARY))

DATETIME = _reg_type('DATETIME',  [1114, 1184, 704, 1186], cast_datetime)
DATE = _reg_type('DATE', [1082], cast_date)
TIME = _reg_type('TIME', [1083, 1266], cast_time)
LOINTERVALNGINTEGER = _reg_type('INTERVAL', [704, 1186], cast_interval)
UNICODE = _reg_type('UNICODE', [19, 18, 25, 1042, 1043], cast_unicode)

