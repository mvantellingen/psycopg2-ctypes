from psycopg2ct._impl.exceptions import ProgrammingError


adapters = {}


def adapt(value):
    """Return the adapter for the given value"""
    obj_type = type(value)
    try:
        return adapters[obj_type](value)
    except KeyError:
        for subtype in obj_type.mro()[1:]:
            try:
                return adapters[subtype](value)
            except KeyError:
                pass

    conform = getattr(value, '__conform__', None)
    if conform is not None:
        return conform()
    raise ProgrammingError("can't adapt type '%s'", obj_type)


def register_adapter(typ, callable):
    adapters[typ] = callable


def _getquoted(param, conn):
    """Helper method"""
    adapter = adapt(param)
    try:
        adapter.prepare(conn)
    except AttributeError:
        pass
    return adapter.getquoted()
