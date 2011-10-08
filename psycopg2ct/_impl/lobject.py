import os
from functools import wraps

from psycopg2ct._impl import exceptions
from psycopg2ct._impl import consts
from psycopg2ct._impl import libpq
from psycopg2ct._impl import util

INV_WRITE = 0x00020000
INV_READ = 0x00040000


def check_unmarked(func):
    @wraps(func)
    def check_unmarked_(self, *args, **kwargs):
        if self._mark != self._conn._mark:
            raise exceptions.ProgrammingError("lobject isn't valid anymore")
        return func(self, *args, **kwargs)
    return check_unmarked_


def check_closed(func):
    @wraps(func)
    def check_closed_(self, *args, **kwargs):
        if self.closed:
            raise exceptions.InterfaceError("lobject already closed")
        return func(self, *args, **kwargs)
    return check_closed_


class LargeObject(object):
    def __init__(self, conn=None, oid=0, mode='', new_oid=0, new_file=None):
        self._conn = conn
        self._oid = oid
        self._mode = self._parse_mode(mode)
        self._smode = mode
        self._new_oid = new_oid
        self._new_file = new_file
        self._fd = -1
        self._mark = conn._mark

        if conn.autocommit:
            raise exceptions.ProgrammingError(
                "can't use a lobject outside of transactions")
        self._open()

    @property
    def oid(self):
        return self._oid

    @property
    def mode(self):
        return self._smode

    @check_closed
    @check_unmarked
    def read(self, size=-1):
        """Read at most size bytes or to the end of the large object."""
        if size < 0:
            where = self.tell()
            end = self.seek(0, os.SEEK_END)
            self.seek(where, os.SEEK_SET)
            size = end - where

        if size == 0:
            return ''

        buf = libpq.create_string_buffer('\0', size)
        length = libpq.lo_read(self._conn._pgconn, self._fd, buf, size)
        if length < 0:
            return

        if self._mode & consts.LOBJECT_BINARY:
            return buf.raw
        else:
            return buf.value.decode(self._conn._py_enc)

    @check_closed
    @check_unmarked
    def write(self, value):
        """Write a string to the large object."""
        if isinstance(value, unicode):
            value = value.encode(self._conn._py_enc)
        length = libpq.lo_write(
            self._conn._pgconn, self._fd, value, len(value))
        if length < 0:
            raise self._conn._create_exception()
        return length

    def export(self, file_name):
        """Export large object to given file."""
        self._conn._begin_transaction()
        if libpq.lo_export(self._conn._pgconn, self._oid, file_name) < 0:
            raise self._conn._create_exception()

    @check_closed
    @check_unmarked
    def seek(self, offset, whence=0):
        """Set the lobject's current position."""
        return libpq.lo_lseek(self._conn._pgconn, self._fd, offset, whence)

    @check_closed
    @check_unmarked
    def tell(self):
        """Return the lobject's current position."""
        return libpq.lo_tell(self._conn._pgconn, self._fd)

    @check_closed
    @check_unmarked
    def truncate(self, length=0):
        ret = libpq.lo_truncate(self._conn._pgconn, self._fd, length)
        if ret < 0:
            raise self._conn._create_exception()
        return ret

    def close(self):
        """Close and then remove the lobject."""
        if self.closed:
            return True
        if self._conn.autocommit or self._conn._mark != self._mark:
            return True

        ret = libpq.lo_close(self._conn._pgconn, self._fd)
        self._fd = -1
        if ret < 0:
            raise self._conn._create_exception()
        else:
            return True

    @property
    def closed(self):
        return self._fd < 0 or not self._conn or self._conn.closed

    def unlink(self):
        self._conn._begin_transaction()
        self.close()
        libpq.lo_unlink(self._conn._pgconn, self._oid)

    def _open(self):
        conn = self._conn

        conn._begin_transaction()

        if self._oid == 0:
            if self._new_file:
                self._oid = libpq.lo_import(conn._pgconn, self._new_file)
            else:
                self._oid = libpq.lo_create(conn._pgconn, self._new_oid)

            self._mode = \
                (self._mode & ~consts.LOBJECT_READ) | consts.LOBJECT_WRITE

        pgmode = 0
        if self._mode & consts.LOBJECT_READ:
            pgmode |= INV_READ
        if self._mode & consts.LOBJECT_WRITE:
            pgmode |= INV_WRITE

        if pgmode:
            self._fd = libpq.lo_open(conn._pgconn, self._oid, pgmode)
            if self._fd < 0:
                raise self._conn._create_exception()

        self._smode = self._unparse_mode(self._mode)

    def _parse_mode(self, smode):
        """Convert a mode string to a mode int"""
        mode = 0
        pos = 0

        if not smode:
            return consts.LOBJECT_READ | consts.LOBJECT_BINARY

        if smode[0:2] == 'rw':
            mode |= consts.LOBJECT_READ | consts.LOBJECT_WRITE
            pos = 2
        else:
            if smode[0] == 'r':
                mode |= consts.LOBJECT_READ
                pos = 1
            elif smode[0] == 'w':
                mode |= consts.LOBJECT_WRITE
                pos = 1
            elif smode[0] == 'n':
                pos = 1
            else:
                mode |= consts.LOBJECT_READ

        if len(smode) > pos:
            if smode[pos] == 't':
                mode |= consts.LOBJECT_TEXT
                pos += 1
            elif smode[pos] == 'b':
                mode |= consts.LOBJECT_BINARY
                pos += 1
            else:
                mode |= consts.LOBJECT_BINARY
        else:
            mode |= consts.LOBJECT_BINARY

        if len(smode) != pos:
            raise ValueError("bad mode for lobject: '%s'", smode)
        return mode

    def _unparse_mode(self, mode):
        """Convert a mode int to a mode string"""
        smode = ''
        if mode & consts.LOBJECT_READ:
            smode += 'r'
        if mode & consts.LOBJECT_WRITE:
            smode += 'w'
        if not smode:
            smode += 'n'

        if mode & consts.LOBJECT_TEXT:
            smode += 't'
        else:
            smode += 'b'
        return smode
