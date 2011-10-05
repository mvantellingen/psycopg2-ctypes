#!/usr/bin/env python

# psycopg2 test suite
#
# Copyright (C) 2007-2011 Federico Di Gregorio  <fog@debian.org>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

import os
import sys

from psycopg2ct.tests import bug_gc
from psycopg2ct.tests import bugX000
from psycopg2ct.tests import extras_dictcursor
from psycopg2ct.tests import test_dates
from psycopg2ct.tests import test_psycopg2_dbapi20
from psycopg2ct.tests import test_quote
from psycopg2ct.tests import test_connection
from psycopg2ct.tests import test_cursor
from psycopg2ct.tests import test_transaction
from psycopg2ct.tests import types_basic
from psycopg2ct.tests import types_extras
from psycopg2ct.tests import test_lobject
from psycopg2ct.tests import test_copy
from psycopg2ct.tests import test_notify
from psycopg2ct.tests import test_async
from psycopg2ct.tests import test_green
from psycopg2ct.tests import test_cancel
from psycopg2ct.tests.testconfig import dsn
from psycopg2ct.tests.testutils import unittest


def test_suite():
    # If connection to test db fails, bail out early.
    import psycopg2
    try:
        cnn = psycopg2.connect(dsn)
    except Exception, e:
        print "Failed connection to test db:", e.__class__.__name__, e
        print "Please set env vars 'PSYCOPG2_TESTDB*' to valid values."
        sys.exit(1)
    else:
        cnn.close()

    suite = unittest.TestSuite()
    suite.addTest(bug_gc.test_suite())
    suite.addTest(bugX000.test_suite())
    suite.addTest(extras_dictcursor.test_suite())
    suite.addTest(test_dates.test_suite())
    suite.addTest(test_psycopg2_dbapi20.test_suite())
    suite.addTest(test_quote.test_suite())
    suite.addTest(test_connection.test_suite())
    suite.addTest(test_cursor.test_suite())
    suite.addTest(test_transaction.test_suite())
    suite.addTest(types_basic.test_suite())
    suite.addTest(types_extras.test_suite())
    suite.addTest(test_lobject.test_suite())
    suite.addTest(test_copy.test_suite())
    suite.addTest(test_notify.test_suite())
    suite.addTest(test_async.test_suite())
    suite.addTest(test_green.test_suite())
    suite.addTest(test_cancel.test_suite())
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
