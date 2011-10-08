import unittest
from psycopg2ct.tests import psycopg2_tests

def suite():
    suite = unittest.TestSuite()
    suite.addTest(psycopg2_tests.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
