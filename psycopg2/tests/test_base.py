from unittest import TestCase

class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.database = 'psycopg2_test'

        cls.dsn = "dbname=%s" % cls.database
        cls.dsn += " user=postgres"
        cls.user = 'postgres'
        cls.password = ''

        cls.ddl1 = "CREATE TABLE booze (name VARCHAR(20))"
        cls.xddl1 = "DROP TABLE booze"
        cls.ddl2 = "CREATE TABLE barflys (name VARCHAR(20))"
        cls.xddl2 = "DROP TABLE barflys"
        cls.ddl3 = 'CREATE TABLE pythons ("id" serial NOT NULL PRIMARY KEY)'
        cls.xddl3 = "DROP TABLE pythons"

        cls.samples = [tuple([s]) for s in [
                'Carlton Cold', 'Carlton Draft', 'Mountain Goat', 'Redback',
                'Victoria Bitter', 'XXXX'
            ]
        ]

        cls.big_unicode_data = u"".join(map(
            unichr,
            # Ignore the surrogates.
            [u for u in xrange(1, 65536) if not 0xD800 <= u <= 0xDFFF]
        ))

    def connect(self):
        import psycopg2
        return psycopg2.connect(self.dsn)

    def assert_roundtrips(self, cursor, value):
        cursor.execute("SELECT %s", (value,))
        rows = cursor.fetchall()
        self.assertEqual(rows, [(value,)])


class TestModule(TestBase):
    def test_version(self):
        import psycopg2

        assert psycopg2.__version__ == "2.4"

    def test_apilevel(self):
        import psycopg2

        assert psycopg2.apilevel == "2.0"

    def test_paramstyle(self):
        import psycopg2

        assert psycopg2.paramstyle == "pyformat"

    def test_exceptions(self):
        import psycopg2

        assert issubclass(psycopg2.Warning, StandardError)
        assert issubclass(psycopg2.Error, StandardError)

        assert issubclass(psycopg2.InterfaceError, psycopg2.Error)
        assert issubclass(psycopg2.DatabaseError, psycopg2.Error)
        assert issubclass(psycopg2.OperationalError, psycopg2.Error)
        assert issubclass(psycopg2.IntegrityError, psycopg2.Error)
        assert issubclass(psycopg2.InternalError, psycopg2.Error)
        assert issubclass(psycopg2.ProgrammingError, psycopg2.Error)
        assert issubclass(psycopg2.NotSupportedError, psycopg2.Error)

    def test_Date(self):
        import time

        import psycopg2

        d1 = psycopg2.Date(2002, 12, 25)
        d2 = psycopg2.DateFromTicks(time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))
