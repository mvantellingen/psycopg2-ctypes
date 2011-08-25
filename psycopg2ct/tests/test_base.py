from unittest import TestCase

class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.database = 'psycopg2ct'

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
        cls.ddl4 = "CREATE TABLE unicode (m\xc3\xa9il SERIAL, \xe6\xb8\xac\xe8\xa9\xa6 Integer)"
        cls.xddl4 = "DROP TABLE unicode"

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
        import psycopg2ct
        return psycopg2ct.connect(self.dsn)

    def assert_roundtrips(self, cursor, value):
        cursor.execute("SELECT %s", (value,))
        rows = cursor.fetchall()
        self.assertEqual(rows, [(value,)])


class TestModule(TestBase):
    def test_version(self):
        import psycopg2ct

        assert psycopg2ct.__version__ == "2.4"

    def test_apilevel(self):
        import psycopg2ct

        assert psycopg2ct.apilevel == "2.0"

    def test_paramstyle(self):
        import psycopg2ct

        assert psycopg2ct.paramstyle == "pyformat"

    def test_exceptions(self):
        import psycopg2ct

        assert issubclass(psycopg2ct.Warning, StandardError)
        assert issubclass(psycopg2ct.Error, StandardError)

        assert issubclass(psycopg2ct.InterfaceError, psycopg2ct.Error)
        assert issubclass(psycopg2ct.DatabaseError, psycopg2ct.Error)
        assert issubclass(psycopg2ct.OperationalError, psycopg2ct.Error)
        assert issubclass(psycopg2ct.IntegrityError, psycopg2ct.Error)
        assert issubclass(psycopg2ct.InternalError, psycopg2ct.Error)
        assert issubclass(psycopg2ct.ProgrammingError, psycopg2ct.Error)
        assert issubclass(psycopg2ct.NotSupportedError, psycopg2ct.Error)

    def test_Date(self):
        import time

        import psycopg2ct

        d1 = psycopg2ct.Date(2002, 12, 25)
        d2 = psycopg2ct.DateFromTicks(time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))
