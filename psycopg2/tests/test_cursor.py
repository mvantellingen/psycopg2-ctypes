from psycopg2.tests.test_base import TestBase


class TestCursor(TestBase):
    def test_cursor_isolation(self):
        conn = self.connect()
        cur1 = conn.cursor()
        cur2 = conn.cursor()

        cur1.execute(self.ddl1)
        cur1.execute("INSERT INTO booze VALUES ('Victoria Bitter')")
        cur2.execute("SELECT name FROM booze")
        booze = cur2.fetchall()
        assert booze == [('Victoria Bitter',)]

        cur = conn.cursor()
        cur.execute(self.xddl1)

        conn.close()

    def test_description(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)
        assert cur.description is None

        cur.execute("SELECT name FROM booze")

        assert len(cur.description) == 1
        assert len(cur.description[0]) == 7
        assert cur.description[0][0] == "name"
        assert cur.description[0][1] == psycopg2.STRING

        cur.execute(self.ddl2)
        self.assertEqual(cur.description, None)

        cur.execute(self.xddl1)
        cur.execute(self.xddl2)

        conn.close()

    def test_rowcount(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)
        assert cur.rowcount == -1
        cur.execute("INSERT INTO booze VALUES ('Victoria Bitter')")
        assert cur.rowcount == 1
        cur.execute("SELECT name FROM booze")
        assert cur.rowcount == 1
        cur.execute(self.ddl2)
        assert cur.rowcount == -1

        cur.execute(self.xddl1)
        cur.execute(self.xddl2)

        conn.close()

    def test_callproc(self):
        conn = self.connect()
        cur = conn.cursor()
        r = cur.callproc("lower", ("FOO",))
        assert r == ("FOO",)
        r = cur.fetchall()
        assert r == [("foo",)]

        conn.close()

    def test_close(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()
        conn.close()

        with self.assertRaises(psycopg2.Error):
            cur.execute(self.ddl1)
        with self.assertRaises(psycopg2.Error):
            conn.commit()
        with self.assertRaises(psycopg2.Error):
            conn.close()

    def test_execute(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)
        cur.execute("INSERT INTO booze VALUES ('Victoria Bitter')")
        assert cur.rowcount == 1
        cur.execute("INSERT INTO booze VALUES (%(beer)s)", {"beer": "Cooper's"})
        assert cur.rowcount == 1
        cur.execute("SELECT name FROM booze")
        res = cur.fetchall()
        assert sorted(res) == [("Cooper's",), ("Victoria Bitter",)]

        cur.execute(self.xddl1)
        conn.close()

    def test_execute_bad_format(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        with self.assertRaises(psycopg2.ProgrammingError):
            cur.execute("INSERT INTO booze VALUES (%(drink", {"drink": "foo"})

        conn.close()

    def test_execute_unicode(self):
        conn = self.connect()
        cur = conn.cursor()
        self.assertEqual(conn.encoding, 'UTF8')
        cur.execute(self.ddl4)
        cur.execute(self.xddl4)
        conn.close()

    def test_executemany(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)

        cur.executemany("INSERT INTO booze VALUES (%(beer)s)", [
            {"beer": "Cooper's"},
            {"beer": "Boag's"},
        ])
        assert cur.rowcount == 2
        cur.execute("SELECT name FROM booze")
        res = cur.fetchall()
        assert sorted(res) == [("Boag's",), ("Cooper's",)]

        cur.execute(self.xddl1)
        conn.close()

    def test_fetchone(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        with self.assertRaises(psycopg2.Error):
            cur.fetchone()
        cur.execute(self.ddl1)
        with self.assertRaises(psycopg2.Error):
            cur.fetchone()
        cur.execute("SELECT name FROM booze")
        assert cur.fetchone() is None
        assert cur.rowcount == 0
        cur.execute("INSERT INTO booze VALUES ('Victoria Bitter')")
        with self.assertRaises(psycopg2.Error):
            cur.fetchone()
        cur.execute("SELECT name FROM booze")
        r = cur.fetchone()
        assert r == ("Victoria Bitter",)
        assert cur.fetchone() is None
        assert cur.rowcount == 1

        cur.execute(self.xddl1)
        conn.close()

    def test_fetchmany(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        with self.assertRaises(psycopg2.Error):
            cur.fetchmany(4)
        cur.execute(self.ddl1)
        cur.executemany("INSERT INTO booze VALUES (%s)", self.samples)
        cur.execute("SELECT name FROM booze")
        r = cur.fetchmany()
        assert len(r) == 1
        cur.arraysize = 10
        r = cur.fetchmany(3)
        assert len(r) == 3
        r = cur.fetchmany(4)
        assert len(r) == 2
        r = cur.fetchmany(4)
        assert len(r) == 0
        assert cur.rowcount == 6

        cur.arraysize = 4
        cur.execute("SELECT name FROM booze")
        r = cur.fetchmany()
        assert len(r) == 4
        r = cur.fetchmany()
        assert len(r) == 2
        r = cur.fetchmany()
        assert len(r) == 0
        assert cur.rowcount == 6

        cur.arraysize = 6
        cur.execute("SELECT name FROM booze")
        rows = cur.fetchmany()
        assert cur.rowcount == 6
        assert sorted(rows) == self.samples
        rows = cur.fetchmany()
        assert len(rows) == 0
        assert cur.rowcount == 6

        cur.execute(self.ddl2)
        cur.execute("SELECT name FROM barflys")
        r = cur.fetchmany()
        assert len(r) == 0
        assert cur.rowcount == 0

        cur.execute(self.xddl1)
        cur.execute(self.xddl2)
        conn.close()

    def test_fetchall(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()
        with self.assertRaises(psycopg2.Error):
            cur.fetchall()
        cur.execute(self.ddl1)
        cur.executemany("INSERT INTO booze VALUES (%s)", self.samples)
        with self.assertRaises(psycopg2.Error):
            cur.fetchall()

        cur.execute("SELECT name FROM booze")
        rows = cur.fetchall()
        assert cur.rowcount == 6
        assert len(rows) == 6
        assert sorted(rows) == self.samples
        rows = cur.fetchall()
        assert len(rows) == 0
        assert cur.rowcount == 6
        cur.execute(self.ddl2)
        cur.execute("SELECT name FROM barflys")
        rows = cur.fetchall()
        assert cur.rowcount == 0
        assert len(rows) == 0

        cur.execute(self.xddl1)
        cur.execute(self.xddl2)
        conn.close()

    def test_mixedfech(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)

        cur.executemany("INSERT INTO booze VALUES (%s)", self.samples)
        cur.execute("SELECT name FROM booze")
        rows1 = cur.fetchone()
        rows23 = cur.fetchmany(2)
        rows4 = cur.fetchone()
        rows56 = cur.fetchall()
        assert cur.rowcount == 6
        assert len(rows23) == 2
        assert len(rows56) == 2
        rows = [rows1] + rows23 + [rows4] + rows56
        assert sorted(rows) == self.samples

        cur.execute(self.xddl1)
        conn.close()

    def test_arraysize(self):
        conn = self.connect()
        cur = conn.cursor()

        assert hasattr(cur, "arraysize")

        conn.close()

    def test_None(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)

        cur.execute("INSERT INTO booze VALUES (NULL)")
        cur.execute("SELECT name FROM booze")
        r = cur.fetchall()
        assert r == [(None,)]

        cur.execute(self.xddl1)
        conn.close()

    def test_quoting(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, "Quote'this\\! ''ok?''")

        conn.close()

    def test_unicode_quoting(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, u"Quote'this\\! ''ok?''")

        conn.close()

    def test_number(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, 1971)
        self.assert_roundtrips(cur, 1971L)

        conn.close()

    def test_decimal(self):
        import decimal

        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, decimal.Decimal("19.10"))

        s = (decimal.Decimal("NaN"),)
        cur.execute("SELECT %s", s)
        r = cur.fetchone()
        assert type(r[0]) is decimal.Decimal
        assert str(r[0]) == "NaN"

        s = (decimal.Decimal("infinity"),)
        cur.execute("SELECT %s", s)
        r = cur.fetchone()
        assert type(r[0]) is decimal.Decimal
        assert str(r[0]) == "NaN"

        s = (decimal.Decimal("-infinity"),)
        cur.execute("SELECT %s", s)
        r = cur.fetchone()
        assert type(r[0]) is decimal.Decimal
        assert str(r[0]) == "NaN"

        conn.close()

    def test_float_nan(self):
        conn = self.connect()
        cur = conn.cursor()

        cur.execute("SELECT %s", (float("nan"),))
        r = cur.fetchone()
        assert type(r[0]) is float
        assert str(r[0]) == "nan"

        conn.close()

    def test_float_inf(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, float("inf"))

        conn.close()

    def test_binary(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        s = "".join(map(chr, range(256)))
        b = psycopg2.Binary(s)
        cur.execute("SELECT %s::bytea", (b,))
        r = cur.fetchone()
        assert str(r[0]) == s

        conn.close()

    def test_binary_empty_string(self):
        import psycopg2

        b = psycopg2.Binary("")
        self.assertEqual(str(b), "''::bytea")

    def test_binary_roundtrip(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        s = "".join(map(chr, range(256)))
        cur.execute("SELECT %s::bytea", (psycopg2.Binary(s),))
        buf1, = cur.fetchone()
        self.assertEqual(str(buf1), s)
        cur.execute("SELECT %s::bytea", (buf1,))
        buf2, = cur.fetchone()
        self.assertEqual(str(buf2), s)

        conn.close()

    def test_array(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, [[1, 2], [3, 4]])
        self.assert_roundtrips(cur, ["one", "two", "three"])

        conn.close()

    def test_type_binary_roundtrip(self):
        conn = self.connect()
        cur = conn.cursor()

        b = buffer("".join(map(chr, range(256))))
        cur.execute("SELECT %s", (b,))
        r, = cur.fetchone()
        assert r == b
        assert type(r) is type(b)
        b = buffer("")
        cur.execute("SELECT %s", (b,))
        r, = cur.fetchone()
        assert r == b
        assert type(r) == type(b)

        conn.close()

    def test_binary_array(self):
        conn = self.connect()
        cur = conn.cursor()

        b = ([buffer("".join(map(chr, range(256))))],)
        cur.execute("SELECT %s", b)
        r = cur.fetchone()
        self.assertEqual(r, b)
        assert type(r[0][0]) is type(b[0][0])

        conn.close()

    def test_bool(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, True)
        self.assert_roundtrips(cur, False)

        conn.close()

    def test_executemany_propagate_exceptions(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)

        def gen():
            yield 1 / 0
        with self.assertRaises(ZeroDivisionError):
            cur.executemany("INSERT INTO booze VALUES (%s)", gen())

        cur.execute(self.xddl1)
        conn.close()

    def test_string_quoting(self):
        conn = self.connect()
        cur = conn.cursor()

        data = """some data with \t chars
        to escape into, 'quotes' and \\ a backslash too.
        """ + "".join(map(chr, range(1, 127)))
        self.assert_roundtrips(cur, data)

        conn.close()

    def test_binary_quoting(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        data = """some data with \000\013 binary
        stuff into, 'quotes' and \\ a backslash too.
        """ + "".join(map(chr, range(256)))

        cur.execute("SELECT %s::bytea", (psycopg2.Binary(data),))
        r, = cur.fetchone()
        self.assertEqual(str(r), data)

        conn.close()

    def test_unicode_blank(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()
        conn.set_client_encoding("UNICODE")
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        cur.execute("select %s::text", (u'',))
        r, = cur.fetchone()
        conn.close()

    def test_unicode_quoting_more(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        data = u"""some data with \t chars
        to escape into, 'quotes', \u20ac euro sign and \\ a backslash too.
        """
        data += self.big_unicode_data
        conn.set_client_encoding("UNICODE")
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        cur.execute("SELECT %s::text", (data,))
        r, = cur.fetchone()
        assert r == data

        conn.close()

    def test_tzinfo_factory(self):
        conn = self.connect()
        cur = conn.cursor()

        assert hasattr(cur, "tzinfo_factory")
        cur.tzinfo_factory
        cur.tzinfo_factory = None
        assert cur.tzinfo_factory is None

        conn.close()

    def test_datetime(self):
        from datetime import datetime

        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, datetime(2005, 7, 31, 12, 30, 45, 180))

        conn.close()

    def test_mogrity(self):
        conn = self.connect()
        cur = conn.cursor()

        assert cur.mogrify("SELECT %s, %s, %s", (None, True, False)) == "SELECT NULL, true, false"

        conn.close()

    def test_query_attr(self):
        conn = self.connect()
        cur = conn.cursor()

        assert cur.query is None
        cur.execute("SELECT %s", (1,))
        assert cur.query == "SELECT 1"

        conn.close()

    def test_integer(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl3)

        cur.execute("INSERT INTO pythons DEFAULT VALUES")
        cur.execute("SELECT id FROM pythons")
        r, = cur.fetchone()
        assert r == 1
        assert type(r) is int

        cur.execute(self.xddl3)
        conn.close()

    def test_iteration(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)

        cur.execute("INSERT INTO booze VALUES ('Rum')")
        cur.execute("INSERT INTO booze VALUES ('Tequila')")
        cur.execute("SELECT name FROM booze")
        c = iter(cur)
        r = c.next()
        assert r == ("Rum",)
        r = c.next()
        assert r == ("Tequila",)
        with self.assertRaises(StopIteration):
            c.next()

        cur.execute(self.xddl1)
        conn.close()

    def test_integrity_error(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl3)

        cur.execute("INSERT INTO pythons VALUES (3)")
        with self.assertRaises(psycopg2.IntegrityError):
            cur.execute("INSERT INTO pythons VALUES (3)")
        conn.rollback()

        # No need to run xddl3, rollback() destroys it.
        conn.close()

    def test_date(self):
        import datetime

        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, datetime.date(1990, 10, 8))

        conn.close()

    def test_long(self):
        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, 9223372036854775807L)

        conn.close()

    def test_time(self):
        import datetime

        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, datetime.time(10, 3, 14))

        conn.close()

    def test_timedelta(self):
        import datetime

        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, datetime.timedelta(-1))
        self.assert_roundtrips(cur, datetime.timedelta(days=15, seconds=5874))
        self.assert_roundtrips(cur, datetime.timedelta(414))

        conn.close()

    def test_arrays(self):
        import decimal

        conn = self.connect()
        cur = conn.cursor()

        self.assert_roundtrips(cur, [5.0])
        self.assert_roundtrips(cur, [decimal.Decimal("6.4")])

        conn.close()

    def test_rowid(self):
        conn = self.connect()
        cur = conn.cursor()

        cur.execute(self.ddl1)
        cur.execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'booze' AND relkind in ('r', 'v')")
        res, = cur.fetchone()
        assert isinstance(res, (int, long))

        cur.execute(self.xddl1)
        conn.close()

    def test_too_many_not_enough_params(self):
        import psycopg2

        conn = self.connect()
        cur = conn.cursor()

        with self.assertRaises(TypeError):
            cur.execute("SELECT %s", (1, 2))
        with self.assertRaises(IndexError):
            cur.execute( "SELECT %s, %s", (1,))

        conn.close()


    def test_percentage_in_text(self):
        import psycopg2

        tests = [
            ("select 6 %% 10", 6),
            ("select 17 %% 10", 7),
            ("select '%%'", '%'),
            ("select '%%%%'", '%%'),
            ("select '%%%%%%'", '%%%'),
            ("select 'hello %% world'", "hello % world")]

        conn = self.connect()
        cur = conn.cursor()
        for expr, result in tests:
            cur.execute(expr, {})
            value = cur.fetchone()[0]
            self.assertEqual(value, result)


    def test_connection_attr(self):
        conn = self.connect()
        cur = conn.cursor()

        assert cur.connection is conn

        conn.close()

    def test_name_attr(self):
        conn = self.connect()
        cur = conn.cursor()

        assert cur.name is None

        conn.close()


class AppTestServerSideCursor(TestBase):
    def test_name_attr(self):
        conn = self.connect()
        cur = conn.cursor("named")

        assert cur.name == "named"

        conn.close()
