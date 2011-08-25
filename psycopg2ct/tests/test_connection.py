from psycopg2ct.tests.test_base import TestBase

class TestConnection(TestBase):
    def test_connect(self):
        import psycopg2ct

        conn = psycopg2ct.connect(
            database=self.database, user=self.user, password=self.password)
        conn.close()

        conn = psycopg2ct.connect(
            database=self.database, user=self.user, password=self.password,
            port=5432)
        conn.close()

        conn = psycopg2ct.connect(
            database=self.database, user=self.user, password=self.password,
            port="5432")
        conn.close()

        with self.assertRaises(TypeError):
            psycopg2ct.connect(database=self.database, user=self.user,
                password=self.password, port=object())

    def test_closed_attr(self):
        conn = self.connect()

        assert not conn.closed
        conn.close()
        assert conn.closed

    def test_cursor_closed_attr(self):
        conn = self.connect()

        cursor = conn.cursor()
        assert not cursor.closed
        cursor.close()
        assert cursor.closed

        cursor = conn.cursor()
        conn.close()
        # Closing the connection closes the cursor.
        assert cursor.closed

    def test_commit(self):
        conn = self.connect()
        conn.commit()
        conn.close()

    def test_rollback(self):
        conn = self.connect()
        conn.rollback()
        conn.close()

    def test_reset(self):
        conn = self.connect()
        level = conn.isolation_level
        conn.set_isolation_level(0)
        assert conn.isolation_level == 0
        conn.reset()
        assert conn.isolation_level == level
        conn.close()

    def test_isolation_level(self):
        import psycopg2ct

        conn = self.connect()
        assert conn.isolation_level == psycopg2ct.extensions.ISOLATION_LEVEL_READ_COMMITTED

    def test_set_isolation_level(self):
        import psycopg2ct

        conn = self.connect()

        for isolation_level in [
            psycopg2ct.extensions.ISOLATION_LEVEL_AUTOCOMMIT,
            psycopg2ct.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            psycopg2ct.extensions.ISOLATION_LEVEL_SERIALIZABLE,
        ]:
            conn.set_isolation_level(isolation_level)
            assert conn.isolation_level == isolation_level

        with self.assertRaises(ValueError):
            conn.set_isolation_level(-1)
        with self.assertRaises(ValueError):
            conn.set_isolation_level(3)

        conn.close()

    def test_set_isolation_abort(self):
        import psycopg2ct

        conn = self.connect()
        cur = conn.cursor()
        cur.execute(self.ddl1)
        conn.commit()

        ext = psycopg2ct.extensions
        self.assertEqual(conn.get_transaction_status(), ext.TRANSACTION_STATUS_IDLE)

        cur.execute("INSERT INTO booze VALUES ('Rum')")
        assert conn.get_transaction_status() == ext.TRANSACTION_STATUS_INTRANS

        conn.set_isolation_level(ext.ISOLATION_LEVEL_SERIALIZABLE)
        assert conn.get_transaction_status() == ext.TRANSACTION_STATUS_IDLE

        cur.execute("SELECT COUNT(*) FROM booze")
        r, = cur.fetchone()
        assert r == 0

        cur.execute("INSERT INTO booze VALUES ('Rum')")
        assert conn.get_transaction_status() == ext.TRANSACTION_STATUS_INTRANS

        conn.set_isolation_level(ext.ISOLATION_LEVEL_AUTOCOMMIT)
        assert conn.get_transaction_status() == ext.TRANSACTION_STATUS_IDLE

        cur.execute("SELECT COUNT(*) FROM booze")
        r, = cur.fetchone()
        assert r == 0

        cur.execute("INSERT INTO booze VALUES ('Rum')")
        assert conn.get_transaction_status() == ext.TRANSACTION_STATUS_IDLE

        conn.set_isolation_level(psycopg2ct.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        assert conn.get_transaction_status() == ext.TRANSACTION_STATUS_IDLE

        cur.execute("SELECT COUNT(*) FROM booze")
        r, = cur.fetchone()
        assert r == 1

        cur.execute(self.xddl1)
        conn.commit()
        conn.close()

    def test_isolation_level_autocommit(self):
        import psycopg2ct

        conn1 = self.connect()
        conn2 = self.connect()
        conn2.set_isolation_level(psycopg2ct.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn1.cursor()
        cur.execute(self.ddl1)
        conn1.commit()

        cur1 = conn1.cursor()
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 0
        conn1.commit()
        cur2 = conn2.cursor()
        cur2.execute("INSERT INTO booze VALUES ('Champagne')")
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 1

        cur.execute(self.xddl1)
        conn1.commit()
        conn1.close()
        conn2.close()

    def test_isolation_level_read_committed(self):
        import psycopg2ct

        conn1 = self.connect()
        conn2 = self.connect()
        conn2.set_isolation_level(psycopg2ct.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        cur = conn1.cursor()
        cur.execute(self.ddl1)
        conn1.commit()

        cur1 = conn1.cursor()
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 0
        conn1.commit()
        cur2 = conn2.cursor()
        cur2.execute("INSERT INTO booze VALUES ('Wine')")
        cur1.execute("INSERT INTO booze VALUES ('Beer')")
        cur2.execute("SELECT COUNT(*) FROM booze")
        r, = cur2.fetchone()
        assert r == 1
        conn1.commit()
        cur2.execute("SELECT COUNT(*) FROM booze")
        r, = cur2.fetchone()
        assert r == 2
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 1
        conn2.commit()
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 2

        cur.execute(self.xddl1)
        conn1.commit()
        conn1.close()
        conn2.close()

    def test_isolation_level_serializable(self):
        import psycopg2ct

        conn1 = self.connect()
        conn2 = self.connect()
        conn2.set_isolation_level(psycopg2ct.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        cur = conn1.cursor()
        cur.execute(self.ddl1)
        conn1.commit()

        cur1 = conn1.cursor()
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 0
        conn1.commit()
        cur2 = conn2.cursor()
        cur2.execute("INSERT INTO booze VALUES ('Whiskey')")
        cur1.execute("INSERT INTO booze VALUES ('Scotch')")
        cur2.execute("SELECT COUNT(*) FROM booze")
        r, = cur2.fetchone()
        assert r == 1
        conn1.commit()
        cur2.execute("SELECT COUNT(*) FROM booze")
        r, = cur2.fetchone()
        assert r == 1
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 1
        conn2.commit()
        cur1.execute("SELECT COUNT(*) FROM booze")
        r, = cur1.fetchone()
        assert r == 2
        cur2.execute("SELECT COUNT(*) FROM booze")
        r, = cur2.fetchone()
        assert r == 2

        conn1.commit()
        conn2.commit()
        cur.execute(self.xddl1)
        conn1.commit()
        conn1.close()
        conn2.close()


    def test_notices(self):
        conn = self.connect()
        cur = conn.cursor()

        cur.execute("CREATE TEMP TABLE chatty (id SERIAL PRIMARY KEY)")
        assert cur.statusmessage == "CREATE TABLE"
        assert conn.notices

        conn.close()

    def test_notices_consistent_order(self):
        conn = self.connect()
        cur = conn.cursor()

        cur.execute("CREATE TEMP TABLE table1 (id SERIAL); CREATE TEMP TABLE table2 (id SERIAL);")
        cur.execute("CREATE TEMP TABLE table3 (id SERIAL); CREATE TEMP TABLE table4 (id SERIAL);")
        assert len(conn.notices) == 4
        assert "table1" in conn.notices[0]
        assert "table2" in conn.notices[1]
        assert "table3" in conn.notices[2]
        assert "table4" in conn.notices[3]

        conn.close()

    def test_notices_limited(self):
        conn = self.connect()
        cur = conn.cursor()

        for i in xrange(0, 100, 10):
            sql = " ".join(
                "CREATE TEMP TABLE table%d (id SERIAL);" % j
                for j in xrange(i, i+10)
            )
            cur.execute(sql)
        assert len(conn.notices) == 50, len(conn.notices)
        assert "table50" in conn.notices[0]
        assert "table51" in conn.notices[1]
        assert "table98" in conn.notices[-2]
        assert "table99" in conn.notices[-1]

        conn.close()
