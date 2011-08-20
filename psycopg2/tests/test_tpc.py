from psycopg2.tests.test_base import TestBase


class TestTPC(TestBase):

    def setUp(self):
        super(TestTPC, self).setUp()

        if not hasattr(type(self), "_last_id"):
            type(self)._last_id = 0
        cur_id = self._last_id
        type(self)._last_id += 1
        self.xid = (42, "dbapi20:%s" % cur_id, "qualifier")

    def test_xid(self):
        conn = self.connect()

        xid = conn.xid(42, "global", "bqual")
        assert xid[0] == 42
        assert xid[1] == "global"
        assert xid[2] == "bqual"

        xid = conn.xid(0, "", "")
        assert tuple(xid) == (0, "", "")

        xid = conn.xid(0x7FFFFFFF, "a" * 64, "b" * 64)
        assert tuple(xid) == (0x7FFFFFFF, "a" * 64, "b" * 64)

        conn.close()

    def test_tpc_begin(self):
        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)

        conn.close()

    def test_tpc_commit_without_prepare(self):
        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.tpc_commit()

        conn.close()

    def test_tpc_rollback_without_prepare(self):
        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.tpc_rollback()

        conn.close()

    def test_tpc_commit_with_prepare(self):
        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.tpc_prepare()
        conn.tpc_commit()

        conn.close()

    def test_tpc_rollback_with_prepare(self):
        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        conn.tpc_prepare()
        conn.tpc_rollback()

        conn.close()

    def test_tpc_begin_in_transaction_fails(self):
        import psycopg2

        conn = self.connect()
        xid = conn.xid(*self.xid)

        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        with self.assertRaises(psycopg2.ProgrammingError):
            conn.tpc_begin(xid)

        conn.close()

    def test_tpc_begin_in_tpc_transaction_fails(self):
        import psycopg2

        conn = self.connect()
        xid = conn.xid(*self.xid)

        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        with self.assertRaises(psycopg2.ProgrammingError):
            conn.tpc_begin(xid)

        conn.close()

    def test_commit_in_tpc_fails(self):
        import psycopg2

        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)
        with self.assertRaises(psycopg2.ProgrammingError):
            conn.commit()

        conn.close()

    def test_rollback_in_tpc_fails(self):
        import psycopg2

        conn = self.connect()
        xid = conn.xid(*self.xid)

        conn.tpc_begin(xid)
        with self.assertRaises(psycopg2.ProgrammingError):
            conn.rollback()

        conn.close()
