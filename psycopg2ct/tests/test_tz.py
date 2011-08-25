from psycopg2ct.tests.test_base import TestBase


class TestTZ(TestBase):
    def test_datetime_tz_roundtrip(self):
        import datetime

        import psycopg2ct

        conn = psycopg2ct.connect(self.dsn)
        cur = conn.cursor()

        tz = psycopg2ct.tz.FixedOffsetTimezone(8 * 60)
        d = (datetime.datetime(2010, 05, 03, 10, 20, 30, tzinfo=tz),)
        cur.execute("SELECT %s", d)
        r = cur.fetchone()
        assert d == r
        assert d[0].tzinfo

        conn.close()
