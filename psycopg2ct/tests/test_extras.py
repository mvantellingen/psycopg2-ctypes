import uuid

from psycopg2ct.tests.test_base import TestBase


class TestExtras(TestBase):

    def test_register_uuid(self):
        import psycopg2ct.extras

        conn = self.connect()
        psycopg2ct.extras.register_uuid([2950], conn)

        cur = conn.cursor()
        cur.execute("SELECT 'b5219e01-19ab-4994-b71e-149225dc51e4'::uuid")
        res, = cur.fetchone()
        self.assertEqual(res, uuid.UUID('b5219e01-19ab-4994-b71e-149225dc51e4'))

        conn.close()


    def test_register_uuid_defaults(self):
        import psycopg2ct.extras

        conn = self.connect()
        psycopg2ct.extras.register_uuid()

        cur = conn.cursor()
        cur.execute("SELECT 'b5219e01-19ab-4994-b71e-149225dc51e4'::uuid")
        res, = cur.fetchone()
        self.assertEqual(res, uuid.UUID('b5219e01-19ab-4994-b71e-149225dc51e4'))

        conn.close()
