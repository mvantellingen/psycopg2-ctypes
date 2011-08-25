from psycopg2ct.tests.test_base import TestBase


class TestExtensions(TestBase):
    def test_adapt_subtype(self):
        from psycopg2ct.extensions import adapt

        class Sub(str):
            pass

        s1 = "hel'lo"
        s2 = Sub(s1)

        assert adapt(s1).getquoted() == adapt(s2).getquoted()

    def test_adapt_most_specific(self):
        from psycopg2ct.extensions import adapt, register_adapter, AsIs

        class A(object):
            pass
        class B(A):
            pass
        class C(B):
            pass

        register_adapter(A, lambda a: AsIs("a"))
        register_adapter(B, lambda b: AsIs("b"))
        assert adapt(C()).getquoted() == "b"

    def test_mro_required(self):
        import psycopg2ct
        from psycopg2ct.extensions import adapt, register_adapter, AsIs

        # Intentionally old-style, they don't expose their MRO.
        class A:
            pass
        class B(A):
            pass

        register_adapter(A, lambda a: AsIs("a"))
        with self.assertRaises(psycopg2ct.ProgrammingError):
            adapt(B())

    def test_register_type(self):
        import psycopg2ct

        psycopg2ct.extensions.register_type(psycopg2ct.extensions.UNICODE)
        with self.assertRaises(TypeError):
            psycopg2ct.extensions.register_type(psycopg2ct.extensions.UNICODE, 3)

    def test_register_type_connection(self):
        import psycopg2ct

        def func(val, cursor):
            return 42
        UUID = psycopg2ct.extensions.new_type((2950,), "UUID", func)

        conn = self.connect()
        psycopg2ct.extensions.register_type(UUID, conn)
        cur = conn.cursor()

        cur.execute("SELECT 'b5219e01-19ab-4994-b71e-149225dc51e4'::uuid")
        res, = cur.fetchone()
        assert res == 42

        conn.close()

    def test_register_type_cursor(self):
        import psycopg2ct

        def func(val, cursor):
            return 42
        UUID = psycopg2ct.extensions.new_type((2950,), "UUID", func)

        conn = self.connect()
        cur = conn.cursor()
        psycopg2ct.extensions.register_type(UUID, cur)

        cur.execute("SELECT 'b5219e01-19ab-4994-b71e-149225dc51e4'::uuid")
        res, = cur.fetchone()
        assert res == 42

        conn.close()

    def test_isolation_levels(self):
        import psycopg2ct

        assert psycopg2ct.extensions.ISOLATION_LEVEL_READ_COMMITTED == \
            psycopg2ct.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED
        assert psycopg2ct.extensions.ISOLATION_LEVEL_SERIALIZABLE == \
            psycopg2ct.extensions.ISOLATION_LEVEL_REPEATABLE_READ
