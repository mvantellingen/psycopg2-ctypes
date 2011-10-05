import sys, os.path

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from psycopg2ct import compat

compat.register()

# Hack to make py.test and nose work
if hasattr(sys, 'modules'):
    sys.modules['psycopg2ct.tests.psycopg2_tests.psycopg2'] = \
        sys.modules['psycopg2']
