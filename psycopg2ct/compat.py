import sys
import psycopg2ct


def register():
    sys.modules['psycopg2'] = psycopg2ct

