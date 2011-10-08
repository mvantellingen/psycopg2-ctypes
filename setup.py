# This file is almost entirely taken from psycopg2 with a couple of
# adjustments for ctypes

import os
import re
import sys
import subprocess
import ctypes.util

from setuptools import setup

from distutils.command.build_py import build_py as _build_py

PLATFORM_IS_WINDOWS = sys.platform.lower().startswith('win')


class PostgresConfig:

    def __init__(self, build_py):
        self.build_py = build_py
        self.pg_config_exe = self.build_py.pg_config
        if not self.pg_config_exe:
            self.pg_config_exe = self.autodetect_pg_config_path()
        if self.pg_config_exe is None:
            sys.stderr.write("""\
Error: pg_config executable not found.

Please add the directory containing pg_config to the PATH
or specify the full executable path with the option:

    python setup.py build_py --pg-config /path/to/pg_config install ...

or with the pg_config option in 'setup.cfg'.
""")
            sys.exit(1)

    def query(self, attr_name):
        """Spawn the pg_config executable, querying for the given config
        name, and return the printed value, sanitized. """
        try:
            pg_config_process = subprocess.Popen(
                [self.pg_config_exe, "--" + attr_name],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        except OSError:
            raise Warning("Unable to find 'pg_config' file in '%s'" %
                          self.pg_config_exe)
        pg_config_process.stdin.close()
        result = pg_config_process.stdout.readline().strip()
        if not result:
            raise Warning(pg_config_process.stderr.readline())
        if not isinstance(result, str):
            result = result.decode('ascii')
        return result

    def find_on_path(self, exename, path_directories=None):
        if not path_directories:
            path_directories = os.environ['PATH'].split(os.pathsep)
        for dir_name in path_directories:
            fullpath = os.path.join(dir_name, exename)
            if os.path.isfile(fullpath):
                return fullpath
        return None

    def autodetect_pg_config_path(self):
        """Find and return the path to the pg_config executable."""
        if PLATFORM_IS_WINDOWS:
            return self.autodetect_pg_config_path_windows()
        else:
            return self.find_on_path('pg_config')

    def autodetect_pg_config_path_windows(self):
        """Attempt several different ways of finding the pg_config
        executable on Windows, and return its full path, if found."""

        # This code only runs if they have not specified a pg_config option
        # in the config file or via the commandline.

        # First, check for pg_config.exe on the PATH, and use that if found.
        pg_config_exe = self.find_on_path('pg_config.exe')
        if pg_config_exe:
            return pg_config_exe

        # Now, try looking in the Windows Registry to find a PostgreSQL
        # installation, and infer the path from that.
        pg_config_exe = self._get_pg_config_from_registry()
        if pg_config_exe:
            return pg_config_exe

        return None

    def _get_pg_config_from_registry(self):
        try:
            import winreg
        except ImportError:
            import _winreg as winreg

        reg = winreg.ConnectRegistry(None, winreg.HKEY_LOCAL_MACHINE)
        try:
            pg_inst_list_key = winreg.OpenKey(reg,
                'SOFTWARE\\PostgreSQL\\Installations')
        except EnvironmentError:
            # No PostgreSQL installation, as best as we can tell.
            return None

        try:
            # Determine the name of the first subkey, if any:
            try:
                first_sub_key_name = winreg.EnumKey(pg_inst_list_key, 0)
            except EnvironmentError:
                return None

            pg_first_inst_key = winreg.OpenKey(reg,
                'SOFTWARE\\PostgreSQL\\Installations\\'
                + first_sub_key_name)
            try:
                pg_inst_base_dir = winreg.QueryValueEx(
                    pg_first_inst_key, 'Base Directory')[0]
            finally:
                winreg.CloseKey(pg_first_inst_key)

        finally:
            winreg.CloseKey(pg_inst_list_key)

        pg_config_path = os.path.join(
            pg_inst_base_dir, 'bin', 'pg_config.exe')
        if not os.path.exists(pg_config_path):
            return None

        # Support unicode paths, if this version of Python provides the
        # necessary infrastructure:
        if sys.version_info[0] < 3 \
        and hasattr(sys, 'getfilesystemencoding'):
            pg_config_path = pg_config_path.encode(
                sys.getfilesystemencoding())

        return pg_config_path


class build_py(_build_py):

    user_options = _build_py.user_options[:]
    user_options.extend([
        ('pg-config=', None,
         "The name of the pg_config binary and/or full path to find it"),
    ])

    def initialize_options(self):
        _build_py.initialize_options(self)
        self.pg_config = None

    def finalize_options(self):
        _build_py.finalize_options(self)
        pg_config_helper = PostgresConfig(self)
        self.libpq_path = self.find_libpq(pg_config_helper)
        self.libpq_version = self.find_version(pg_config_helper)

    def find_version(self, helper):
        try:
            # Here we take a conservative approach: we suppose that
            # *at least* PostgreSQL 7.4 is available (this is the only
            # 7.x series supported by psycopg 2)
            pgversion = helper.query('version').split()[1]
        except:
            pgversion = '7.4.0'

        verre = re.compile(
            r'(\d+)\.(\d+)(?:(?:\.(\d+))|(devel|(alpha|beta|rc)\d+))')
        m = verre.match(pgversion)
        if m:
            pgmajor, pgminor, pgpatch = m.group(1, 2, 3)
            if pgpatch is None or not pgpatch.isdigit():
                pgpatch = 0
        else:
            sys.stderr.write(
                "Error: could not determine PostgreSQL version from '%s'"
                % pgversion)
            sys.exit(1)

        return '0x%02X%02X%02X' % (int(pgmajor), int(pgminor), int(pgpatch))

    def find_libpq(self, helper):
        path = helper.query('libdir')
        fname = None
        if os.name == 'posix':
            if sys.platform == 'darwin':
                fname = os.path.join(path, 'libpq.dylib')
            if sys.platform in ['linux2', 'linux3']:
                fname = os.path.join(path, 'libpq.so')

        if fname:
            print
            print '=' * 80
            print
            print 'Found libpq at:'
            print ' -> %s' %  fname
            print
            print '=' * 80
            return fname
        else:
            fname = ctypes.util.find_library('pq')
            print
            print '=' * 80
            print
            print 'Unable to find the libpq for your platform in:'
            print ' -> %s' %  path
            print
            print 'Ignoring pg_config, trying ctypes.util.find_library()'
            if fname:
                print ' -> OK (%s)' % fname
            else:
                print ' -> FAILED'
            print
            print '=' * 80
            if not fname:
                sys.exit(1)
            return fname

    def run(self):
        if not self.dry_run:
            target_path = os.path.join(self.build_lib, 'psycopg2ct')
            self.mkpath(target_path)

            with open(os.path.join(target_path, '_config.py'), 'w') as fh:
                fh.write('# Auto-generated by setup.py\n')
                fh.write('PG_LIBRARY = "%s"\n' % self.libpq_path)
                fh.write('PG_VERSION = %s\n' % self.libpq_version)

        _build_py.run(self)

README = []
with open('README', 'r') as fh:
    README = fh.readlines()


setup(
    name='psycopg2ct',
    author='Michael van Tellingen',
    author_email='michaelvantellingen@gmail.com',
    license='LGPL',
    url='http://github.com/mvantellingen/psycopg2-ctypes',
    version='0.2.1',
    cmdclass={
        'build_py': build_py
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: SQL',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',

    ],
    platforms=['any'],
    test_suite='psycopg2ct.tests.suite',
    description=README[0].strip(),
    long_description=''.join(README),
    packages=['psycopg2ct', 'psycopg2ct.tests'],
)
