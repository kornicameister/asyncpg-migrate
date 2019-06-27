import sys
import setuptools
import pathlib

if sys.version_info < (3, 5):
    raise RuntimeError('asyncpg-migrate requires Python 3.5 or greater')

_ROOT = pathlib.Path(__file__).parent
with open(str(_ROOT / 'asyncpg_migrate' / '__init__.py')) as f:
    for line in f:
        if line.startswith('__version__ ='):
            _, _, version = line.partition('=')
            VERSION = version.strip(" \n'\"")
            break
    else:
        raise RuntimeError(
            'unable to read the version from asyncpg/__init__.py')

setuptools.setup(
    name='asyncpg-migrate',
    version=VERSION,
)
