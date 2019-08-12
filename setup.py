import sys

from setuptools import setup

__title__ = 'asyncpg-migrate'
__author__ = 'Tomasz Trębski'
__author_email__ = 'kornicameister@gmail.com'
__maintainer__ = __author__
__url__ = 'https://github.com/kornicameister/asyncpg-migrate'

if sys.version_info < (3, 6):
    raise RuntimeError('asyncpg-migrate requires Python 3.6 or greater')

setup(
    setup_requires='setupmeta',
    python_requires='>=3.6.8',
    install_requires=[
        'asyncpg>=0.17.0',
        'click>=7.0',
        'loguru>=0.3.0',
        'tabulate>=0.8.0',
        'dataclasses>=0.6 ; python_version < "3.7"',
    ],
    extras_require={
        'uvloop': ['uvloop>=0.12.0'],
    },
    # setupmeta options
    versioning='post',
)
