import sys

from setuptools import find_packages, setup

__title__ = 'asyncpg-migrate'
__author__ = 'Tomasz Trębski'
__author_email__ = 'kornicameister@gmail.com'
__maintainer__ = __author__
__url__ = 'https://github.com/kornicameister/asyncpg-migrate'

if sys.version_info < (3, 7):
    raise RuntimeError('asyncpg-migrate requires Python 3.7 or greater')

setup(
    setup_requires='setupmeta',
    python_requires='>=3.7.0',
    install_requires=[
        'asyncpg>=0.20.0',
        'click>=7.0',
        'loguru>=0.3.0',
        'tabulate>=0.8.0',
    ],
    packages=find_packages(include=['asyncpg_migrate', 'asyncpg_migrate.*']),
    extras_require={
        'uvloop': ['uvloop>=0.13.0'],
    },
    # setupmeta options
    versioning='post',
)
