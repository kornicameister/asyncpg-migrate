import sys

import setuptools

__title__ = 'asyncpg-migrate'
__author__ = 'Tomasz Trębski'
__author_email__ = 'kornicameister@gmail.com'
__maintainer__ = __author__
__url__ = 'https://github.com/kornicameister/asyncpg-migrate'

if sys.version_info < (3, 6):
    raise RuntimeError('asyncpg-migrate requires Python 3.6 or greater')

setuptools.setup(
    setup_requires='setupmeta',
    # setupmeta options
    versioning='post',
)
