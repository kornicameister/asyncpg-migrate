from pathlib import Path
import sys
import typing as t

import setuptools

__title__ = 'asyncpg-migrate'
__author__ = 'Tomasz Trębski'
__author_email__ = 'kornicameister@gmail.com'
__maintainer__ = __author__
__url__ = 'https://github.com/kornicameister/asyncpg-migrate'

if sys.version_info < (3, 6):
    raise RuntimeError('asyncpg-migrate requires Python 3.6 or greater')


def read_requirements(
        path: Path,
        all_requirements: t.Optional[t.List[str]] = None,
) -> t.List[str]:
    if not all_requirements:
        all_requirements = []
    with path.open('r') as handler:
        r_l = handler.readline()
        while r_l:
            r_l = r_l.replace('\n', '').strip()
            if r_l.startswith('-r'):
                nested_file_path = Path(r_l.split('-r ')[1])
                if nested_file_path.is_absolute():
                    all_requirements.extend(read_requirements(nested_file_path))
                else:
                    all_requirements.extend(
                        read_requirements(
                            Path.cwd() / nested_file_path,
                        ),
                    )
            elif r_l:
                all_requirements.append(r_l)
            r_l = handler.readline()
    return all_requirements


setuptools.setup(
    setup_requires='setupmeta',
    # setupmeta options
    versioning='post',
    # custom overrides
    install_requires=read_requirements(Path('./requirements.txt')),
    tests_require=read_requirements(Path('./test-requirements.txt')),
)
