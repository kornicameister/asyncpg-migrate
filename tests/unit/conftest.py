import datetime as dt
from pathlib import Path
import typing as t

import pytest
import pytest_mock as ptm

from asyncpg_migrate import model


@pytest.fixture(
    params=[
        'relative',
        'absolute',
    ],
)
def config_env(
        tmp_path: Path,
        request: t.Any,
        config_with_migrations: t.Tuple[Path, model.Config, int],
) -> t.Tuple[Path, Path, t.Dict[str, str]]:
    script_location, _, _ = config_with_migrations
    cf = tmp_path / str(dt.datetime.utcnow())
    cf.touch(exist_ok=False)

    configuration = [
        '[migrations]',
        f'script_location = {script_location}',
        'db_user = ${postgres_user}',
        'db_password = ${postgres_password}',
        'db_host = ${postgres_host}',
        'db_port = ${postgres_port}',
        'db_name = ${postgres_database}',
    ]
    cf.write_text('\n'.join(configuration))

    environ = {
        'postgres_user': 'me',
        'postgres_password': 'strong',
        'postgres_host': 'database',
        'postgres_port': '6666',
        'postgres_database': 'internal',
    }

    cf_path = cf.absolute()
    if request.param == 'relative':
        cf_path = Path(
            '/'.join(['..' for _ in range(len(cf.absolute().parents) + 1)]) + str(cf),
        )

    return cf_path, script_location, environ


@pytest.fixture(
    params=[
        'relative',
        'absolute',
    ],
)
def config_with_migrations(
        tmp_path: Path,
        mocker: ptm.MockFixture,
        request: t.Any,
) -> t.Tuple[Path, model.Config, int]:
    migrations_count = 10
    script_location = tmp_path
    if request.param == 'relative':
        script_location = Path(
            '/'.join(['..' for _ in range(len(
                script_location.absolute().parents,
            ) + 1)]) + str(tmp_path),
        )

    files = [(script_location / f'migration_{i}.py') for i in range(migrations_count)]
    for i, f in enumerate(files):
        f.touch(exist_ok=False)
        f.write_text(
            '\n'.join([
                '',
                f'revision = {i+1}',
                '',
                'async def upgrade():',
                '    ...',
                '',
                'async def downgrade():',
                '    ...',
            ]),
        )

    return script_location, model.Config(
        script_location=tmp_path,
        database_name=mocker.stub(),
        database_dsn=mocker.stub(),
    ), migrations_count
