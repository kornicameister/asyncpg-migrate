import datetime as dt
from pathlib import Path
import typing as t

import pytest
import pytest_mock as ptm

from asyncpg_migrate import model

EnvironValues = t.Dict[str, str]


@pytest.fixture(scope='session')
def config_env(tmp_path_factory: t.Any) -> t.Tuple[Path, EnvironValues]:
    tmp_path = tmp_path_factory.mktemp(__name__)
    cf = tmp_path / str(dt.datetime.utcnow())
    cf.touch(exist_ok=False)

    configuration = [
        '[migrations]',
        'script_location = funky_service/migrations',
        'db_user = ${postgres_user}',
        'db_password = ${postgres_password}',
        'db_host = ${postgres_host}',
        'db_port = ${postgres_port}',
        'db_name = ${postgres_database}',
    ]
    cf.write_text('\n'.join(configuration))

    environ: EnvironValues = {
        'postgres_user': 'me',
        'postgres_password': 'strong',
        'postgres_host': 'database',
        'postgres_port': '6666',
        'postgres_database': 'internal',
    }

    return cf.absolute(), environ


@pytest.fixture
def config_with_migrations(
        tmp_path: t.Any,
        mocker: ptm.MockFixture,
) -> t.Tuple[model.Config, int]:
    migrations_count = 10
    files = [(tmp_path / f'migration_{i}.py') for i in range(migrations_count)]
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

    return model.Config(
        script_location=tmp_path,
        database_name=mocker.stub(),
        database_dsn=mocker.stub(),
    ), migrations_count


def test_load_configuration_env(
        config_env: t.Tuple[Path, EnvironValues],
        mocker: ptm.MockFixture,
) -> None:
    from asyncpg_migrate import loader

    mocker.patch.dict(
        'os.environ',
        config_env[1],
    )
    config = loader.load_configuration(
        cwd=config_env[0].parent,
        filename=config_env[0].name,
    )

    assert config.database_name == config_env[1]['postgres_database']
    assert config.database_dsn
    assert config.script_location


def test_load_migrations(config_with_migrations: t.Tuple[model.Config, int]) -> None:
    from asyncpg_migrate import loader

    config = config_with_migrations[0]
    migrations_count = config_with_migrations[1]

    migrations = loader.load_migrations(config)

    assert migrations
    assert len(migrations) == migrations_count
