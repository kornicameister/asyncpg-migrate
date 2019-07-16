from pathlib import Path
import typing as t

import pytest_mock as ptm

from asyncpg_migrate import model


def test_load_configuration_env(
        config_env: t.Tuple[Path, Path, t.Dict[str, str]],
        mocker: ptm.MockFixture,
) -> None:
    from asyncpg_migrate import loader

    mocker.patch.dict(
        'os.environ',
        config_env[2],
    )
    config = loader.load_configuration(filename=config_env[0])

    assert config.database_name == config_env[2]['postgres_database']
    assert config.database_dsn
    assert config.script_location


def test_load_migrations(
        config_with_migrations: t.Tuple[Path, model.Config, int],
) -> None:
    from asyncpg_migrate import loader

    config = config_with_migrations[1]
    migrations_count = config_with_migrations[2]

    migrations = loader.load_migrations(config)

    assert migrations
    assert len(migrations) == migrations_count
