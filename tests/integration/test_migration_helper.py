import os

import asyncpg
import pytest
import pytest_mock as ptm

from asyncpg_migrate import constants
from asyncpg_migrate import model
from asyncpg_migrate.commands import _helper


@pytest.fixture(scope='session')
def db_name() -> str:
    db_name = os.getenv('POSTGRES_DB')
    assert db_name, 'db_name not set'
    return db_name


@pytest.fixture(scope='session')
def db_dsn(db_name: str) -> str:
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_port = os.getenv('POSTGRES_PORT')
    db_host = os.getenv('POSTGRES_HOST')

    assert db_user, 'db_user not set'
    assert db_password, 'db_password not set'
    assert db_port, 'db_port not set'
    assert db_host, 'db_host not set'

    return f'postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'


@pytest.mark.asyncio
async def test_ensure_migrations_table_create(
        db_dsn: str,
        db_name: str,
        mocker: ptm.MockFixture,
) -> None:

    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )

    await _helper.migrations_table_create(
        config=config,
        migrations_table_schema=constants.MIGRATIONS_SCHEMA,
        migrations_table_name=constants.MIGRATIONS_TABLE,
    )

    con = await asyncpg.connect(dsn=db_dsn)
    table_name_in_db = await con.fetchval(
        """
        select to_regclass('{schema}.{table}')
        """.format(
            schema=constants.MIGRATIONS_SCHEMA,
            table=constants.MIGRATIONS_TABLE,
        ),
    )
    assert table_name_in_db == constants.MIGRATIONS_TABLE
