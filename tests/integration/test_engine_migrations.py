import datetime as dt

import asyncpg
import pytest
import pytest_mock as ptm

from asyncpg_migrate import constants
from asyncpg_migrate import model
from asyncpg_migrate.engine import migration


@pytest.mark.asyncio
async def test_get_revision_no_migrations_table(
        db_dsn: str,
        db_name: str,
        mocker: ptm.MockFixture,
) -> None:
    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )
    with pytest.raises(migration.MigrationTableMissing):
        await migration.latest_revision(
            config=config,
            table_schema=constants.MIGRATIONS_SCHEMA,
            table_name=constants.MIGRATIONS_TABLE,
        )


@pytest.mark.asyncio
async def test_get_revision_migration_table_exists_no_entries(
        db_dsn: str,
        db_name: str,
        mocker: ptm.MockFixture,
) -> None:
    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )

    await migration.create_table(
        config=config,
        table_schema=constants.MIGRATIONS_SCHEMA,
        table_name=constants.MIGRATIONS_TABLE,
    )

    assert (
        await migration.latest_revision(
            config=config,
            table_schema=constants.MIGRATIONS_SCHEMA,
            table_name=constants.MIGRATIONS_TABLE,
        )
    ) is None


@pytest.mark.asyncio
async def test_get_revision_migration_table_exists_with_entries(
        db_dsn: str,
        db_name: str,
        mocker: ptm.MockFixture,
        db_connection: asyncpg.Connection,
) -> None:
    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )

    await migration.create_table(
        config=config,
        table_schema=constants.MIGRATIONS_SCHEMA,
        table_name=constants.MIGRATIONS_TABLE,
    )
    await db_connection.execute(
        f'insert into {constants.MIGRATIONS_SCHEMA}.{constants.MIGRATIONS_TABLE}'
        f'(revision, label, timestamp, direction) '
        f'values ($1, $2, $3, $4)',
        10,
        __name__,
        dt.datetime.today(),
        model.MigrationDir.DOWN,
    )

    assert (
        await migration.latest_revision(
            config=config,
            table_schema=constants.MIGRATIONS_SCHEMA,
            table_name=constants.MIGRATIONS_TABLE,
        )
    ) == 10


@pytest.mark.asyncio
async def test_ensure_create_table(
        db_dsn: str,
        db_name: str,
        db_connection: asyncpg.Connection,
        mocker: ptm.MockFixture,
) -> None:

    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )

    await migration.create_table(
        config=config,
        table_schema=constants.MIGRATIONS_SCHEMA,
        table_name=constants.MIGRATIONS_TABLE,
    )

    table_name_in_db = await db_connection.fetchval(
        """
        select to_regclass('{schema}.{table}')
        """.format(
            schema=constants.MIGRATIONS_SCHEMA,
            table=constants.MIGRATIONS_TABLE,
        ),
    )
    assert table_name_in_db == constants.MIGRATIONS_TABLE
