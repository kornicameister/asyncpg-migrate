import datetime as dt
import secrets

import asyncpg
import pytest
import pytest_mock as ptm

from asyncpg_migrate import constants
from asyncpg_migrate import model
from asyncpg_migrate.engine import migration


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'table_schema,table_name',
    [
        (constants.MIGRATIONS_SCHEMA, constants.MIGRATIONS_TABLE),
        (constants.MIGRATIONS_SCHEMA, '_foo_'),
        (constants.MIGRATIONS_SCHEMA, 'ordinary'),
    ],
)
async def test_get_revision_no_migrations_table(
        db_dsn: str,
        db_name: str,
        table_schema: str,
        table_name: str,
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
            table_schema=table_schema,
            table_name=table_name,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'table_schema,table_name',
    [
        (constants.MIGRATIONS_SCHEMA, constants.MIGRATIONS_TABLE),
        (constants.MIGRATIONS_SCHEMA, '_foo_'),
        (constants.MIGRATIONS_SCHEMA, 'ordinary'),
    ],
)
async def test_get_revision_migration_table_exists_no_entries(
        db_dsn: str,
        db_name: str,
        table_schema: str,
        table_name: str,
        mocker: ptm.MockFixture,
) -> None:
    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )

    await migration.create_table(
        config=config,
        table_schema=table_schema,
        table_name=table_name,
    )

    assert (
        await migration.latest_revision(
            config=config,
            table_schema=table_schema,
            table_name=table_name,
        )
    ) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'table_schema,table_name',
    [
        (constants.MIGRATIONS_SCHEMA, constants.MIGRATIONS_TABLE),
        (constants.MIGRATIONS_SCHEMA, '_foo_'),
        (constants.MIGRATIONS_SCHEMA, 'ordinary'),
    ],
)
async def test_get_revision_migration_table_exists_with_entries(
        db_dsn: str,
        db_name: str,
        table_schema: str,
        table_name: str,
        mocker: ptm.MockFixture,
        db_connection: asyncpg.Connection,
) -> None:
    max_revisions = 10
    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=db_dsn,
        database_name=db_name,
    )

    await migration.create_table(
        config=config,
        table_schema=table_schema,
        table_name=table_name,
    )
    for i in range(1, max_revisions + 1):
        await db_connection.execute(
            f'insert into {table_schema}.{table_name}'
            f'(revision, label, timestamp, direction) '
            f'values ($1, $2, $3, $4)',
            i,
            __name__,
            dt.datetime.today(),
            secrets.choice([
                model.MigrationDir.DOWN,
                model.MigrationDir.UP,
            ]),
        )

    assert (
        await migration.latest_revision(
            config=config,
            table_schema=table_schema,
            table_name=table_name,
        )
    ) == max_revisions


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'table_schema,table_name',
    [
        (constants.MIGRATIONS_SCHEMA, constants.MIGRATIONS_TABLE),
        (constants.MIGRATIONS_SCHEMA, '_foo_'),
        (constants.MIGRATIONS_SCHEMA, 'ordinary'),
    ],
)
async def test_ensure_create_table(
        db_dsn: str,
        db_name: str,
        table_schema: str,
        table_name: str,
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
        table_schema=table_schema,
        table_name=table_name,
    )

    table_name_in_db = await db_connection.fetchval(
        """
        select to_regclass('{schema}.{table}')
        """.format(
            schema=table_schema,
            table=table_name,
        ),
    )
    assert table_name_in_db == table_name
