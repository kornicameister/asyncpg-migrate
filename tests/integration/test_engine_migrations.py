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
        db_connection: asyncpg.Connection,
        table_schema: str,
        table_name: str,
) -> None:
    with pytest.raises(migration.MigrationTableMissing):
        await migration.latest_revision(
            connection=db_connection,
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
        db_connection: asyncpg.Connection,
        table_schema: str,
        table_name: str,
) -> None:
    await migration.create_table(
        connection=db_connection,
        table_schema=table_schema,
        table_name=table_name,
    )

    assert (
        await migration.latest_revision(
            connection=db_connection,
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
        db_connection: asyncpg.Connection,
        table_schema: str,
        table_name: str,
        mocker: ptm.MockFixture,
) -> None:
    max_revisions = 10
    await migration.create_table(
        connection=db_connection,
        table_schema=table_schema,
        table_name=table_name,
    )
    for i in range(1, max_revisions + 1):
        await migration.save(
            connection=db_connection,
            migration=model.Migration(
                revision=model.Revision(i),
                label=__name__,
                path=mocker.stub(),
                upgrade=mocker.stub(),
                downgrade=mocker.stub(),
            ),
            direction=secrets.choice([
                model.MigrationDir.DOWN,
                model.MigrationDir.UP,
            ]),
            table_schema=table_schema,
            table_name=table_name,
        )

    assert (
        await migration.latest_revision(
            connection=db_connection,
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
        db_connection: asyncpg.Connection,
        table_schema: str,
        table_name: str,
        mocker: ptm.MockFixture,
) -> None:
    await migration.create_table(
        connection=db_connection,
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
