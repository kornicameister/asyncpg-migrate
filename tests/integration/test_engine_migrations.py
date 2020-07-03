import typing as t

import asyncpg
import pytest
import pytest_mock as ptm

from asyncpg_migrate import constants
from asyncpg_migrate import model
from asyncpg_migrate.engine import downgrade
from asyncpg_migrate.engine import migration
from asyncpg_migrate.engine import upgrade


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
            direction=model.MigrationDir.UP,
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


@pytest.mark.asyncio
async def test_migration_history_no_table(db_connection: asyncpg.Connection) -> None:
    with pytest.raises(migration.MigrationTableMissing):
        await migration.list(db_connection)


@pytest.mark.asyncio
async def test_migration_history_no_revision(db_connection: asyncpg.Connection) -> None:
    await migration.create_table(db_connection)
    assert not (await migration.list(db_connection))


@pytest.mark.asyncio
async def test_migration_history_up_head(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        await upgrade.run(
            config,
            'HEAD',
            db_connection,
        )
        history = await migration.list(db_connection)
        db_rev = await migration.latest_revision(db_connection)

        assert history is not None
        assert len(history) == migrations_count

        latest_rev = history[-1]
        assert latest_rev.revision == db_rev
        assert latest_rev.direction == model.MigrationDir.UP


@pytest.mark.asyncio
async def test_migration_history_up_head_down_base(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        await upgrade.run(
            config,
            'HEAD',
            db_connection,
        )
        await downgrade.run(
            config,
            'BASE',
            db_connection,
        )

        history = await migration.list(db_connection)
        db_rev = await migration.latest_revision(db_connection)

        assert history is not None
        assert len(history) == 2 * migrations_count

        latest_rev = history[-1]
        assert latest_rev.revision == db_rev
        assert latest_rev.direction == model.MigrationDir.DOWN


@pytest.mark.asyncio
async def test_migration_history_up_head_down_1(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        await upgrade.run(
            config,
            'HEAD',
            db_connection,
        )
        await downgrade.run(
            config,
            -1,
            db_connection,
        )
        history = await migration.list(db_connection)
        db_rev = await migration.latest_revision(db_connection)

        assert history is not None
        assert len(history) == migrations_count + 1

        latest_rev = history[-1]
        assert latest_rev.revision == db_rev
        assert latest_rev.direction == model.MigrationDir.DOWN
