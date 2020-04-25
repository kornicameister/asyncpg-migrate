import secrets
import typing as t

import asyncpg
import pytest

from asyncpg_migrate import model
from asyncpg_migrate.engine import migration
from asyncpg_migrate.engine import upgrade


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'target_revision',
    [
        'head',
        'base',
    ],
)
async def test_upgrade(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
    target_revision: str,
) -> None:
    config, migrations_count = migration_config

    if not migrations_count:
        finished_revision = await upgrade.run(
            config,
            target_revision,
            db_connection,
        )

        assert finished_revision is None
        assert (await migration.latest_revision(db_connection)) is None
    elif target_revision == 'base':
        with pytest.raises(ValueError):
            await upgrade.run(
                config,
                target_revision,
                db_connection,
            )
        assert (await migration.latest_revision(db_connection)) is None
    else:
        finished_revision = await upgrade.run(
            config,
            target_revision,
            db_connection,
        )

        assert finished_revision is not None
        assert (await migration.latest_revision(db_connection)) is not None
        assert (await migration.latest_revision(db_connection)) == finished_revision


@pytest.mark.asyncio
async def test_upgrade_stepped(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        finished_revision = None
        for rev in range(1, migrations_count + 1):
            finished_revision = await upgrade.run(
                config,
                rev,
                db_connection,
            )
            assert finished_revision is not None

        assert finished_revision is not None
        assert (await migration.latest_revision(db_connection)) is not None
        assert (await migration.latest_revision(db_connection)) == finished_revision


@pytest.mark.asyncio
async def test_upgrade_skip_revision_exists(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        random_revision = secrets.randbelow(migrations_count)

        if random_revision == 0:
            random_revision = 1

        first_run_rev = await upgrade.run(
            config,
            random_revision,
            db_connection,
        )
        assert first_run_rev is not None

        second_run_rev = await upgrade.run(
            config,
            random_revision,
            db_connection,
        )
        assert second_run_rev is None

        assert (await migration.latest_revision(db_connection)) is not None
        assert (await migration.latest_revision(db_connection)) == first_run_rev


@pytest.mark.asyncio
async def test_upgrade_to_lower_revision(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        last_revision = migrations_count
        before_the_last_revision = last_revision - 1

        first_run_rev = await upgrade.run(
            config,
            last_revision,
            db_connection,
        )
        assert first_run_rev is not None

        second_run_rev = await upgrade.run(
            config,
            before_the_last_revision,
            db_connection,
        )
        assert second_run_rev is None

        assert (await migration.latest_revision(db_connection)) is not None
        assert (await migration.latest_revision(db_connection)) == first_run_rev
        assert (await migration.latest_revision(db_connection)) == last_revision
