import typing as t

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
        target_revision: str,
) -> None:
    config, migrations_count = migration_config
    did_steps = 0

    if not migrations_count:
        async for step in upgrade.run(
                config,
                target_revision,
        ):
            did_steps += step
        assert did_steps == 0
        assert (await migration.latest_revision(config=config)) is None
    else:
        async for step in upgrade.run(
                config,
                target_revision,
        ):
            did_steps += step
        if target_revision == 'head':
            assert did_steps == migrations_count
        elif target_revision == 'base':
            assert did_steps == 1

        assert (await migration.latest_revision(config=config)) is not None
        assert (await migration.latest_revision(config=config)) == did_steps


@pytest.mark.asyncio
async def test_upgrade_stepped(migration_config: t.Tuple[model.Config, int]) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        for rev in range(1, migrations_count + 1):
            async for _ in upgrade.run(
                    config,
                    rev,
            ):
                ...
        assert (await migration.latest_revision(config=config)) is not None
        assert (await migration.latest_revision(config=config)) == rev
