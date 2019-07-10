import typing as t

import pytest

from asyncpg_migrate import model
from asyncpg_migrate.engine import upgrade


@pytest.mark.asyncio
@pytest.mark.parametrize('target_revision', [
    'head',
    'base',
])
async def test_upgrade(
        migration_config: t.Tuple[model.Config, int],
        target_revision: str,
) -> None:
    config, migrations_count = migration_config
    did_steps = 0

    if migrations_count == 0:
        async for step in upgrade.run(config, target_revision):
            did_steps += step
        assert did_steps == -1
    else:
        async for step in upgrade.run(config, target_revision):
            did_steps += step
        if target_revision == 'head':
            assert did_steps == migrations_count
        elif target_revision == 'base':
            assert did_steps == 1
