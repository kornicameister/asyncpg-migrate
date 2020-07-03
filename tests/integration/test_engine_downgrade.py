import typing as t

import asyncpg
import pytest

from asyncpg_migrate import model
from asyncpg_migrate.engine import downgrade
from asyncpg_migrate.engine import migration
from asyncpg_migrate.engine import upgrade


@pytest.mark.asyncio
async def test_downgrade_no_revision(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
) -> None:
    config, _ = migration_config
    finished_revision = await downgrade.run(
        config,
        'base',
        db_connection,
    )

    assert finished_revision is None
    assert (await migration.latest_revision(db_connection)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'target_revision',
    [
        'head',
        'base',
    ],
)
async def test_downgrade(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
    target_revision: str,
) -> None:
    config, migrations_count = migration_config

    if not migrations_count:
        finished_revision = await downgrade.run(
            config,
            target_revision,
            db_connection,
        )
        assert finished_revision is None
    else:
        finished_revision = None

        await upgrade.run(
            config,
            'HEAD',
            db_connection,
        )

        if target_revision == 'head':
            with pytest.raises(ValueError):
                finished_revision = await downgrade.run(
                    config,
                    target_revision,
                    db_connection,
                )
            assert finished_revision is None
        else:
            finished_revision = await downgrade.run(
                config,
                target_revision,
                db_connection,
            )
            db_revision = await migration.latest_revision(db_connection)

            assert finished_revision is not None
            assert db_revision is not None
            assert db_revision == finished_revision


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'target_revision',
    [
        'base',
        1,
    ],
)
async def test_downgrade_repeat_no_action_taken(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
    target_revision: t.Union[str, int],
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        await upgrade.run(
            config,
            'HEAD',
            db_connection,
        )

        first_db_revision = await downgrade.run(
            config,
            target_revision,
            db_connection,
        )
        second_db_revision = await downgrade.run(
            config,
            target_revision,
            db_connection,
        )

        assert first_db_revision is not None
        assert second_db_revision is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'step_revision',
    [
        -1,
        -2,
        -3,
        -4,
        -10,
    ],
)
async def test_downgrade_stepped(
    migration_config: t.Tuple[model.Config, int],
    db_connection: asyncpg.Connection,
    step_revision: int,
) -> None:
    config, migrations_count = migration_config
    if migrations_count:
        await upgrade.run(
            config,
            'HEAD',
            db_connection,
        )

        while migrations_count > 0:
            await downgrade.run(
                config,
                step_revision,
                db_connection,
            )
            migrations_count -= 1

        assert (await migration.latest_revision(db_connection)) is not None
        assert (await migration.latest_revision(db_connection)) == 0
