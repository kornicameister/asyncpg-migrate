import pytest
import pytest_mock as ptm

from asyncpg_migrate import constants
from asyncpg_migrate import model
from asyncpg_migrate.cmd import _helper


@pytest.mark.asyncio
async def test_ensure_migrations_table_create(mocker: ptm.MockFixture) -> None:
    config = model.Config(
        script_location=mocker.stub(),
        database_dsn='postgres://root:root@localhost:5432/asyncpg_migrate',
        database_name='asyncpg_migrate',
    )
    await _helper.migrations_table_create(config)

    import asyncpg
    c = await asyncpg.connect('postgres://root:root@localhost:5432/asyncpg_migrate')
    async with c.transaction():
        res = await c.execute((
            """
            select to_regclass('{table_schema}.{table_name}')
            """
        ).format(
            table_schema=constants.MIGRATIONS_SCHEMA,
            table_name=constants.MIGRATIONS_TABLE,
            migration_up=model.MigrationDir.UP,
            migration_down=model.MigrationDir.DOWN,
        ))
        import loguru
        loguru.logger.debug(res)
