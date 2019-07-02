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
