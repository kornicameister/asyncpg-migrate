import os

import pytest
import pytest_mock as ptm

from asyncpg_migrate import constants
from asyncpg_migrate import model
from asyncpg_migrate.cmd import _helper


@pytest.mark.asyncio
async def test_ensure_migrations_table_create(mocker: ptm.MockFixture) -> None:
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')
    db_port = os.getenv('POSTGRES_PORT')

    config = model.Config(
        script_location=mocker.stub(),
        database_dsn=f'postgres://{db_user}:{db_password}@localhost:{db_port}/{db_name}',
        database_name='asyncpg_migrate',
    )

    await _helper.migrations_table_create(config)
