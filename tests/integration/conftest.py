import os
import typing as t

import asyncpg
import pytest

from asyncpg_migrate import constants
from asyncpg_migrate import model


@pytest.fixture(scope='session')
def db_name() -> str:
    return os.getenv('POSTGRES_DB', 'test')


@pytest.fixture(scope='session')
def db_dsn(db_name: str) -> str:
    db_user = os.getenv('POSTGRES_USER', 'test')
    db_password = os.getenv('POSTGRES_PASSWORD', 'test')
    db_port = os.getenv('POSTGRES_PORT', 5432)
    db_host = os.getenv('POSTGRES_HOST', 'postgres')
    if bool(os.getenv('CI', False)):
        db_host = 'localhost'
    return f'postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'


@pytest.mark.asyncio
@pytest.fixture(scope='function')
async def db_connection(db_dsn: str) -> asyncpg.Connection:
    return await asyncpg.connect(dsn=db_dsn)


@pytest.mark.asyncio
@pytest.fixture(autouse=True)
async def clean_db(db_connection: asyncpg.Connection) -> t.AsyncGenerator[None, None]:
    await db_connection.execute(
        f'create schema if not exists {constants.MIGRATIONS_SCHEMA}',
    )
    yield
    await db_connection.execute(
        f'drop schema if exists {constants.MIGRATIONS_SCHEMA} cascade',
    )


@pytest.fixture(
    scope='session',
    params=[
        0,
        1,
        5,
    ],
)
def migration_config(
        db_name: str,
        db_dsn: str,
        tmp_path_factory: t.Any,
        request: t.Any,
) -> t.Tuple[model.Config, int]:
    tmp_path = tmp_path_factory.mktemp(__name__)
    migrations_count = request.param

    files = [(tmp_path / f'migration_{i}.py') for i in range(migrations_count)]

    for i, f in enumerate(files):
        f.touch(exist_ok=False)
        f.write_text(
            """
import asyncpg

revision={revision}

async def upgrade(c: asyncpg.Connection) -> None:
    await c.execute(
        '''
        create table table_{revision}
        (
            id uuid not null,
            field_a text not null,
            field_b text not null
        );
        ''',
    )


async def downgrade(c: asyncpg.Connection) -> None:
    await c.execute('drop table table_{revision};')

            """.format(revision=i + 1),
        )

    return model.Config(
        script_location=tmp_path,
        database_name=db_name,
        database_dsn=db_dsn,
    ), migrations_count
