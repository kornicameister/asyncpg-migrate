import datetime as dt
import typing as t

import asyncpg
import asyncpg.exceptions
from decorator import decorator
from loguru import logger

from asyncpg_migrate import constants
from asyncpg_migrate import model


class MigrationTableMissing(Exception):
    ...


class MigrationProcessingError(Exception):
    ...


@decorator
async def error_trap(
        func: t.Callable[..., t.Awaitable[t.Any]],
        *args: t.Any,
        **kwargs: t.Any,
) -> t.Any:
    try:
        return await func(*args, **kwargs)
    except asyncpg.exceptions.UndefinedTableError as ex:
        logger.exception('Migration table is gone, you need to run migrations first')
        raise MigrationTableMissing() from ex
    except Exception as ex:
        logger.exception('Unknown error occurred')
        raise MigrationProcessingError() from ex


@error_trap
async def latest_revision(
        connection: asyncpg.Connection,
        table_schema: str = constants.MIGRATIONS_SCHEMA,
        table_name: str = constants.MIGRATIONS_TABLE,
) -> t.Optional[model.Revision]:
    await connection.reload_schema_state()
    val = await connection.fetchval(
        """
            select revision from {table_schema}.{table_name} order
            by timestamp desc limit 1;
        """.format(
            table_schema=table_schema,
            table_name=table_name,
        ),
    )
    return model.Revision(val) if val is not None else None


async def create_table(
        connection: asyncpg.Connection,
        table_schema: str = constants.MIGRATIONS_SCHEMA,
        table_name: str = constants.MIGRATIONS_TABLE,
) -> None:
    logger.opt(lazy=True).debug(
        'Creating migrations table {table_schema}.{table_name}',
        table_name=lambda: table_name,
        table_schema=lambda: table_schema,
    )

    await connection.reload_schema_state()

    async with connection.transaction():
        await connection.execute((
            """
            do $$ begin
                create type {table_schema}.{table_name}_direction as enum (
                    '{migration_up}',
                    '{migration_down}'
                );
            exception
                when duplicate_object then null;
            end $$;

            create table if not exists {table_schema}.{table_name} (
                revision integer not null,
                label text not null,
                timestamp timestamp not null,
                direction {table_schema}.{table_name}_direction not null,

                check(revision >= 0)
            );
            """
        ).format(
            table_schema=table_schema,
            table_name=table_name,
            migration_up=model.MigrationDir.UP,
            migration_down=model.MigrationDir.DOWN,
        ))


@error_trap
async def save(
        migration: model.Migration,
        direction: model.MigrationDir,
        connection: asyncpg.Connection,
        table_schema: str = constants.MIGRATIONS_SCHEMA,
        table_name: str = constants.MIGRATIONS_TABLE,
) -> None:
    await connection.execute(
        f'insert into '
        f'{table_schema}.'
        f'{table_name}'
        f' (revision, label, timestamp, direction)'
        f' values ($1, $2, $3, $4)',
        (
            migration.revision
            if direction == model.MigrationDir.UP else migration.revision - 1
        ),
        migration.label,
        dt.datetime.today(),
        direction,
    )


@error_trap
async def list(
        connection: asyncpg.Connection,
        table_schema: str = constants.MIGRATIONS_SCHEMA,
        table_name: str = constants.MIGRATIONS_TABLE,
) -> model.MigrationHistory:
    logger.debug('Getting a history of migrations')

    history = model.MigrationHistory()

    await connection.reload_schema_state()
    async with connection.transaction():
        async for record in connection.cursor("""
                select revision, label, timestamp, direction from
                    {table_schema}.{table_name}
                    order by timestamp asc;
                """.format(
                table_schema=table_schema,
                table_name=table_name,
        )):
            history.append(
                model.MigrationHistoryEntry(
                    revision=model.Revision(record['revision']),
                    label=record['label'],
                    timestamp=record['timestamp'],
                    direction=model.MigrationDir(record['direction']),
                ),
            )

    return history
