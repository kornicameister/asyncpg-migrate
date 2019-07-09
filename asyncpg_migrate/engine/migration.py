import typing as t

import asyncpg
from loguru import logger

from asyncpg_migrate import model


class MigrationTableMissing(Exception):
    ...


class MigrationProcessingError(Exception):
    ...


async def latest_revision(
        config: model.Config,
        table_schema: str,
        table_name: str,
) -> t.Optional[model.Revision]:
    try:
        c = await asyncpg.connect(dsn=config.database_dsn)
        await c.reload_schema_state()

        table_name_in_db = await c.fetchval(
            """
            select to_regclass('{schema}.{table}')
            """.format(
                schema=table_schema,
                table=table_name,
            ),
        )
        db_revision = None
        if table_name_in_db is None:
            raise MigrationTableMissing(f'{table_name} table does not exist')
        else:
            db_revision = model.Revision(
                await c.fetchval(
                    """
                    select revision from {table_schema}.{table_name} order
                    by timestamp desc limit 1;
                    """.format(
                        table_schema=table_schema,
                        table_name=table_name,
                    ),
                ),
            )

        c.terminate()

        return db_revision
    except MigrationTableMissing:
        logger.exception('Migration table seems to be missing')
        raise
    except Exception as ex:
        logger.exception('Unknown error occurred while getting latest revision')
        raise MigrationProcessingError() from ex


async def create_table(
        config: model.Config,
        table_schema: str,
        table_name: str,
) -> None:
    logger.debug('Creating migrations table, if needed')

    c = await asyncpg.connect(dsn=config.database_dsn)
    await c.reload_schema_state()

    async with c.transaction():
        await c.execute((
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

    c.terminate()
