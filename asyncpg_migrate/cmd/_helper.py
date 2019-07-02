import asyncpg
from loguru import logger

from asyncpg_migrate import constants
from asyncpg_migrate import model


async def migrations_table_create(config: model.Config) -> None:
    # TODO(kornicameister) add passing different names for
    # migrations table schema and table name itself

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
            table_schema=constants.MIGRATIONS_SCHEMA,
            table_name=constants.MIGRATIONS_TABLE,
            migration_up=model.MigrationDir.UP,
            migration_down=model.MigrationDir.DOWN,
        ))
