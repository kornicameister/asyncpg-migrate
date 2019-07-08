import typing as t

import asyncpg
from loguru import logger

from asyncpg_migrate import model


@logger.exception
async def latest_migration_revision(
        config: model.Config,
        migrations_table_schema: str,
        migrations_table_name: str,
) -> t.Optional[model.Revision]:
    c = await asyncpg.connect(dsn=config.database_dsn)
    await c.reload_schema_state()

    table_name_in_db = await c.fetchval(
        """
        select to_regclass('{schema}.{table}')
        """.format(
            schema=migrations_table_schema,
            table=migrations_table_name,
        ),
    )
    if table_name_in_db is None:
        raise RuntimeError(
            f'{migrations_table_name} table does not exist, '
            f'run "upgrade" to create migrations first',
        )
    else:
        db_revision = await c.fetchval(
            f'select revision from '
            '{migrations_table_schema}.{migrations_table_name} '
            f'order by timestamp desc limit 1',
        )
        if not db_revision:
            return None
        return model.Revision(int(db_revision))

    c.terminate()


async def migrations_table_create(
        config: model.Config,
        migrations_table_schema: str,
        migrations_table_name: str,
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
            table_schema=migrations_table_schema,
            table_name=migrations_table_name,
            migration_up=model.MigrationDir.UP,
            migration_down=model.MigrationDir.DOWN,
        ))

    c.terminate()
